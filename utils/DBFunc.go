package utils

import (
	"context"
	"database/sql"
	"fmt"
	"io/fs"
	"log"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
)

// Retrieves all the tables for a given database
func getDateTables(db *sql.DB) ([]string, error) {
	const query = `SELECT name FROM sqlite_master WHERE type='table';`
	rows, err := db.Query(query)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var tables []string
	dateRegex := regexp.MustCompile(`^\d{4}_\d{2}_\d{2}$`) // YYYY_MM_DD format

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, err
		}
		if dateRegex.MatchString(name) {
			tables = append(tables, name)
		}
	}
	return tables, rows.Err()
}

const (
	sqliteDriver  = "sqlite"
	pragmaWAL     = "PRAGMA journal_mode = WAL;"
	pragmaSyncOff = "PRAGMA synchronous = NORMAL;"
)

type Task struct {
	Path     string
	FileName string
}

func RunParallelDBProcessing(
	dir string,
	excluded map[string]struct{},
	workers int,
) error {
	tasks := make(chan Task)
	var wg sync.WaitGroup

	// start N workers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for t := range tasks {
				if err := processDB(t.Path, t.FileName, excluded); err != nil {
					log.Printf("error processing %s: %v", t.Path, err)
				}
			}
		}()
	}

	// discover and enqueue tasks
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			log.Printf("walk error %q: %v", path, err)
			return nil
		}
		if d.IsDir() {
			return nil
		}
		name := d.Name()
		if strings.HasSuffix(name, "-wal") || strings.HasSuffix(name, "-shm") {
			return nil
		}
		if !strings.HasSuffix(name, ".db") {
			return nil
		}
		if _, skip := excluded[name]; skip {
			return nil
		}
		tasks <- Task{Path: path, FileName: name}
		return nil
	})
	close(tasks) // signal workers no more tasks
	wg.Wait()    // wait for all workers to finish
	return err
}

func processDB(path, fileName string, excluded map[string]struct{}) error {
	db, err := sql.Open(sqliteDriver, path)
	if err != nil {
		return fmt.Errorf("open: %w", err)
	}
	defer db.Close()

	if _, err := db.Exec(pragmaWAL); err != nil {
		return fmt.Errorf("pragma WAL: %w", err)
	}
	if _, err := db.Exec(pragmaSyncOff); err != nil {
		return fmt.Errorf("pragma sync: %w", err)
	}

	// Fetch first/last entries
	startOpt, endOpt, startStk, endStk, balanceData, err := GetFirstAndLastEntries(db, fileName)
	if err != nil {
		return fmt.Errorf("fetch entries: %w", err)
	}

	ctx := context.Background()
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	defer tx.Rollback()

	// Determine which tables + data to insert
	var inserts []struct {
		table string
		rows  interface{}
	}

	switch {
	case fileName == "Balance.db":
		inserts = []struct {
			table string
			rows  interface{}
		}{
			{"EODPrices", balanceData},
		}

	case strings.HasSuffix(fileName, ".db") && len(fileName) > 8:
		inserts = []struct {
			table string
			rows  interface{}
		}{
			{"EODPrices", endOpt},
			{"SODPrices", startOpt},
		}

	default:
		inserts = []struct {
			table string
			rows  interface{}
		}{
			{"EODPrices", endStk},
			{"SODPrices", startStk},
		}
	}

	for _, ins := range inserts {
		if err := ensureTable(tx, ins.table, fileName); err != nil {
			return err
		}
		if err := batchInsert(tx, ins.table, ins.rows); err != nil {
			return err
		}
	}

	return tx.Commit()
}

func ensureTable(tx *sql.Tx, tableName, fileName string) error {
	var ddl string

	switch {
	case fileName == "Balance.db":
		// Balance data → only one table (EODPrices)
		ddl = `
CREATE TABLE IF NOT EXISTS EODPrices (
    timestamp   INTEGER PRIMARY KEY,
    balance     REAL NOT NULL,
    realBalance REAL NOT NULL
);`

	case len(fileName) > 8:
		// Option data → EODPrices or SODPrices with Greeks
		ddl = fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    timestamp   INTEGER PRIMARY KEY,
    bid_price   REAL NOT NULL,
    ask_price   REAL NOT NULL,
    last_price  REAL NOT NULL,
    high_price  REAL NOT NULL,
    iv          REAL NOT NULL,
    delta       REAL NOT NULL,
    gamma       REAL NOT NULL,
    theta       REAL NOT NULL,
    vega        REAL NOT NULL
);`, tableName)

	default:
		// Stock data → EODPrices or SODPrices with sizes
		ddl = fmt.Sprintf(`
CREATE TABLE IF NOT EXISTS %s (
    timestamp   INTEGER PRIMARY KEY,
    bid_price   REAL NOT NULL,
    ask_price   REAL NOT NULL,
    last_price  REAL NOT NULL,
    bid_size    INTEGER NOT NULL,
    ask_size    INTEGER NOT NULL
);`, tableName)
	}

	if _, err := tx.Exec(ddl); err != nil {
		return fmt.Errorf("creating table %s: %w", tableName, err)
	}
	return nil
}

// batchInsert switches on the data type and executes batched prepared-stmts
func batchInsert(tx *sql.Tx, table string, data interface{}) error {
	switch rows := data.(type) {

	case []OptionDbData:
		stmt, err := tx.Prepare(fmt.Sprintf(`
			INSERT OR REPLACE INTO %s (
				timestamp, bid_price, ask_price,
				last_price, high_price, iv,
				delta, gamma, theta, vega
			) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
			table,
		))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, e := range rows {
			if _, err := stmt.Exec(e.Timestamp, e.Bid, e.Ask,
				e.Last, e.High, e.IV,
				e.Delta, e.Gamma,
				e.Theta, e.Vega); err != nil {
				return err
			}
		}

	case []StockDbData:
		stmt, err := tx.Prepare(fmt.Sprintf(`
			INSERT OR REPLACE INTO %s (
				timestamp, bid_price, ask_price,
				last_price, bid_size, ask_size
			) VALUES (?, ?, ?, ?, ?, ?)`,
			table,
		))
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, r := range rows {
			if _, err := stmt.Exec(r.Timestamp, r.Bid, r.Ask, r.BidSize, r.AskSize); err != nil {
				return err
			}
		}

	case []BalanceDbData:
		stmt, err := tx.Prepare(
			`INSERT OR REPLACE INTO EODPrices (timestamp, balance, realBalance) VALUES (?, ?, ?)`)
		if err != nil {
			return err
		}
		defer stmt.Close()
		for _, r := range rows {
			if _, err := stmt.Exec(r.Timestamp, r.Balance, r.RealBalance); err != nil {
				return err
			}
		}

	default:
		return fmt.Errorf("unsupported data type %T", data)
	}

	return nil
}

// Deletes all the filler data for each database
func DeleteUnusedData() error {
	dir := "."

	excluded := map[string]struct{}{
		"Open.db":    {},
		"Close.db":   {},
		"Tracker.db": {},
	}

	const deleteTpl = `
		DELETE FROM "%[1]s"
		WHERE timestamp NOT IN (
			(SELECT timestamp FROM "%[1]s" ORDER BY timestamp ASC  LIMIT 1),
			(SELECT timestamp FROM "%[1]s" ORDER BY timestamp DESC LIMIT 1)
		);
		`

	// WalkDir to filter out non-.db files and system files early
	return filepath.WalkDir(dir, func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil || d.IsDir() {
			return walkErr
		}

		name := d.Name()
		if _, skip := excluded[name]; skip ||
			strings.HasSuffix(name, "-shm") ||
			strings.HasSuffix(name, "-wal") ||
			!strings.HasSuffix(name, ".db") {
			return nil
		}

		log.Printf("Cleaning database: %s", path)
		db, err := sql.Open("sqlite", path)
		if err != nil {
			return fmt.Errorf("open %s: %w", path, err)
		}
		defer db.Close()

		tables, err := getDateTables(db)
		if err != nil {
			return fmt.Errorf("getDateTables(%s): %w", path, err)
		}

		// Begin a transaction
		tx, err := db.Begin()
		if err != nil {
			return fmt.Errorf("begin tx: %w", err)
		}

		for _, tbl := range tables {
			stmt := fmt.Sprintf(deleteTpl, tbl)
			if _, err := tx.Exec(stmt); err != nil {
				tx.Rollback()
				return fmt.Errorf("delete from %s: %w", tbl, err)
			}
		}

		// Commit the deletes as one atomic unit
		if err = tx.Commit(); err != nil {
			return fmt.Errorf("commit transaction: %w", err)
		}

		// VACUUM now that the deletions are complete
		if _, err = db.Exec("VACUUM;"); err != nil {
			log.Printf("WARNING: VACUUM failed on %s: %v", path, err)
		}

		log.Println("Finished Cleaning")

		return nil
	})
}

// Gets the last entries of each table
func GetFirstAndLastEntries(db *sql.DB, fileName string) ([]StockDbData, []StockDbData, []BalanceDbData, []OptionDbData, []OptionDbData, error) {
	tables, err := getDateTables(db) // get all date-named tables
	if err != nil {
		return nil, nil, nil, nil, nil, fmt.Errorf("failed to get date tables: %w", err)
	}

	if len(fileName) > 8 && fileName != "Balance.db" {
		var startResults []OptionDbData
		var endResults []OptionDbData

		for _, tableName := range tables {
			query := fmt.Sprintf(`
					SELECT timestamp, bid_price, ask_price, last_price, high_price, iv, delta, gamma, theta, vega
					FROM "%s"
					ORDER BY timestamp DESC
					LIMIT 1;
				`, tableName)

			row := db.QueryRow(query)

			var e OptionDbData
			err := row.Scan(&e.Timestamp, &e.Bid, &e.Ask, &e.Last, &e.High, &e.IV, &e.Delta, &e.Gamma, &e.Theta, &e.Vega)
			if err != nil {
				if err == sql.ErrNoRows {
					continue
				}
				return nil, nil, nil, nil, nil, fmt.Errorf("scan failed in table %s: %w", tableName, err)
			}
			endResults = append(endResults, e)

			query = fmt.Sprintf(`
					SELECT timestamp, bid_price, ask_price, last_price, high_price, iv, delta, gamma, theta, vega
					FROM "%s"
					ORDER BY timestamp ASC
					LIMIT 1;
				`, tableName)

			row = db.QueryRow(query)

			err = row.Scan(&e.Timestamp, &e.Bid, &e.Ask, &e.Last, &e.High, &e.IV, &e.Delta, &e.Gamma, &e.Theta, &e.Vega)
			if err != nil {
				if err == sql.ErrNoRows {
					continue
				}
				return nil, nil, nil, nil, nil, fmt.Errorf("scan failed in table %s: %w", tableName, err)
			}
			startResults = append(startResults, e)
		}
		return nil, nil, nil, startResults, endResults, nil
	} else if fileName == "Balance.db" {
		var allResults []BalanceDbData

		for _, tableName := range tables {
			query := fmt.Sprintf(`
				SELECT timestamp, balance, realBalance
				FROM "%s"
				ORDER BY timestamp DESC
				LIMIT 1;
			`, tableName)

			row := db.QueryRow(query)

			var e BalanceDbData
			err := row.Scan(&e.Timestamp, &e.Balance, &e.RealBalance)
			if err != nil {
				if err == sql.ErrNoRows {
					continue
				}
				return nil, nil, nil, nil, nil, fmt.Errorf("scan failed in table %s: %w", tableName, err)
			}
			allResults = append(allResults, e)
		}
		return nil, nil, allResults, nil, nil, nil
	} else {
		var startResults []StockDbData
		var endResults []StockDbData
		for _, tableName := range tables {
			query := fmt.Sprintf(`
				SELECT timestamp, bid_price, ask_price, last_price, bid_size, ask_size
				FROM "%s"
				ORDER BY timestamp DESC
				LIMIT 1;
			`, tableName)

			row := db.QueryRow(query)

			var e StockDbData
			err := row.Scan(&e.Timestamp, &e.Bid, &e.Ask, &e.Last, &e.BidSize, &e.AskSize)
			if err != nil {
				if err == sql.ErrNoRows {
					continue
				}
				return nil, nil, nil, nil, nil, fmt.Errorf("scan failed in table %s: %w", tableName, err)
			}
			endResults = append(endResults, e)
			query = fmt.Sprintf(`
					SELECT timestamp, bid_price, ask_price, last_price, bid_size, ask_size
					FROM "%s"
					ORDER BY timestamp ASC
					LIMIT 1;
				`, tableName)

			row = db.QueryRow(query)

			err = row.Scan(&e.Timestamp, &e.Bid, &e.Ask, &e.Last, &e.BidSize, &e.AskSize)
			if err != nil {
				if err == sql.ErrNoRows {
					continue
				}
				return nil, nil, nil, nil, nil, fmt.Errorf("scan failed in table %s: %w", tableName, err)
			}
			startResults = append(startResults, e)
		}
		return startResults, endResults, nil, nil, nil, nil

	}
}
