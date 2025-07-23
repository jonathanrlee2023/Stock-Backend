package utils

import (
	"database/sql"
	"fmt"
	"io/fs"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strings"
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

// Function to make sure the EOD or Start of day table exists in option db
func ensureOptionTable(db *sql.Tx, tableName string) error {
	sql := fmt.Sprintf(`
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

	if _, err := db.Exec(sql); err != nil {
		return fmt.Errorf("create %s: %w", tableName, err)
	}
	return nil
}

// Function to make sure the EOD or Start of day table exists in stock db
func ensureStockTable(db *sql.Tx, tableName string) error {
	sql := fmt.Sprintf(`
      CREATE TABLE IF NOT EXISTS %s (
			timestamp INTEGER PRIMARY KEY,
			bid_price REAL NOT NULL,
			ask_price REAL NOT NULL,
			last_price REAL NOT NULL,
			bid_size INTEGER NOT NULL,
			ask_size INTEGER NOT NULL
		);`, tableName)

	if _, err := db.Exec(sql); err != nil {
		return fmt.Errorf("create %s: %w", tableName, err)
	}
	return nil
}

// Function to insert prices into EOD or SOD table for options
func insertOptionPrices(db *sql.Tx, tableName string, data []OptionDbData) error {
	insertSQL := fmt.Sprintf(`
      INSERT OR REPLACE INTO %s (
        timestamp, bid_price, ask_price,
        last_price, high_price, iv,
        delta, gamma, theta, vega
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		tableName,
	)

	stmt, err := db.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("prepare insert %s: %w", tableName, err)
	}
	defer stmt.Close()

	for _, e := range data {
		if _, err := stmt.Exec(
			e.Timestamp, e.Bid, e.Ask,
			e.Last, e.High, e.IV,
			e.Delta, e.Gamma,
			e.Theta, e.Vega,
		); err != nil {
			return fmt.Errorf("insert into %s %+v: %w", tableName, e, err)
		}
	}
	return nil
}

// Function to insert prices into EOD or SOD table for stocks
func insertStockPrices(db *sql.Tx, tableName string, data []StockDbData) error {
	insertSQL := fmt.Sprintf(`
      INSERT OR REPLACE INTO %s (
        timestamp, bid_price, ask_price,
        last_price, bid_size, ask_size
      ) VALUES (?, ?, ?, ?, ?, ?)`,
		tableName,
	)

	stmt, err := db.Prepare(insertSQL)
	if err != nil {
		return fmt.Errorf("prepare insert %s: %w", tableName, err)
	}
	defer stmt.Close()

	for _, e := range data {
		if _, err := stmt.Exec(
			e.Timestamp, e.Bid, e.Ask,
			e.Last, e.BidSize, e.AskSize,
		); err != nil {
			return fmt.Errorf("insert into %s %+v: %w", tableName, e, err)
		}
	}
	return nil
}

// Walks through every db and writes the SOD and EOD data in its own table
func WriteOpenCloseData() {
	dir := "." // current working directory

	excluded := map[string]struct{}{
		"Open.db":    {},
		"Close.db":   {},
		"Tracker.db": {},
	}

	err := filepath.Walk(dir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			log.Printf("Error accessing path %q: %v\n", path, err)
			return nil // or return err if you want to stop walking
		}

		if info == nil {
			log.Printf("FileInfo is nil for path: %s\n", path)
			return nil
		}
		if strings.Contains(info.Name(), "-shm") || strings.Contains(info.Name(), "-wal") {
			fmt.Printf("Skipping SQLite system file: %s\n", info.Name())
			return nil
		}

		if info.IsDir() {
			return nil
		}

		if !strings.HasSuffix(info.Name(), ".db") {
			return nil
		}

		if _, found := excluded[info.Name()]; found {
			fmt.Printf("Skipping excluded file: %s\n", info.Name())
			return nil
		}

		fmt.Printf("Processing database: %s\n", path)

		db, err := sql.Open("sqlite", path)
		if err != nil {
			log.Printf("Failed to open database %s: %v\n", path, err)
			return nil
		}
		defer db.Close()

		fileName := filepath.Base(path)

		if len(fileName) > 8 && fileName != "Balance.db" {
			_, _, _, startData, endData, err := GetFirstAndLastEntries(db, fileName)
			if err != nil {
				return fmt.Errorf("fetching data: %w", err)
			}

			// Optional: group all into one transaction
			tx, err := db.Begin()
			if err != nil {
				return err
			}
			defer tx.Rollback()

			tables := []struct {
				name string
				data []OptionDbData
			}{
				{"EODPrices", endData},
				{"SODPrices", startData},
			}

			for _, tbl := range tables {
				if err := ensureOptionTable(tx, tbl.name); err != nil {
					return err
				}
				if err := insertOptionPrices(tx, tbl.name, tbl.data); err != nil {
					return err
				}
			}

			return tx.Commit()

		} else if fileName == "Balance.db" {
			_, _, data, _, _, err := GetFirstAndLastEntries(db, fileName)
			if err != nil {
				log.Println(err)
			}
			fmt.Println(data)
			createTableSQL := `
				CREATE TABLE IF NOT EXISTS EODPrices (
					timestamp INTEGER PRIMARY KEY,
					balance REAL NOT NULL,
					realBalance REAL NOT NULL
				);`

			if _, err := db.Exec(createTableSQL); err != nil {
				return fmt.Errorf("failed to create table: %w", err)
			}

			insertSQL := `INSERT OR REPLACE INTO EODPrices (
                            timestamp, balance, realBalance
                        ) VALUES (?, ?, ?)`
			stmt, err := db.Prepare(insertSQL)
			if err != nil {
				return fmt.Errorf("failed to prepare insert statement: %w", err)
			}
			defer stmt.Close()

			for _, entry := range data {
				_, err = stmt.Exec(entry.Timestamp, entry.Balance, entry.RealBalance)
				if err != nil {
					return fmt.Errorf("failed to insert entry %+v: %w", entry, err)
				}
			}
		} else {
			startData, endData, _, _, _, err := GetFirstAndLastEntries(db, fileName)
			if err != nil {
				return fmt.Errorf("fetching data: %w", err)
			}

			// Optional: group all into one transaction
			tx, err := db.Begin()
			if err != nil {
				return err
			}
			defer tx.Rollback()

			tables := []struct {
				name string
				data []StockDbData
			}{
				{"EODPrices", endData},
				{"SODPrices", startData},
			}

			for _, tbl := range tables {
				if err := ensureStockTable(tx, tbl.name); err != nil {
					return err
				}
				if err := insertStockPrices(tx, tbl.name, tbl.data); err != nil {
					return err
				}
			}

			return tx.Commit()

		}
		return nil
	})

	if err != nil {
		log.Printf("Failed walking folder: %v", err)
	}
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
