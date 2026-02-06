package main

import (
	"Go-API/utils"
	"context"
	"database/sql"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var ids []string
var ctx = context.Background()

func main() {
	// cancels on ctrl c
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	priceDB, err := initDB("./PriceData.db")
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	openDB, err := initDB("./Open.db")
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	balanceDB, err := initDB("./Balance.db")
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	closeDB, err := initDB("./Close.db")
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	trackerDB, err := initDB("./Tracker.db")

	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}

	initSchemas(openDB, balanceDB, closeDB, trackerDB)

	// Start Redis Container
	utils.StartRedisContainer()

	// 2. INITIALIZE CLIENT
	rdb := utils.InitRedis()

	// SAFETY CHECK: Wait for Redis to actually be ready
	// Sometimes the container is "Up" but the database inside is still booting
	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pingCancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Fatalf("Redis started but not responding: %v", err)
	}

	hub := utils.NewHub()

	// Start Background Services
	go hub.Run()
	go utils.ListenToRedis(context.Background(), rdb, hub)

	// Endpoints for API
	mux := http.NewServeMux()
	mux.HandleFunc("/connect", func(w http.ResponseWriter, r *http.Request) {
		utils.WebsocketConnectHandler(hub, openDB, balanceDB, priceDB, trackerDB, w, r)
	})
	mux.HandleFunc("/startOptionStream", func(w http.ResponseWriter, r *http.Request) {
		utils.StartOptionStream(rdb, w, r)
	})
	mux.HandleFunc("/startStockStream", func(w http.ResponseWriter, r *http.Request) {
		utils.StartStockStream(rdb, w, r)
	})
	mux.HandleFunc("/newTracker", func(w http.ResponseWriter, r *http.Request) {
		utils.NewTrackerHandler(trackerDB, w, r)
	})
	mux.HandleFunc("/openPosition", func(w http.ResponseWriter, r *http.Request) {
		utils.OpenPositionHandler(openDB, balanceDB, w, r)
	})
	mux.HandleFunc("/closePosition", func(w http.ResponseWriter, r *http.Request) {
		utils.ClosePositionHandler(openDB, closeDB, balanceDB, w, r)
	})
	mux.HandleFunc("/closeTracker", func(w http.ResponseWriter, r *http.Request) {
		utils.RemoveTrackerHandler(trackerDB, w, r)
	})

	handler := CorsMiddleware(mux)

	server := &http.Server{
		Addr:    ":8080",
		Handler: handler,
	}

	runDailyAt(15, 0, 5, func() {
		totalShutdown(server)
	})

	// Run server in a goroutine
	go func() {
		fmt.Println("Server is running on port 8080...")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe(): %v", err)
		}
	}()

	<-ctx.Done()

	defer openDB.Close()
	defer balanceDB.Close()
	defer priceDB.Close()
	defer closeDB.Close()
	defer trackerDB.Close()

	totalShutdown(server)
	utils.StopRedisContainer()
}

func totalShutdown(server *http.Server) {
	log.Println("Shutting down gracefully...")

	utils.ShutdownAllClients()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Printf("Server Shutdown Failed:%+v", err)
	}
	log.Println("Server exited")
}

// Runs a function daily at a specified time
func runDailyAt(hour, min, sec int, tasks ...func()) {
	go func() {
		for {
			now := time.Now()
			// Next occurrence of the target time today or tomorrow
			next := time.Date(now.Year(), now.Month(), now.Day(), hour, min, sec, 0, now.Location())
			if !next.After(now) {
				next = next.Add(24 * time.Hour)
			}
			duration := next.Sub(now)
			time.Sleep(duration)
			for _, task := range tasks {
				task()
			}
		}
	}()
}

// Function that handles http permissions
func CorsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func initDB(path string) (*sql.DB, error) {
	db, err := sql.Open("sqlite", path)
	if err != nil {
		return nil, err
	}

	// Configure for high-concurrency and reliability
	db.SetMaxOpenConns(10) // Allow multiple readers
	db.SetMaxIdleConns(5)  // Keep a few connections ready
	db.SetConnMaxIdleTime(30 * time.Second)

	var lastErr error
	for i := 0; i < 5; i++ {
		_, lastErr = db.Exec("PRAGMA journal_mode=WAL; PRAGMA busy_timeout=5000; PRAGMA synchronous=NORMAL;")
		if lastErr == nil {
			return db, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return nil, fmt.Errorf("WAL/Timeout failed on %s: %v", path, lastErr)
}

func initSchemas(openDB, balanceDB, closeDB, trackerDB *sql.DB) {
	schemas := []struct {
		db  *sql.DB
		sql string
	}{
		{openDB, `CREATE TABLE IF NOT EXISTS OpenPositions (id TEXT PRIMARY KEY, price REAL, amount INTEGER);`},
		{balanceDB, `CREATE TABLE IF NOT EXISTS Balance (timestamp INTEGER PRIMARY KEY, balance REAL, cash REAL);`},
		{closeDB, `CREATE TABLE IF NOT EXISTS ClosePositions (order_number INTEGER PRIMARY KEY AUTOINCREMENT, id TEXT, price REAL, amount INTEGER, pl REAL);`},
		{trackerDB, `CREATE TABLE IF NOT EXISTS Tracker (id TEXT PRIMARY KEY);`},
	}

	for _, s := range schemas {
		if _, err := s.db.Exec(s.sql); err != nil {
			log.Printf("Schema init error: %v", err)
		}
	}
}
