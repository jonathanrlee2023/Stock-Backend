package main

import (
	"Go-API/utils"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/joho/godotenv"
)

var ids []string
var ctx = context.Background()

func main() {
	// cancels on ctrl c
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	godotenv.Load("../.env")
	dbDir := os.Getenv("DB_DIR")
	if dbDir == "" {
		dbDir = "../Database" // Safe fallback for local dev
	}

	balanceDBPath := filepath.Join(dbDir, "Balance.db")
	openDBPath := filepath.Join(dbDir, "Open.db")
	closeDBPath := filepath.Join(dbDir, "Close.db")
	trackerDBPath := filepath.Join(dbDir, "Tracker.db")

	
	openDB, err := utils.InitDB(openDBPath)
	if err != nil {
		log.Printf("Error initializing database: %v", err)
	}
	balanceDB, err := utils.InitDB(balanceDBPath)
	if err != nil {
		log.Printf("Error initializing database: %v", err)
	}
	closeDB, err := utils.InitDB(closeDBPath)
	if err != nil {
		log.Printf("Error initializing database: %v", err)
	}
	trackerDB, err := utils.InitDB(trackerDBPath)

	if err != nil {
		log.Printf("Error initializing database: %v", err)
	}
	utils.InitSchemas(openDB, balanceDB, closeDB, trackerDB)

	utils.GlobalDatabasePool = &utils.DatabasePool{OpenDB: openDB, BalanceDB: balanceDB, CloseDB: closeDB, TrackerDB: trackerDB}

	// 2. INITIALIZE CLIENT
	rdb := utils.InitRedis()

	// SAFETY CHECK: Wait for Redis to actually be ready
	// Sometimes the container is "Up" but the database inside is still booting
	pingCtx, pingCancel := context.WithTimeout(ctx, 5*time.Second)
	defer pingCancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		log.Printf("Redis started but not responding: %v", err)
	}

	hub := utils.NewHub()

	// Start Background Services
	go hub.Run()
	go utils.ListenToRedis(context.Background(), rdb, hub, "Stream_Channel")
	go utils.ListenToRedis(context.Background(), rdb, hub, "Company_Channel")
	go utils.ListenToRedis(context.Background(), rdb, hub, "One_Time_Data_Channel")
	go utils.ListenToRedis(context.Background(), rdb, hub, "Global_News_Channel")
	go utils.ListenToRedis(context.Background(), rdb, hub, "Backtest_Channel")

	// Endpoints for API
	mux := http.NewServeMux()
	mux.HandleFunc("/connect", func(w http.ResponseWriter, r *http.Request) {
		utils.WebsocketConnectHandler(hub, w, r)
	})
	mux.HandleFunc("/startOptionStream", func(w http.ResponseWriter, r *http.Request) {
		utils.StartOptionStream(rdb, w, r)
	})
	mux.HandleFunc("/startStockStream", func(w http.ResponseWriter, r *http.Request) {
		utils.StartStockStream(rdb, w, r)
	})
	mux.HandleFunc("/newTracker", func(w http.ResponseWriter, r *http.Request) {
		utils.NewTrackerHandler(w, r)
	})
	mux.HandleFunc("/openPosition", func(w http.ResponseWriter, r *http.Request) {
		utils.OpenSharesPositionHandler(w, r)
	})
	mux.HandleFunc("/closePosition", func(w http.ResponseWriter, r *http.Request) {
		utils.ClosePositionHandler(w, r)
	})
	mux.HandleFunc("/closeTracker", func(w http.ResponseWriter, r *http.Request) {
		utils.RemoveTrackerHandler(w, r)
	})
	mux.HandleFunc("/newPortfolio", func(w http.ResponseWriter, r *http.Request) {
		utils.NewPortfolioHandler(w, r)
	})
	mux.HandleFunc("/deletePortfolio", func(w http.ResponseWriter, r *http.Request) {
		utils.DeletePortfolioHandler(w, r)
	})
	mux.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		utils.LoginHandler(w, r)
	})
	mux.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		utils.CreateUserHandler(w, r)
	})
	mux.HandleFunc("/startBacktest", func(w http.ResponseWriter, r *http.Request) {
		utils.StartBacktest(rdb, w, r)
	})


	handler := CorsMiddleware(mux)

	server := &http.Server{
		Addr:    ":8080",
		Handler: handler,
	}

	// Run server in a goroutine
	go func() {
		fmt.Println("Server is running on port 8080...")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("ListenAndServe(): %v", err)
		}
	}()

	<-ctx.Done()

	defer openDB.Close()
	defer balanceDB.Close()
	defer closeDB.Close()
	defer trackerDB.Close()

	totalShutdown(server)
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

// Function that handles http permissions
func CorsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}
