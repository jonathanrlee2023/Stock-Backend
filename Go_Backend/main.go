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

	priceDBPath := filepath.Join(dbDir, "PriceData.db")
	balanceDBPath := filepath.Join(dbDir, "Balance.db")
	openDBPath := filepath.Join(dbDir, "Open.db")
	closeDBPath := filepath.Join(dbDir, "Close.db")
	trackerDBPath := filepath.Join(dbDir, "Tracker.db")

	priceDB, err := utils.InitDB(priceDBPath)
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	openDB, err := utils.InitDB(openDBPath)
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	balanceDB, err := utils.InitDB(balanceDBPath)
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	closeDB, err := utils.InitDB(closeDBPath)
	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}
	trackerDB, err := utils.InitDB(trackerDBPath)

	if err != nil {
		log.Fatalf("Error initializing database: %v", err)
	}

	utils.InitSchemas(openDB, balanceDB, closeDB, trackerDB)

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
	go utils.ListenToRedis(context.Background(), rdb, hub, "Stream_Channel")
	go utils.ListenToRedis(context.Background(), rdb, hub, "Company_Channel")
	go utils.ListenToRedis(context.Background(), rdb, hub, "One_Time_Data_Channel")

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
	mux.HandleFunc("/companyStats", func(w http.ResponseWriter, r *http.Request) {
		utils.CompanyHandler(rdb, w, r)
	})

	handler := CorsMiddleware(mux)

	server := &http.Server{
		Addr:    ":8080",
		Handler: handler,
	}

	// utils.RunDailyAt(15, 0, 5, func() {
	// 	totalShutdown(server)
	// })

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
