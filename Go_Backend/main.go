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

	protected := http.NewServeMux()
	protected.HandleFunc("/startOptionStream", func(w http.ResponseWriter, r *http.Request) {
		utils.StartOptionStream(rdb, w, r)
	})
	protected.HandleFunc("/startStockStream", func(w http.ResponseWriter, r *http.Request) {
		utils.StartStockStream(rdb, w, r)
	})
	protected.HandleFunc("/newTracker", func(w http.ResponseWriter, r *http.Request) {
		utils.NewTrackerHandler(w, r)
	})
	protected.HandleFunc("/openPosition", func(w http.ResponseWriter, r *http.Request) {
		utils.OpenSharesPositionHandler(w, r)
	})
	protected.HandleFunc("/closePosition", func(w http.ResponseWriter, r *http.Request) {
		utils.ClosePositionHandler(w, r)
	})
	protected.HandleFunc("/closeTracker", func(w http.ResponseWriter, r *http.Request) {
		utils.RemoveTrackerHandler(w, r)
	})
	protected.HandleFunc("/newPortfolio", func(w http.ResponseWriter, r *http.Request) {
		utils.NewPortfolioHandler(w, r)
	})
	protected.HandleFunc("/deletePortfolio", func(w http.ResponseWriter, r *http.Request) {
		utils.DeletePortfolioHandler(w, r)
	})
	protected.HandleFunc("/startBacktest", func(w http.ResponseWriter, r *http.Request) {
		utils.StartBacktest(rdb, w, r)
	})

	wsHandler := utils.RequireAuthWS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		utils.WebsocketConnectHandler(hub, w, r)
	}))

	root := http.NewServeMux()
	root.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		utils.LoginHandler(w, r)
	})
	root.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		utils.CreateUserHandler(w, r)
	})
	root.Handle("/connect", wsHandler)
	root.Handle("/", utils.RequireAuth(protected))

	handler := SecurityHeadersMiddleware(CorsMiddleware(root))

	server := &http.Server{
		Addr:              ":8080",
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
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
	allowedOrigin := os.Getenv("ALLOWED_ORIGIN")
	if allowedOrigin == "" {
		allowedOrigin = "http://localhost:5173"
	}

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		origin := r.Header.Get("Origin")
		if origin == allowedOrigin {
			w.Header().Set("Access-Control-Allow-Origin", origin)
		}
		w.Header().Set("Vary", "Origin")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS, DELETE")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		next.ServeHTTP(w, r)
	})
}

func SecurityHeadersMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("X-Frame-Options", "DENY")
		w.Header().Set("Referrer-Policy", "no-referrer")
		w.Header().Set("Content-Security-Policy", "default-src 'none'; frame-ancestors 'none'")
		next.ServeHTTP(w, r)
	})
}
