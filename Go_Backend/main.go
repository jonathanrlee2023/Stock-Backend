package main

import (
	"Go-API/utils"
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
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
		dbDir = "../Database"
	}

	factory := utils.ProductionStackFactory{}
	dataStack, err := factory.BuildDataStores(ctx, dbDir)
	if err != nil {
		log.Fatalf("Failed to build data stack: %v", err)
	}
	utils.GlobalDatabasePool = dataStack.Pool

	realtimeStack, err := factory.BuildRealtime(ctx)
	if err != nil {
		log.Fatalf("Failed to build realtime stack: %v", err)
	}
	rdb := realtimeStack.Redis
	hub := realtimeStack.Hub

	// Start Background Services
	go hub.Run()
	go utils.ListenToRedis(context.Background(), rdb, hub, "Stream_Channel")
	go utils.ListenToRedis(context.Background(), rdb, hub, "Company_Channel")
	go utils.ListenToRedis(context.Background(), rdb, hub, "One_Time_Data_Channel")
	go utils.ListenToRedis(context.Background(), rdb, hub, "Global_News_Channel")
	go utils.ListenToRedis(context.Background(), rdb, hub, "Backtest_Channel")

	httpStack, err := factory.BuildHTTP(ctx, utils.ServerStack{
		Data:     dataStack,
		Realtime: realtimeStack,
	})
	if err != nil {
		log.Fatalf("Failed to build HTTP stack: %v", err)
	}

	handler := SecurityHeadersMiddleware(CorsMiddleware(httpStack.Root))

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

	defer dataStack.OpenDB.Close()
	defer dataStack.BalanceDB.Close()
	defer dataStack.CloseDB.Close()
	defer dataStack.TrackerDB.Close()

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
