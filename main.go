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
)

var ids []string

func main() {
	stop := make(chan os.Signal, 1)
	// Check if ctrl C is pressed
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	// Endpoints for API
	mux := http.NewServeMux()
	mux.HandleFunc("/connect", utils.WebsocketConnectHandler)
	mux.HandleFunc("/startOptionStream", utils.StartOptionStream)
	mux.HandleFunc("/startStockStream", utils.StartStockStream)
	mux.HandleFunc("/dataReady", utils.DataReadyHandler)
	mux.HandleFunc("/newTracker", utils.NewTrackerHandler)
	mux.HandleFunc("/openPosition", utils.OpenPositionHandler)
	mux.HandleFunc("/closePosition", utils.ClosePositionHandler)
	mux.HandleFunc("/closeTracker", utils.RemoveTrackerHandler)

	handler := CorsMiddleware(mux)

	server := &http.Server{
		Addr:    ":8080",
		Handler: handler,
	}

	runDailyAt(15, 0, 5, func() {
		utils.WriteOpenCloseData("Start")
		utils.WriteOpenCloseData("End")
		utils.InitCSVData()
		utils.DeleteUnusedData()
		totalShutdown(server)
	})

	utils.InitCSVData()

	// Run server in a goroutine
	go func() {
		fmt.Println("Server is running on port 8080...")
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe(): %v", err)
		}
	}()

	<-stop
	totalShutdown(server)
}

func totalShutdown(server *http.Server) {
	log.Println("Shutting down gracefully...")

	utils.ShutdownAllClients()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("Server Shutdown Failed:%+v", err)
	}
	log.Println("Server exited")
	os.Exit(0)
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
