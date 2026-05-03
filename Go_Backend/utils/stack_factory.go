package utils

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"path/filepath"
	"time"

	"github.com/redis/go-redis/v9"
)

type DataStoreStack struct {
	OpenDB    *sql.DB
	BalanceDB *sql.DB
	CloseDB   *sql.DB
	TrackerDB *sql.DB
	Pool      *DatabasePool
}

type RealtimeStack struct {
	Redis *redis.Client
	Hub   *Hub
}

type HTTPStack struct {
	Protected http.Handler
	Websocket http.Handler
	Root      http.Handler
}

type ServerStack struct {
	Data     DataStoreStack
	Realtime RealtimeStack
	HTTP     HTTPStack
}

type ServerStackFactory interface {
	BuildDataStores(ctx context.Context, dbDir string) (DataStoreStack, error)
	BuildRealtime(ctx context.Context) (RealtimeStack, error)
	BuildHTTP(ctx context.Context, deps ServerStack) (HTTPStack, error)
}

type ProductionStackFactory struct{}

func (ProductionStackFactory) BuildDataStores(_ context.Context, dbDir string) (DataStoreStack, error) {
	balanceDBPath := filepath.Join(dbDir, "Balance.db")
	openDBPath := filepath.Join(dbDir, "Open.db")
	closeDBPath := filepath.Join(dbDir, "Close.db")
	trackerDBPath := filepath.Join(dbDir, "Tracker.db")

	openDB, err := InitDB(openDBPath)
	if err != nil {
		return DataStoreStack{}, fmt.Errorf("init open db: %w", err)
	}
	balanceDB, err := InitDB(balanceDBPath)
	if err != nil {
		return DataStoreStack{}, fmt.Errorf("init balance db: %w", err)
	}
	closeDB, err := InitDB(closeDBPath)
	if err != nil {
		return DataStoreStack{}, fmt.Errorf("init close db: %w", err)
	}
	trackerDB, err := InitDB(trackerDBPath)
	if err != nil {
		return DataStoreStack{}, fmt.Errorf("init tracker db: %w", err)
	}

	InitSchemas(openDB, balanceDB, closeDB, trackerDB)
	pool := &DatabasePool{
		OpenDB:    openDB,
		BalanceDB: balanceDB,
		CloseDB:   closeDB,
		TrackerDB: trackerDB,
	}

	return DataStoreStack{
		OpenDB:    openDB,
		BalanceDB: balanceDB,
		CloseDB:   closeDB,
		TrackerDB: trackerDB,
		Pool:      pool,
	}, nil
}

func (ProductionStackFactory) BuildRealtime(ctx context.Context) (RealtimeStack, error) {
	rdb := InitRedis()
	pingCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := rdb.Ping(pingCtx).Err(); err != nil {
		return RealtimeStack{}, fmt.Errorf("redis ping failed: %w", err)
	}

	return RealtimeStack{
		Redis: rdb,
		Hub:   NewHub(),
	}, nil
}

func (ProductionStackFactory) BuildHTTP(_ context.Context, deps ServerStack) (HTTPStack, error) {
	if deps.Realtime.Redis == nil || deps.Realtime.Hub == nil {
		return HTTPStack{}, fmt.Errorf("realtime dependencies are required to build HTTP stack")
	}

	protected := http.NewServeMux()
	protected.HandleFunc("/startOptionStream", func(w http.ResponseWriter, r *http.Request) {
		StartOptionStream(deps.Realtime.Redis, w, r)
	})
	protected.HandleFunc("/startStockStream", func(w http.ResponseWriter, r *http.Request) {
		StartStockStream(deps.Realtime.Redis, w, r)
	})
	protected.HandleFunc("/newTracker", func(w http.ResponseWriter, r *http.Request) {
		NewTrackerHandler(w, r)
	})
	protected.HandleFunc("/openPosition", func(w http.ResponseWriter, r *http.Request) {
		OpenSharesPositionHandler(w, r)
	})
	protected.HandleFunc("/closePosition", func(w http.ResponseWriter, r *http.Request) {
		ClosePositionHandler(w, r)
	})
	protected.HandleFunc("/closeTracker", func(w http.ResponseWriter, r *http.Request) {
		RemoveTrackerHandler(w, r)
	})
	protected.HandleFunc("/newPortfolio", func(w http.ResponseWriter, r *http.Request) {
		NewPortfolioHandler(w, r)
	})
	protected.HandleFunc("/deletePortfolio", func(w http.ResponseWriter, r *http.Request) {
		DeletePortfolioHandler(w, r)
	})
	protected.HandleFunc("/startBacktest", func(w http.ResponseWriter, r *http.Request) {
		StartBacktest(deps.Realtime.Redis, w, r)
	})

	wsHandler := RequireAuthWS(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		WebsocketConnectHandler(deps.Realtime.Hub, w, r)
	}))

	root := http.NewServeMux()
	root.HandleFunc("/login", func(w http.ResponseWriter, r *http.Request) {
		LoginHandler(w, r)
	})
	root.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		CreateUserHandler(w, r)
	})
	root.Handle("/connect", wsHandler)
	root.Handle("/", RequireAuth(protected))

	return HTTPStack{
		Protected: protected,
		Websocket: wsHandler,
		Root:      root,
	}, nil
}
