package utils

import (
	"database/sql"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func setupTestDB(t *testing.T) *sql.DB {
    db, err := sql.Open("sqlite", ":memory:")
    require.NoError(t, err)
    
    _, err = db.Exec(`CREATE TABLE IF NOT EXISTS Tracker (id TEXT PRIMARY KEY)`)
    require.NoError(t, err)
    
    _, err = db.Exec(`CREATE TABLE IF NOT EXISTS OpenPositions (
        id TEXT, price REAL, amount REAL, 
        portfolio_id INTEGER, user_id INTEGER,
        PRIMARY KEY (id, portfolio_id, user_id)
    )`)
    require.NoError(t, err)

    _, err = db.Exec(`CREATE TABLE IF NOT EXISTS Balance (
        timestamp INTEGER, balance REAL, cash REAL,
        portfolio_id INTEGER, user_id INTEGER,
        PRIMARY KEY (timestamp, portfolio_id, user_id)
    )`)
    require.NoError(t, err)

    _, err = db.Exec(`CREATE TABLE IF NOT EXISTS Users (
        user_id INTEGER PRIMARY KEY AUTOINCREMENT,
        username TEXT UNIQUE, password TEXT
    )`)
    require.NoError(t, err)

    _, err = db.Exec(`CREATE TABLE IF NOT EXISTS Portfolios (
        portfolio_id INTEGER, user_id INTEGER, 
        name TEXT, PRIMARY KEY (portfolio_id, user_id)
    )`)
    require.NoError(t, err)

	// Runs automatically when test ends
    t.Cleanup(func() {
        db.Close()
    })

    return db
}

func TestHashPassword(t *testing.T) {
	hash, err := HashPassword("mypassword")
	assert.NoError(t, err)
	assert.NotEqual(t, "mypassword", hash) 
	assert.True(t, CheckPasswordHash("mypassword", hash))
}

func TestCheckPasswordHash_WrongPassword(t *testing.T) {
	hash, _ := HashPassword("correct")
	assert.False(t, CheckPasswordHash("wrong", hash))
}

func TestNewTrackerHandler_WrongMethod(t *testing.T) {
    req := httptest.NewRequest(http.MethodGet, "/newTracker", nil)
    w := httptest.NewRecorder()
    NewTrackerHandler(w, req)
    assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestNewTrackerHandler_InvalidInput(t *testing.T) {
    jsonBody := `{"name": 12345}` 
    req := httptest.NewRequest(http.MethodPost, "/newTracker", strings.NewReader(jsonBody))
    req.Header.Set("Content-Type", "application/json")
    
    w := httptest.NewRecorder()

    NewTrackerHandler(w, req)

    assert.Equal(t, http.StatusBadRequest, w.Code)
    
    assert.Contains(t, w.Body.String(), "Tracker ID is required and must be a string")
}

func TestNewTrackerHandler_InvalidJSON(t *testing.T) {
    req := httptest.NewRequest(http.MethodPost, "/newTracker", 
        strings.NewReader(`{invalid json`))
    req.Header.Set("Content-Type", "application/json")
    w := httptest.NewRecorder()
    NewTrackerHandler(w, req)
    assert.Equal(t, http.StatusBadRequest, w.Code)
}


func TestNewTrackerHandler_ValidRequest(t *testing.T) {
    db := setupTestDB(t)
    GlobalDatabasePool.TrackerDB = db
    body := strings.NewReader(`{"id": "AAPL"}`)
    req := httptest.NewRequest(http.MethodPost, "/newTracker", body)
    w := httptest.NewRecorder()

    NewTrackerHandler(w, req)
    assert.Equal(t, http.StatusOK, w.Code)

    // Verify it actually wrote to DB
    var id string
    err := db.QueryRow("SELECT id FROM Tracker WHERE id = ?", "AAPL").Scan(&id)
    assert.NoError(t, err)
    assert.Equal(t, "AAPL", id)
}

func TestRemoveTrackerHandler_ValidRequest(t *testing.T) {
    db := setupTestDB(t)
    GlobalDatabasePool.TrackerDB = db

    db.Exec("INSERT INTO Tracker (id) VALUES (?)", "AAPL")

    body := strings.NewReader(`{"id": "AAPL"}`)
    req := httptest.NewRequest(http.MethodPost, "/closeTracker", body)
    w := httptest.NewRecorder()

    RemoveTrackerHandler(w, req)
    assert.Equal(t, http.StatusOK, w.Code)

    var count int
    db.QueryRow("SELECT COUNT(*) FROM Tracker WHERE id = ?", "AAPL").Scan(&count)
    assert.Equal(t, 0, count)
}


func TestLoginHandler_ValidCredentials(t *testing.T) {
    t.Setenv("JWT_SECRET", "test-secret")

    db := setupTestDB(t)
    GlobalDatabasePool.BalanceDB = db

    hash, _ := HashPassword("password123")
    db.Exec("INSERT INTO Users (username, password) VALUES (?, ?)", "testuser", hash)

    body := strings.NewReader(`{"username": "testuser", "password": "password123"}`)
    req := httptest.NewRequest(http.MethodPost, "/login", body)
    w := httptest.NewRecorder()

    LoginHandler(w, req)
    assert.Equal(t, http.StatusOK, w.Code)
}

func TestLoginHandler_WrongPassword(t *testing.T) {
    db := setupTestDB(t)
    GlobalDatabasePool.BalanceDB = db

    hash, _ := HashPassword("correctpassword")
    db.Exec("INSERT INTO Users (username, password) VALUES (?, ?)", "testuser", hash)

    body := strings.NewReader(`{"username": "testuser", "password": "wrongpassword"}`)
    req := httptest.NewRequest(http.MethodPost, "/login", body)
    w := httptest.NewRecorder()

    LoginHandler(w, req)
    assert.Equal(t, http.StatusUnauthorized, w.Code)
}

func TestLoginHandler_UserNotFound(t *testing.T) {
    db := setupTestDB(t)
    GlobalDatabasePool.BalanceDB = db
    body := strings.NewReader(`{"username": "nobody", "password": "password"}`)
    req := httptest.NewRequest(http.MethodPost, "/login", body)
    w := httptest.NewRecorder()

    LoginHandler(w, req)
    assert.Equal(t, http.StatusNotFound, w.Code)
}