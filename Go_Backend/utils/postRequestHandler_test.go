package utils

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
)

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
    NewTrackerHandler(nil, w, req)
    assert.Equal(t, http.StatusMethodNotAllowed, w.Code)
}

func TestNewTrackerHandler_InvalidInput(t *testing.T) {
    jsonBody := `{"name": 12345}` 
    req := httptest.NewRequest(http.MethodPost, "/newTracker", strings.NewReader(jsonBody))
    req.Header.Set("Content-Type", "application/json")
    
    w := httptest.NewRecorder()

    NewTrackerHandler(nil, w, req)

    assert.Equal(t, http.StatusBadRequest, w.Code)
    
    assert.Contains(t, w.Body.String(), "Tracker ID is required and must be a string")
}