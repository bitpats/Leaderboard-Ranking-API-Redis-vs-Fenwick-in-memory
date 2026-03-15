package main

import (
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"time"

	"leaderboard-fenwick/store"

	"github.com/gin-gonic/gin"
)

var globalStore *store.Store

func main() {
	rand.Seed(42)
	globalStore = store.New()

	r := gin.Default()

	r.GET("/ping", ping)
	r.POST("/restore_random", restoreRandom)
	r.GET("/user/:user_id/score", getUserScore)
	r.GET("/user/:user_id/rank", getUserRank)
	r.GET("/score/:score/count", scoreCountHandler)
	r.GET("/leaderboard", getLeaderboard)
	r.GET("/leaderboard/all", getLeaderboardAll)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
	}
	r.Run(":" + port)
}

func ping(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{"status": "ok", "mode": "in-memory-fenwick"})
}

func restoreRandom(c *gin.Context) {
	n, err := strconv.Atoi(c.Query("n"))
	if err != nil || n < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "n required as positive integer"})
		return
	}
	globalStore.RestoreRandom(n, rand.New(rand.NewSource(time.Now().UnixNano())))
	c.JSON(http.StatusOK, gin.H{"status": "ok", "users_loaded": n})
}

func getUserScore(c *gin.Context) {
	userID := c.Param("user_id")
	score, ok := globalStore.GetUserScore(userID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"user_id": userID, "score": score})
}

func getUserRank(c *gin.Context) {
	userID := c.Param("user_id")
	rank, ok := globalStore.GetUserRank(userID)
	if !ok {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"user_id": userID, "rank": rank})
}

func scoreCountHandler(c *gin.Context) {
	scoreVal, err := strconv.Atoi(c.Param("score"))
	if err != nil || scoreVal < store.MinScore || scoreVal > store.MaxScore {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid score"})
		return
	}
	count := globalStore.GetScoreCount(scoreVal)
	c.JSON(http.StatusOK, gin.H{"score": scoreVal, "count": count})
}

func getLeaderboard(c *gin.Context) {
	minRankQ := c.Query("min_rank")
	maxRankQ := c.Query("max_rank")
	minScoreQ := c.Query("min_score")
	maxScoreQ := c.Query("max_score")

	var minRank, maxRank *int
	if minRankQ != "" {
		if v, err := strconv.Atoi(minRankQ); err == nil && v >= 1 {
			minRank = &v
		}
	}
	if maxRankQ != "" {
		if v, err := strconv.Atoi(maxRankQ); err == nil && v >= 1 {
			maxRank = &v
		}
	}
	if minRank != nil && maxRank != nil && *minRank > *maxRank {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "min_rank must be <= max_rank"})
		return
	}

	var minScoreVal, maxScoreVal *int
	if minScoreQ != "" {
		if v, err := strconv.Atoi(minScoreQ); err == nil {
			minScoreVal = &v
		}
	}
	if maxScoreQ != "" {
		if v, err := strconv.Atoi(maxScoreQ); err == nil {
			maxScoreVal = &v
		}
	}
	if minScoreVal != nil && maxScoreVal != nil && *minScoreVal > *maxScoreVal {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "min_score must be <= max_score"})
		return
	}

	startRank := 1
	endRank := store.LeaderboardTop
	if minRank != nil {
		startRank = *minRank
	}
	if maxRank != nil {
		endRank = *maxRank
	}
	if startRank < 1 {
		startRank = 1
	}

	entries := globalStore.GetLeaderboard(startRank, endRank, minScoreVal, maxScoreVal)
	out := make([]gin.H, len(entries))
	for i, e := range entries {
		out[i] = gin.H{"rank": e.Rank, "user_id": e.UserID, "score": e.Score}
	}
	c.JSON(http.StatusOK, gin.H{"leaderboard": out})
}

func getLeaderboardAll(c *gin.Context) {
	entries := globalStore.GetLeaderboardAll()
	out := make([]gin.H, len(entries))
	for i, e := range entries {
		out[i] = gin.H{"user_id": e.UserID, "score": e.Score}
	}
	c.JSON(http.StatusOK, gin.H{"users": out})
}
