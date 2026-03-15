package main

import (
	"math/rand"
	"net/http"
	"os"
	"strconv"

	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

const (
	zsetKey        = "leaderboard:zset"
	userScoreKey   = "leaderboard:user_score"
	scoreCountKey  = "leaderboard:score_count"
	minScore       = 1
	maxScore       = 5000
	batchSize      = 5000
	leaderboardTop = 100
)

var rdb *redis.Client

func getRedisURL() string {
	if u := os.Getenv("REDIS_URL"); u != "" {
		return u
	}
	return "redis://localhost:6379"
}

func main() {
	opt, err := redis.ParseURL(getRedisURL())
	if err != nil {
		panic(err)
	}
	rdb = redis.NewClient(opt)
	defer rdb.Close()

	r := gin.Default()

	r.GET("/ping", ping)
	r.POST("/restore_random", restoreRandom)
	r.GET("/user/:user_id/score", getUserScore)
	r.GET("/user/:user_id/rank", getUserRank)
	r.GET("/score/:score/count", scoreCount)
	r.GET("/leaderboard", getLeaderboard)
	r.GET("/leaderboard/all", getLeaderboardAll)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	r.Run(":" + port)
}

func ping(c *gin.Context) {
	if err := rdb.Ping(c.Request.Context()).Err(); err != nil {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status": "error", "redis": "disconnected", "message": err.Error(),
		})
		return
	}
	c.JSON(http.StatusOK, gin.H{"status": "ok", "redis": "connected"})
}

func restoreRandom(c *gin.Context) {
	n, err := strconv.Atoi(c.Query("n"))
	if err != nil || n < 1 {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "n required as positive integer"})
		return
	}
	ctx := c.Request.Context()

	pipe := rdb.Pipeline()
	pipe.Del(ctx, zsetKey, userScoreKey, scoreCountKey)
	if _, err := pipe.Exec(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}

	scoreCounts := make(map[int]int)
	for i := 1; i <= n; i++ {
		userID := strconv.Itoa(i)
		score := minScore + rand.Intn(maxScore-minScore+1)
		pipe.ZAdd(ctx, zsetKey, redis.Z{Score: float64(score), Member: userID})
		pipe.HSet(ctx, userScoreKey, userID, score)
		scoreCounts[score]++
		if i%batchSize == 0 {
			pipe.Exec(ctx)
			pipe = rdb.Pipeline()
		}
	}
	if _, err := pipe.Exec(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}

	pipe = rdb.Pipeline()
	for score, count := range scoreCounts {
		pipe.HSet(ctx, scoreCountKey, strconv.Itoa(score), count)
	}
	if _, err := pipe.Exec(ctx); err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}

	c.JSON(http.StatusOK, gin.H{"status": "ok", "users_loaded": n})
}

func getUserScore(c *gin.Context) {
	userID := c.Param("user_id")
	val, err := rdb.HGet(c.Request.Context(), userScoreKey, userID).Result()
	if err == redis.Nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}
	score, _ := strconv.Atoi(val)
	c.JSON(http.StatusOK, gin.H{"user_id": userID, "score": score})
}

func getUserRank(c *gin.Context) {
	userID := c.Param("user_id")
	rank, err := rdb.ZRevRank(c.Request.Context(), zsetKey, userID).Result()
	if err == redis.Nil {
		c.JSON(http.StatusNotFound, gin.H{"detail": "User not found"})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}
	c.JSON(http.StatusOK, gin.H{"user_id": userID, "rank": rank + 1})
}

func scoreCount(c *gin.Context) {
	scoreStr := c.Param("score")
	score, err := strconv.Atoi(scoreStr)
	if err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "invalid score"})
		return
	}
	val, err := rdb.HGet(c.Request.Context(), scoreCountKey, scoreStr).Result()
	if err == redis.Nil {
		c.JSON(http.StatusOK, gin.H{"score": score, "count": 0})
		return
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}
	count, _ := strconv.Atoi(val)
	c.JSON(http.StatusOK, gin.H{"score": score, "count": count})
}

func getLeaderboard(c *gin.Context) {
	ctx := c.Request.Context()
	minRankQ := c.Query("min_rank")
	maxRankQ := c.Query("max_rank")
	minScoreQ := c.Query("min_score")
	maxScoreQ := c.Query("max_score")

	var minRank, maxRank *int
	if minRankQ != "" {
		v, _ := strconv.Atoi(minRankQ)
		minRank = &v
	}
	if maxRankQ != "" {
		v, _ := strconv.Atoi(maxRankQ)
		maxRank = &v
	}
	if minRank != nil && maxRank != nil && *minRank > *maxRank {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "min_rank must be <= max_rank"})
		return
	}

	var minScoreVal, maxScoreVal *int
	if minScoreQ != "" {
		v, _ := strconv.Atoi(minScoreQ)
		minScoreVal = &v
	}
	if maxScoreQ != "" {
		v, _ := strconv.Atoi(maxScoreQ)
		maxScoreVal = &v
	}
	if minScoreVal != nil && maxScoreVal != nil && *minScoreVal > *maxScoreVal {
		c.JSON(http.StatusBadRequest, gin.H{"detail": "min_score must be <= max_score"})
		return
	}

	var results []redis.Z
	var err error
	if minRank != nil && maxRank != nil {
		results, err = rdb.ZRevRangeWithScores(ctx, zsetKey, int64(*minRank-1), int64(*maxRank-1)).Result()
	} else if minScoreVal != nil || maxScoreVal != nil {
		minS := "-inf"
		maxS := "+inf"
		if minScoreVal != nil {
			minS = strconv.Itoa(*minScoreVal)
		}
		if maxScoreVal != nil {
			maxS = strconv.Itoa(*maxScoreVal)
		}
		results, err = rdb.ZRevRangeByScoreWithScores(ctx, zsetKey, &redis.ZRangeBy{Min: minS, Max: maxS}).Result()
	} else {
		results, err = rdb.ZRevRangeWithScores(ctx, zsetKey, 0, leaderboardTop-1).Result()
	}
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}

	rankOffset := 1
	if minRank != nil && maxRank != nil {
		rankOffset = *minRank
	} else if len(results) > 0 {
		if member, ok := results[0].Member.(string); ok {
			if r, e := rdb.ZRevRank(ctx, zsetKey, member).Result(); e == nil {
				rankOffset = int(r) + 1
			}
		}
	}

	leaderboard := make([]gin.H, 0, len(results))
	for i, z := range results {
		userID, _ := z.Member.(string)
		score := int(z.Score)
		if minRank != nil && maxRank != nil && (minScoreVal != nil || maxScoreVal != nil) {
			if minScoreVal != nil && score < *minScoreVal {
				continue
			}
			if maxScoreVal != nil && score > *maxScoreVal {
				continue
			}
		}
		leaderboard = append(leaderboard, gin.H{
			"rank": rankOffset + i, "user_id": userID, "score": score,
		})
	}
	c.JSON(http.StatusOK, gin.H{"leaderboard": leaderboard})
}

func getLeaderboardAll(c *gin.Context) {
	ctx := c.Request.Context()
	results, err := rdb.ZRevRangeWithScores(ctx, zsetKey, 0, -1).Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{"detail": err.Error()})
		return
	}
	out := make([]gin.H, len(results))
	for i, z := range results {
		userID, _ := z.Member.(string)
		out[i] = gin.H{"user_id": userID, "score": int(z.Score)}
	}
	c.JSON(http.StatusOK, gin.H{"users": out})
}
