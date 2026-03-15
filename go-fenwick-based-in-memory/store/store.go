package store

import (
	"math/rand"
	"sort"
	"strconv"
	"sync"
)

const (
	MinScore       = 1
	MaxScore       = 5000
	LeaderboardTop = 100
)

type fenwickTree struct {
	n   int
	bit []int
}

func newFenwick(n int) *fenwickTree {
	return &fenwickTree{
		n:   n,
		bit: make([]int, n+2),
	}
}

func (f *fenwickTree) update(idx, delta int) {
	if idx <= 0 {
		return
	}
	for i := idx; i <= f.n; i += i & -i {
		f.bit[i] += delta
	}
}

func (f *fenwickTree) query(idx int) int {
	if idx <= 0 {
		return 0
	}
	if idx > f.n {
		idx = f.n
	}
	sum := 0
	for i := idx; i > 0; i -= i & -i {
		sum += f.bit[i]
	}
	return sum
}

func insertSorted(bucket []string, id string) []string {
	i := sort.SearchStrings(bucket, id)
	bucket = append(bucket, "")
	copy(bucket[i+1:], bucket[i:])
	bucket[i] = id
	return bucket
}

func removeFromSorted(bucket []string, id string) []string {
	i := sort.SearchStrings(bucket, id)
	if i < len(bucket) && bucket[i] == id {
		return append(bucket[:i], bucket[i+1:]...)
	}
	return bucket
}

// Store holds in-memory leaderboard state (Fenwick + score buckets).
type Store struct {
	userScore    map[string]int
	scoreBuckets [][]string
	scoreCount   []int
	totalUsers   int
	tree         *fenwickTree
	mu           sync.RWMutex
}

// New creates a new Store (for direct testing; server uses global).
func New() *Store {
	s := &Store{
		userScore:    make(map[string]int),
		scoreBuckets: make([][]string, MaxScore+1),
		scoreCount:   make([]int, MaxScore+1),
		tree:         newFenwick(MaxScore),
	}
	for i := range s.scoreBuckets {
		s.scoreBuckets[i] = make([]string, 0, 4)
	}
	return s
}

func (s *Store) reset() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.userScore = make(map[string]int)
	for i := range s.scoreBuckets {
		s.scoreBuckets[i] = s.scoreBuckets[i][:0]
	}
	for i := range s.scoreCount {
		s.scoreCount[i] = 0
	}
	for i := range s.tree.bit {
		s.tree.bit[i] = 0
	}
	s.totalUsers = 0
}

// AddUser adds or updates a user's score (used by RestoreRandom).
func (s *Store) AddUser(id string, score int) {
	if score < MinScore || score > MaxScore {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if old, ok := s.userScore[id]; ok {
		if old >= MinScore && old <= MaxScore {
			s.scoreBuckets[old] = removeFromSorted(s.scoreBuckets[old], id)
			s.scoreCount[old]--
			s.tree.update(old, -1)
			s.totalUsers--
		}
	}
	s.userScore[id] = score
	s.scoreBuckets[score] = insertSorted(s.scoreBuckets[score], id)
	s.scoreCount[score]++
	s.tree.update(score, 1)
	s.totalUsers++
}

// RestoreRandom fills the store with n users with random scores (uses global rand).
func (s *Store) RestoreRandom(n int, rng *rand.Rand) {
	s.reset()
	for i := 1; i <= n; i++ {
		userID := strconv.Itoa(i)
		score := MinScore + rng.Intn(MaxScore-MinScore+1)
		s.AddUser(userID, score)
	}
}

// Ping is a no-op for measuring call overhead in direct tests.
func (s *Store) Ping() {}

// GetUserScore returns score and true if user exists.
func (s *Store) GetUserScore(userID string) (int, bool) {
	s.mu.RLock()
	score, ok := s.userScore[userID]
	s.mu.RUnlock()
	return score, ok
}

// GetUserRank returns 1-based rank and true if user exists.
func (s *Store) GetUserRank(userID string) (int, bool) {
	s.mu.RLock()
	score, ok := s.userScore[userID]
	localTotal := s.totalUsers
	s.mu.RUnlock()
	if !ok {
		return 0, false
	}
	usersLeq := s.tree.query(score)
	rank := localTotal - usersLeq + 1
	return rank, true
}

// GetScoreCount returns number of users with the given score.
func (s *Store) GetScoreCount(score int) int {
	if score < MinScore || score > MaxScore {
		return 0
	}
	s.mu.RLock()
	c := s.scoreCount[score]
	s.mu.RUnlock()
	return c
}

// LeaderboardEntry is one row in leaderboard results.
type LeaderboardEntry struct {
	Rank   int
	UserID string
	Score  int
}

// GetLeaderboard returns entries for the rank range, optionally filtered by score range.
func (s *Store) GetLeaderboard(startRank, endRank int, minScoreVal, maxScoreVal *int) []LeaderboardEntry {
	needCount := endRank - startRank + 1
	if needCount <= 0 {
		return nil
	}
	skipCount := startRank - 1
	s.mu.RLock()
	result := make([]LeaderboardEntry, 0, needCount)
	rank := 0
	for sc := MaxScore; sc >= MinScore && len(result) < needCount; sc-- {
		bucket := s.scoreBuckets[sc]
		for _, id := range bucket {
			rank++
			if rank <= skipCount {
				continue
			}
			if minScoreVal != nil && sc < *minScoreVal {
				continue
			}
			if maxScoreVal != nil && sc > *maxScoreVal {
				continue
			}
			result = append(result, LeaderboardEntry{
				Rank:   startRank + len(result),
				UserID: id,
				Score:  sc,
			})
			if len(result) >= needCount {
				break
			}
		}
	}
	s.mu.RUnlock()
	return result
}

// LeaderboardAllEntry is one row for /leaderboard/all.
type LeaderboardAllEntry struct {
	UserID string
	Score  int
}

// GetLeaderboardAll returns all users sorted by score descending.
func (s *Store) GetLeaderboardAll() []LeaderboardAllEntry {
	s.mu.RLock()
	n := s.totalUsers
	if n == 0 {
		s.mu.RUnlock()
		return nil
	}
	out := make([]LeaderboardAllEntry, 0, n)
	for sc := MaxScore; sc >= MinScore; sc-- {
		for _, id := range s.scoreBuckets[sc] {
			out = append(out, LeaderboardAllEntry{UserID: id, Score: sc})
		}
	}
	s.mu.RUnlock()
	return out
}
