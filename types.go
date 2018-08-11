package main

type RatingData struct {
	UserID    int
	MovieID   int
	Rating    float64
	Timestamp int
}
type UserGenreScore struct {
	UserID int    `json:"user_id"`
	Genre  string `json:"genre"`
	Score  int    `json:"score"`
}

func LessUserGenreScore(a, b UserGenreScore) bool {
	return a.Score < b.Score
}
