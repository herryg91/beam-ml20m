package main

import (
	"fmt"
	"log"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/stats"
	"github.com/herryg91/beam-ml20m/movie"
)

func extractFileRating(row string, emit func(RatingData)) {
	var userID int
	var movieID int
	var rating float64
	var timestamp int
	_, err := fmt.Sscanf(row, "%d,%d,%f,%d", &userID, &movieID, &rating, &timestamp)
	if err != nil {
		log.Println("[error] extractFileRating:", err, row)
		return
	}
	emit(RatingData{userID, movieID, rating, timestamp})
}

type mapRatingToUserGenreTendency struct {
	MovieInstance *movie.Instance `json:"search"`
}

func (f *mapRatingToUserGenreTendency) ProcessElement(data RatingData, emit func(key UserGenreScore, score int)) {
	movieInfo := f.MovieInstance.GetMovieInfo(data.MovieID)
	for _, genre := range movieInfo.Genres {
		emit(UserGenreScore{data.UserID, genre, 1}, 1)
	}
}

func mapUpdateSummedScore(data UserGenreScore, score int, emit func(UserGenreScore)) {
	emit(UserGenreScore{data.UserID, data.Genre, score})
}

func GetUserGenreTendency(s beam.Scope, lines beam.PCollection, movieInstance *movie.Instance) beam.PCollection {
	s = s.Scope("GetUserGenreTendency")
	p1 := beam.ParDo(s, extractFileRating, lines)
	p2 := beam.ParDo(s, &mapRatingToUserGenreTendency{movieInstance}, p1)
	p3 := stats.SumPerKey(s, p2)
	p4 := beam.ParDo(s, mapUpdateSummedScore, p3)
	return p4
}
