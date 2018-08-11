package main

import (
	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/transforms/top"
)

func mapKVByUserID(data UserGenreScore, emit func(userID int, value UserGenreScore)) {
	emit(data.UserID, data)
}

func GetTopThreeUserGenreTendency(s beam.Scope, lines beam.PCollection) beam.PCollection {
	s = s.Scope("GetTopThreeUserGenreTendency")
	p1 := beam.ParDo(s, mapKVByUserID, lines)
	p2 := top.LargestPerKey(s, p1, 3, LessUserGenreScore)
	return p2
}
