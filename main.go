package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local"
	"github.com/apache/beam/sdks/go/pkg/beam/io/textio"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/direct"
	"github.com/herryg91/beam-ml20m/movie"
)

func main() {
	startLogging := time.Now()
	flag.Parse()

	// beam.Init()
	p := beam.NewPipeline()
	s := p.Root()
	local.New(context.Background())

	lines := textio.Read(s, *input)

	movieInstance := movie.New()

	p1 := GetUserGenreTendency(s, lines, movieInstance)
	p2 := GetTopThreeUserGenreTendency(s, p1)

	outputToWrite := beam.ParDo(s, func(userID int, datas []UserGenreScore, emit func(string)) {
		genres := []string{}
		for _, data := range datas {
			genres = append(genres, data.Genre)
		}
		emit(fmt.Sprintf("%d,%s", userID, strings.Join(genres, "|")))
	}, p2)
	textio.Write(s, *output, outputToWrite)

	direct.Execute(context.Background(), p)

	log.Println("Pipeline Done in: ", time.Since(startLogging).Seconds())
}
