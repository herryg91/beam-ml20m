package main

import (
	"bufio"
	"context"
	"fmt"
	"strings"
	"sync"

	"github.com/apache/beam/sdks/go/pkg/beam"
	"github.com/apache/beam/sdks/go/pkg/beam/runners/direct"
	"github.com/herryg91/beam-ml20m/movie"
)

type pipelineParam struct {
	Rows          []string
	OutputFile    *bufio.Writer
	Lock          *sync.Mutex
	MovieInstance *movie.Instance
}

func processToPipeline(param pipelineParam) {
	p, s := beam.NewPipelineWithRoot()

	lines := beam.CreateList(s, param.Rows)
	p1 := GetUserGenreTendency(s, lines, param.MovieInstance)
	p2 := GetTopThreeUserGenreTendency(s, p1)

	beam.ParDo0(s, func(userID int, datas []UserGenreScore) {
		genres := []string{}
		for _, data := range datas {
			genres = append(genres, data.Genre)
		}

		param.Lock.Lock()
		param.OutputFile.WriteString(fmt.Sprintln(fmt.Sprintf("%d,%s", userID, strings.Join(genres, "|"))))
		param.OutputFile.Flush()
		param.Lock.Unlock()
	}, p2)

	direct.Execute(context.Background(), p)
}

func pipelineWorker(id int, jobs <-chan pipelineParam, wg *sync.WaitGroup) {
	for j := range jobs {
		processToPipeline(j)
		wg.Done()
	}
}
