package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"sync"
	"time"

	"github.com/apache/beam/sdks/go/pkg/beam/io/filesystem/local"
	"github.com/herryg91/beam-ml20m/movie"
)

func main() {
	startLogging := time.Now()
	initFlag()
	local.New(context.Background())

	wg := &sync.WaitGroup{}

	jobs := make(chan pipelineParam, *workerSize)
	for w := 1; w <= *workerSize; w++ {
		go pipelineWorker(w, jobs, wg)
	}

	movieInstance := movie.New()

	/*Setup Output File*/
	fileOutput, err := os.OpenFile(*output, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0600)
	if err != nil {
		panic(err)
	}
	defer fileOutput.Close()
	bufOutput := bufio.NewWriter(fileOutput)

	/* Setup Read File*/
	fileInput, _ := os.Open(*input)
	defer fileInput.Close()
	scanner := bufio.NewScanner(fileInput)
	scanner.Split(bufio.ScanLines)

	/*Batching File & Sent it to Pipeline*/
	currentUserID := -1
	totalUid := 0
	datasToBeProcessed := []string{}
	lock := &sync.Mutex{}

	for scanner.Scan() {
		line := scanner.Text()
		var uid int
		_, errParse := fmt.Sscanf(line, "%d,", &uid)
		if errParse != nil {
			continue
		}

		if currentUserID != uid {
			if totalUid >= *batchSize {
				if len(datasToBeProcessed) > 0 {
					wg.Add(1)
					jobs <- pipelineParam{Rows: datasToBeProcessed, OutputFile: bufOutput, Lock: lock, MovieInstance: movieInstance}
				}
				datasToBeProcessed = datasToBeProcessed[:0]
				totalUid = 0
			}
			currentUserID = uid
			totalUid++
		}

		datasToBeProcessed = append(datasToBeProcessed, line)
	}
	if len(datasToBeProcessed) > 0 {
		wg.Add(1)
		jobs <- pipelineParam{Rows: datasToBeProcessed, OutputFile: bufOutput, Lock: lock, MovieInstance: movieInstance}
	}
	close(jobs)

	wg.Wait()
	log.Println("Pipeline Done in: ", time.Since(startLogging).Seconds(), "second")
}
