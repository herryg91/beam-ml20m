package main

import "flag"

var (
	input      = flag.String("input", "", "File(s) to read (required)")
	output     = flag.String("output", "", "Output file (required)")
	workerSize = flag.Int("worker", 1, "Data processing worker default: 1")
	batchSize  = flag.Int("batch", 1000, "user id minibatch size default: 1000")
)

func initFlag() {
	flag.Parse()
}
