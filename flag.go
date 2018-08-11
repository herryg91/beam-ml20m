package main

import "flag"

var (
	input  = flag.String("input", "", "File(s) to read.")
	output = flag.String("output", "", "Output file (required).")
)
