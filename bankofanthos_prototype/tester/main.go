package main

import (
	"flag"
	"fmt"
	"log"
	"strconv"
	"strings"
)

const reqLog = "reqlog.json" //req log file

func main() {
	var countsList string

	flag.StringVar(&countsList, "counts", "5", "Req count per user, must be >= 3, split by,")
	flag.Parse()

	var counts []int
	for _, c := range strings.Split(countsList, ",") {
		c = strings.TrimSpace(c)
		i, err := strconv.Atoi(c)
		if err != nil {
			log.Panicf("failed to parse count, err=%s", err)
		}
		if i < 3 || i > 100 {
			fmt.Println("WARNING: Please provide req count between 3 to 100")
			return
		}
		counts = append(counts, i)
	}

	g, err := newGenerator(counts)
	if err != nil {
		log.Panic(err)
	}

	if err := g.generate(); err != nil {
		log.Panicf("failed to generate requests, err=%s", err)
	}
}
