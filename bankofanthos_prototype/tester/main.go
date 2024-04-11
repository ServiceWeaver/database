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
	var counts string

	flag.StringVar(&counts, "counts", "6, 9", "Req count per user, must be >= 3, split by,")
	flag.Parse()

	var count []int
	for _, c := range strings.Split(counts, ",") {
		c = strings.ReplaceAll(c, " ", "") // remove extra space
		i, err := strconv.Atoi(c)
		if err != nil {
			log.Panicf("failed to parse count, err=%s", err)
		}
		if i < 3 || i > 100 {
			fmt.Println("WARNING: Please provide req count between 3 to 100")
			return
		}
		count = append(count, i)
	}

	g, err := newGenerator(count)
	if err != nil {
		log.Panic(err)
	}

	if err := g.generate(); err != nil {
		log.Panicf("failed to generate requests, err=%s", err)
	}
}
