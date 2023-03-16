package main

import (
	"bufio"
	"github.com/minus5/svckit/log"
	"os"
	"strings"
)

func main() {
	file, err := os.Open("./input/input.csv")
	if err != nil {
		log.Error(err)
		return
	}

	scanner := bufio.NewScanner(file)
	ss := []string{}
	output := []string{}
	counter := 0
	for scanner.Scan() {
		s := scanner.Text()
		ss = append(ss, strings.Split(s, ",")[0])
		counter++
		if counter == 1000 {
			output = append(output, "INSERT INTO #table(id) VALUES ("+strings.Join(ss, "),(")+");")
			ss = []string{}
			counter = 0
		}
	}

	out, err := os.Create("./output")

	if err != nil {
		log.Error(err)
		return
	}

	out.WriteString(strings.Join(output, "\n"))
}
