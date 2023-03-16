package main

import (
	"bufio"
	"fmt"
	"github.com/minus5/svckit/log"
	"os"
	"sort"
	"strconv"
	"strings"
)

const (
	firstFileId  = "DBID"
	secondFileId = "MongoID"
)

// Data from first file will be written starting from the left side to the right, the id column of the second file
// is ignored.
// Script expects a header
func main() {
	file1, err := os.Open("./input/input1.csv")
	if err != nil {
		log.Error(err)
		return
	}
	file2, err := os.Open("./input/input2.csv")
	if err != nil {
		log.Error(err)
		return
	}

	headers1 := make(map[string]int)
	headers2 := make(map[string]int)
	scanner1 := bufio.NewScanner(file1)
	scanner2 := bufio.NewScanner(file2)
	if !scanner1.Scan() || !scanner2.Scan() {
		return
	}
	extractHeaders := func(ss string, m map[string]int) {
		for i, s := range strings.Split(ss, ",") {
			cleaned := s
			if strings.HasSuffix(cleaned, "\"") {
				cleaned = strings.TrimSuffix(cleaned, "\"")
			}
			if strings.HasPrefix(cleaned, "\"") {
				cleaned = strings.TrimPrefix(cleaned, "\"")
			}

			m[cleaned] = i
		}
	}

	extractHeaders(scanner1.Text(), headers1)
	extractHeaders(scanner2.Text(), headers2)

	quote := func(s string) string {
		out := s
		if !strings.HasSuffix(out, "\"") {
			out = out + "\""
		}
		if !strings.HasPrefix(out, "\"") {
			out = "\"" + out
		}
		if out == "\"" {
			out += "\""
		}
		return out
	}

	header := make([]string, len(headers1)+len(headers1)-1)
	header[0] = "ID"
	for k, v := range headers1 {
		if k == firstFileId {
			continue
		}

		header[v] = quote(k)
	}
	for k, v := range headers2 {
		if k == secondFileId {
			continue
		}
		shifter := 0
		if v > headers2[secondFileId] {
			shifter = 1
		}
		header[v+len(headers1)-shifter] = quote(k)
	}

	output := []string{}
	output = append(output, strings.Join(header, ","))
	m1 := make(map[int]map[string]string)
	m2 := make(map[int]map[string]string)

	for scanner1.Scan() && scanner2.Scan() {
		str1 := strings.Split(scanner1.Text(), "\",\"")
		i, _ := strconv.Atoi(strings.TrimPrefix(str1[0], "\""))
		if m1[i] == nil {
			m1[i] = make(map[string]string)
		}
		for k, v := range headers1 {
			if len(str1) >= v {
				m1[i][k] = str1[v]
			} else {
				m1[i][k] = ""
			}
		}

		str2 := strings.Split(scanner2.Text(), ",\"")
		i, _ = strconv.Atoi(str2[0])
		if m2[i] == nil {
			m2[i] = make(map[string]string)
		}
		fmt.Println(str2)
		for k, v := range headers2 {
			if len(str2) >= v {
				m2[i][k] = str2[v]
			} else {
				m2[i][k] = ""
			}
		}
	}

	keys := make(map[int]struct{})
	for k, _ := range m1 {
		keys[k] = struct{}{}
	}
	for k, _ := range m2 {
		keys[k] = struct{}{}
	}

	sortKeys := func(m map[string]int, exclusion string) []string {
		keys := make([]string, 0, len(m)-1)
		for h := range m {
			if h == exclusion {
				continue
			}
			keys = append(keys, h)
		}
		sort.Slice(keys, func(i, j int) bool {
			return m[keys[i]] < m[keys[j]]
		})

		return keys
	}

	for id, _ := range keys {
		s := []string{}
		s = append(s, quote(fmt.Sprint(id)))
		m := m1[id]
		keys := sortKeys(headers1, firstFileId)
		for _, k := range keys {
			if m == nil {
				s = append(s, quote(""))
				continue
			}
			s = append(s, quote(m[k]))
		}

		m = m2[id]
		keys = sortKeys(headers2, secondFileId)
		for _, k := range keys {
			if m == nil {
				s = append(s, quote(""))
				continue
			}
			s = append(s, quote(m[k]))
		}

		output = append(output, strings.Join(s, ","))
	}

	out, err := os.Create("./conc_output")

	if err != nil {
		log.Error(err)
		return
	}

	out.WriteString(strings.Join(output, "\n"))
}
