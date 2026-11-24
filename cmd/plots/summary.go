package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"strconv"
	"strings"
)

// summarize prints the medians of a comparison log, one line per engine and
// producer count, after validating it strictly: this is the one validator for
// benchmark receipts, shared with the plotter's group checks. It fails, naming
// every problem, on empty input, a stray token, a duplicate or non-canonical
// number, a FAILED trial, a missing engine and producer group or the wrong trial
// count. With allowIncomplete it prints the problems first and still prints the
// medians of the valid trials. Medians only: no ranking and no verdict.
func summarize(w io.Writer, path string, want int, producers []int, allowIncomplete bool) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	type key struct {
		engine  string
		workers int
	}
	type metrics struct {
		rps, p99, rss, reopen []float64
		trials                map[int]bool
	}
	groups := map[key]*metrics{}
	var order []key
	engines := map[string]bool{}
	var engineOrder []string
	var problems []string
	problem := func(format string, args ...any) { problems = append(problems, fmt.Sprintf(format, args...)) }
	lines, failed := 0, 0

	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<16), 1<<20)
	for n := 1; sc.Scan(); n++ {
		line := strings.TrimSpace(sc.Text())
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if !strings.HasPrefix(fields[0], "trial=") {
			problem("line %d: not a trial line: %.120s", n, line)
			continue
		}
		lines++
		kv := map[string]string{}
		malformed := ""
		for _, field := range fields {
			if field == "FAILED" {
				continue
			}
			k, v, ok := strings.Cut(field, "=")
			if !ok || k == "" {
				if malformed == "" {
					malformed = fmt.Sprintf("stray token %q", field)
				}
				continue
			}
			if _, dup := kv[k]; dup && malformed == "" {
				malformed = fmt.Sprintf("field %q repeated", k)
			}
			kv[k] = v
		}
		if containsWord(fields, "FAILED") {
			failed++
			if e := kv["engine"]; e != "" && !engines[e] {
				engines[e] = true
				engineOrder = append(engineOrder, e)
			}
			problem("line %d: FAILED trial (%s, %s producers): %.120s", n, kv["engine"], kv["workers"], line)
			continue
		}
		engine := kv["engine"]
		trial, trialErr := canonicalInt(kv["trial"])
		workers, workersErr := canonicalInt(kv["workers"])
		rss := kv["peakRSS_KiB"]
		if v, ok := kv["total_rss_KiB"]; ok {
			rss = v
		}
		vals := make([]float64, 4)
		for i, raw := range []string{kv["records/s"], kv["p99_us"], rss, kv["reopen_ms"]} {
			v, err := strconv.ParseFloat(raw, 64)
			if err != nil || math.IsNaN(v) || math.IsInf(v, 0) || v < 0 {
				malformed = fmt.Sprintf("value %q is not a non-negative number", raw)
				break
			}
			vals[i] = v
		}
		switch {
		case malformed != "":
		case engine == "":
			malformed = "no engine"
		case trialErr != nil:
			malformed = "trial: " + trialErr.Error()
		case workersErr != nil:
			malformed = "workers: " + workersErr.Error()
		}
		if malformed != "" {
			problem("line %d: malformed trial line (%s): %.120s", n, malformed, line)
			continue
		}
		if !engines[engine] {
			engines[engine] = true
			engineOrder = append(engineOrder, engine)
		}
		k := key{engine, workers}
		m := groups[k]
		if m == nil {
			m = &metrics{trials: map[int]bool{}}
			groups[k] = m
			order = append(order, k)
		}
		if m.trials[trial] {
			problem("line %d: duplicate trial number %d for %s at %d producers", n, trial, engine, workers)
			continue
		}
		m.trials[trial] = true
		m.rps, m.p99, m.rss, m.reopen = append(m.rps, vals[0]), append(m.p99, vals[1]), append(m.rss, vals[2]), append(m.reopen, vals[3])
	}
	if err := sc.Err(); err != nil {
		return err
	}
	if lines == 0 {
		problem("no trial lines in %s", path)
	}
	for _, e := range engineOrder {
		for _, p := range producers {
			m := groups[key{e, p}]
			switch {
			case m == nil:
				problem("missing: %s at %d producers has no valid trial", e, p)
			case len(m.trials) != want:
				problem("%s at %d producers has %d valid trials, want %d", e, p, len(m.trials), want)
			}
		}
	}
	if len(problems) > 0 && !allowIncomplete {
		return errors.New("receipt rejected:\n  " + strings.Join(problems, "\n  "))
	}
	for _, p := range problems {
		fmt.Fprintln(w, "PROBLEM: "+p)
	}
	fmt.Fprintf(w, "lines=%d failed=%d\n", lines, failed)
	for _, k := range order {
		m := groups[k]
		fmt.Fprintf(w, "engine=%s workers=%d trials=%d records/s=%s p99_us=%s total_rss_KiB=%s reopen_ms=%s\n", k.engine, k.workers, len(m.trials), med(m.rps), med(m.p99), med(m.rss), med(m.reopen))
	}
	return nil
}

func containsWord(fields []string, word string) bool {
	for _, f := range fields {
		if f == word {
			return true
		}
	}
	return false
}

// canonicalInt parses a non-negative integer written without a sign or leading
// zeros, so "1" and "01" cannot both name trial 1.
func canonicalInt(s string) (int, error) {
	n, err := strconv.Atoi(s)
	if err != nil || n < 0 || strconv.Itoa(n) != s {
		return 0, fmt.Errorf("%q is not a canonical non-negative integer", s)
	}
	return n, nil
}

// med formats the median of v: the middle value, or the mean of the middle two.
func med(v []float64) string {
	if len(v) == 0 {
		return "NA"
	}
	return strconv.FormatFloat(median(v), 'f', -1, 64)
}

// parseProducers reads a comma separated list such as "1,8,32".
func parseProducers(s string) ([]int, error) {
	var out []int
	for _, part := range strings.Split(s, ",") {
		n, err := canonicalInt(strings.TrimSpace(part))
		if err != nil || n < 1 {
			return nil, fmt.Errorf("-expect-producers: %q is not a producer count", part)
		}
		out = append(out, n)
	}
	sort.Ints(out)
	return out, nil
}
