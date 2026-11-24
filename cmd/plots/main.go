// Command plots renders the charts in docs/img from the raw receipt lines in
// docs/*-results.txt. It uses only the standard library and is deterministic:
// the same receipts always produce the same bytes.
package main

import (
	"bufio"
	"flag"
	"fmt"
	"html"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// trial is one receipt line: space separated key=value pairs.
type trial map[string]string

func (t trial) num(key string) (float64, bool) {
	v, err := strconv.ParseFloat(t[key], 64)
	return v, err == nil
}

// parseTrials reads every line that contains an engine= field.
func parseTrials(path string) ([]trial, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	var out []trial
	sc := bufio.NewScanner(f)
	sc.Buffer(make([]byte, 1<<16), 1<<20)
	for sc.Scan() {
		t := trial{}
		for _, field := range strings.Fields(sc.Text()) {
			if k, v, ok := strings.Cut(field, "="); ok {
				t[k] = v
			}
		}
		if len(t) > 0 {
			out = append(out, t)
		}
	}
	return out, sc.Err()
}

func parseMeta(path string) (map[string]string, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer f.Close()
	meta := map[string]string{}
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		if k, v, ok := strings.Cut(sc.Text(), "="); ok && !strings.HasPrefix(k, "#") {
			meta[strings.TrimSpace(k)] = strings.TrimSpace(v)
		}
	}
	return meta, sc.Err()
}

func median(v []float64) float64 {
	s := append([]float64(nil), v...)
	sort.Float64s(s)
	if len(s) == 0 {
		return 0
	}
	if len(s)%2 == 1 {
		return s[len(s)/2]
	}
	return (s[len(s)/2-1] + s[len(s)/2]) / 2
}

// series is one engine's values at each producer count.
type series struct {
	engine string
	values map[int][]float64
}

func collect(trials []trial, key string, scale float64) []series {
	byEngine := map[string]*series{}
	for _, t := range trials {
		w, err := strconv.Atoi(t["workers"])
		v, ok := t.num(key)
		if t["engine"] == "" || err != nil || !ok {
			continue
		}
		s := byEngine[t["engine"]]
		if s == nil {
			s = &series{engine: t["engine"], values: map[int][]float64{}}
			byEngine[t["engine"]] = s
		}
		s.values[w] = append(s.values[w], v*scale)
	}
	var out []series
	for _, s := range byEngine {
		out = append(out, *s)
	}
	// Dostoevsky first so it always takes the first palette slot.
	sort.Slice(out, func(i, j int) bool {
		if (out[i].engine == "dostoevsky") != (out[j].engine == "dostoevsky") {
			return out[i].engine == "dostoevsky"
		}
		return out[i].engine < out[j].engine
	})
	return out
}

type chart struct {
	file, title, unit, alt, note string
	key                          string
	scale                        float64
	decimals                     int
}

var gateCharts = []chart{
	{file: "throughput.svg", title: "Synchronous publish throughput", unit: "messages per second", key: "records/s", scale: 1, decimals: 0,
		alt:  "Grouped bars of median messages per second at 1, 8 and 32 producers for Dostoevsky and embedded JetStream, with each trial as a dot.",
		note: "Higher is better."},
	{file: "memory.svg", title: "Peak process memory", unit: "MiB peak RSS", key: "peakRSS_KiB", scale: 1.0 / 1024, decimals: 1,
		alt:  "Grouped bars of median peak resident memory in MiB at 1, 8 and 32 producers for Dostoevsky and embedded JetStream, with each trial as a dot.",
		note: "Lower is better."},
	{file: "p99-latency.svg", title: "p99 publish latency", unit: "milliseconds", key: "p99_us", scale: 1.0 / 1000, decimals: 2,
		alt:  "Grouped bars of median p99 publish-to-acknowledgement latency in milliseconds at 1, 8 and 32 producers for Dostoevsky and embedded JetStream, with each trial as a dot.",
		note: "Lower is better."},
	{file: "reopen.svg", title: "Reopen and verify time", unit: "milliseconds", key: "reopen_ms", scale: 1, decimals: 1,
		alt:  "Grouped bars of median reopen time in milliseconds at 1, 8 and 32 producers. Embedded JetStream reopens faster than Dostoevsky.",
		note: "Lower is better. JetStream is faster here: Dostoevsky scans its journal on open."},
}

func main() {
	receipt := flag.String("receipt", "docs/publisher-gate-results.txt", "raw trial lines of the publisher gate")
	metaPath := flag.String("meta", "docs/publisher-gate-meta.txt", "key=value description of the run that produced the receipt")
	sketchPath := flag.String("sketch", "docs/sketch-accuracy-results.txt", "sketch accuracy receipt; skipped when absent")
	out := flag.String("out", "docs/img", "output directory")
	flag.Parse()
	if err := run(*receipt, *metaPath, *sketchPath, *out); err != nil {
		fmt.Fprintln(os.Stderr, "plots:", err)
		os.Exit(1)
	}
}

func run(receipt, metaPath, sketchPath, out string) error {
	trials, err := parseTrials(receipt)
	if err != nil {
		return err
	}
	meta, err := parseMeta(metaPath)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(out, 0o755); err != nil {
		return err
	}
	footer := footerLines(meta, receipt)
	for _, c := range gateCharts {
		data := collect(trials, c.key, c.scale)
		if len(data) == 0 {
			return fmt.Errorf("%s: no %q values in %s", c.file, c.key, receipt)
		}
		if err := os.WriteFile(filepath.Join(out, c.file), []byte(renderBars(c, data, footer)), 0o644); err != nil {
			return err
		}
	}
	if err := os.WriteFile(filepath.Join(out, "architecture.svg"), []byte(renderArchitecture()), 0o644); err != nil {
		return err
	}
	if sk, err := parseTrials(sketchPath); err == nil {
		svg := renderSketch(sk, []string{"Receipt: " + sketchPath, "Command: " + meta["sketch_command"]})
		return os.WriteFile(filepath.Join(out, "sketch-accuracy.svg"), []byte(svg), 0o644)
	} else if !os.IsNotExist(err) {
		return err
	}
	fmt.Fprintln(os.Stderr, "plots: no", sketchPath, "so sketch-accuracy.svg was not written")
	return nil
}

func footerLines(meta map[string]string, receipt string) []string {
	return []string{
		fmt.Sprintf("%s; %s; Go %s; GOMAXPROCS=%s; CGO_ENABLED=%s", meta["machine"], meta["os"], meta["go"], meta["gomaxprocs"], meta["cgo"]),
		fmt.Sprintf("Payload %s bytes; %s records per trial; %s; one topic, one replica; JetStream %s", meta["payload_bytes"], meta["records"], meta["sync"], meta["jetstream"]),
		fmt.Sprintf("Receipt: %s. Command: %s", receipt, meta["command"]),
	}
}

const (
	width, height          = 760, 460
	left, right, top, base = 72, 24, 64, 340
)

// Colors are CSS custom properties so the same file works in light and dark.
const style = `<style>
:root{--fg:#1f2328;--muted:#59636e;--grid:#d1d9e0;--a:#0b6bcb;--b:#c2410c}
@media (prefers-color-scheme:dark){:root{--fg:#e6edf3;--muted:#9198a1;--grid:#3d444d;--a:#58a6ff;--b:#f0883e}}
text{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;fill:var(--fg)}
.m{fill:var(--muted)}.g{stroke:var(--grid);stroke-width:1}
.s0{fill:var(--a)}.s1{fill:var(--b)}.d0{fill:var(--fg);stroke:var(--a);stroke-width:1.5}.d1{fill:var(--fg);stroke:var(--b);stroke-width:1.5}
</style>`

// niceMax rounds up to 1, 2, 2.5, 5 or 10 times a power of ten.
func niceMax(v float64) float64 {
	if v <= 0 {
		return 1
	}
	p := 1.0
	for p*10 <= v {
		p *= 10
	}
	for p > v {
		p /= 10
	}
	for _, m := range []float64{1, 2, 2.5, 5, 10} {
		if m*p >= v {
			return m * p
		}
	}
	return 10 * p
}

func format(v float64, decimals int) string {
	s := strconv.FormatFloat(v, 'f', decimals, 64)
	if decimals == 0 {
		return commas(s)
	}
	return s
}

func commas(s string) string {
	n := len(s)
	if n <= 3 {
		return s
	}
	var b strings.Builder
	for i, r := range s {
		if i > 0 && (n-i)%3 == 0 {
			b.WriteByte(',')
		}
		b.WriteRune(r)
	}
	return b.String()
}

func label(engine string) string {
	switch engine {
	case "dostoevsky":
		return "Dostoevsky"
	case "jetstream":
		return "Embedded JetStream"
	}
	return engine
}

func renderBars(c chart, data []series, footer []string) string {
	producers := map[int]bool{}
	maxV := 0.0
	for _, s := range data {
		for w, vs := range s.values {
			producers[w] = true
			for _, v := range vs {
				if v > maxV {
					maxV = v
				}
			}
		}
	}
	var ws []int
	for w := range producers {
		ws = append(ws, w)
	}
	sort.Ints(ws)
	top1 := niceMax(maxV)
	plotW, plotH := float64(width-left-right), float64(base-top)
	y := func(v float64) float64 { return float64(base) - v/top1*plotH }

	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d" role="img" aria-labelledby="t d">`+"\n", width, height, width, height)
	fmt.Fprintf(&b, "<title id=\"t\">%s</title><desc id=\"d\">%s</desc>\n%s\n", html.EscapeString(c.title), html.EscapeString(c.alt), style)
	fmt.Fprintf(&b, `<text x="%d" y="26" font-size="18" font-weight="600">%s</text>`+"\n", left, html.EscapeString(c.title))
	fmt.Fprintf(&b, `<text x="%d" y="46" font-size="12" class="m">%s. Bars are medians; dots are trials. %s</text>`+"\n", left, html.EscapeString(c.unit), html.EscapeString(c.note))
	for i := 0; i <= 4; i++ {
		v := top1 * float64(i) / 4
		fmt.Fprintf(&b, `<line class="g" x1="%d" x2="%d" y1="%.1f" y2="%.1f"/><text class="m" x="%d" y="%.1f" font-size="11" text-anchor="end">%s</text>`+"\n",
			left, width-right, y(v), y(v), left-8, y(v)+4, format(v, tickDecimals(top1)))
	}
	groupW := plotW / float64(len(ws))
	barW := groupW * 0.3
	for gi, w := range ws {
		gx := float64(left) + groupW*float64(gi)
		fmt.Fprintf(&b, `<text x="%.1f" y="%d" font-size="13" text-anchor="middle">%d producer%s</text>`+"\n", gx+groupW/2, base+22, w, plural(w))
		for si, s := range data {
			vs := s.values[w]
			if len(vs) == 0 {
				continue
			}
			m := median(vs)
			x := gx + groupW/2 + (float64(si)-float64(len(data))/2)*(barW+6) + 3
			fmt.Fprintf(&b, `<rect class="s%d" x="%.1f" y="%.1f" width="%.1f" height="%.1f"><title>%s, %d producers: median %s</title></rect>`+"\n",
				si, x, y(m), barW, float64(base)-y(m), label(s.engine), w, format(m, c.decimals))
			fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" font-size="12" text-anchor="middle" font-weight="600">%s</text>`+"\n", x+barW/2, y(m)-6-dotRoom(vs, y, m), format(m, c.decimals))
			sorted := append([]float64(nil), vs...)
			sort.Float64s(sorted)
			for ti, v := range sorted {
				dx := x + barW/2 + (float64(ti)-float64(len(sorted)-1)/2)*7
				fmt.Fprintf(&b, `<circle class="d%d" cx="%.1f" cy="%.1f" r="3"/>`+"\n", si, dx, y(v))
			}
		}
	}
	for si, s := range data {
		lx := left + si*190
		fmt.Fprintf(&b, `<rect class="s%d" x="%d" y="%d" width="12" height="12"/><text x="%d" y="%d" font-size="12">%s</text>`+"\n", si, lx, base+40, lx+18, base+50, label(s.engine))
	}
	for i, line := range footer {
		fmt.Fprintf(&b, `<text class="m" x="%d" y="%d" font-size="10.5">%s</text>`+"\n", left, base+74+i*15, html.EscapeString(line))
	}
	b.WriteString("</svg>\n")
	return b.String()
}

// dotRoom lifts the value label above the highest trial dot.
func dotRoom(vs []float64, y func(float64) float64, m float64) float64 {
	hi := m
	for _, v := range vs {
		if v > hi {
			hi = v
		}
	}
	return y(m) - y(hi)
}

func tickDecimals(top float64) int {
	if top >= 10 {
		return 0
	}
	if top >= 1 {
		return 1
	}
	return 2
}

func plural(n int) string {
	if n == 1 {
		return ""
	}
	return "s"
}
