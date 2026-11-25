package main

import (
	"errors"
	"fmt"
	"html"
	"sort"
	"strings"
)

// styleMulti extends style with eight series colors (a color-blind-safe set, one per
// engine in engineOrder, in light and dark) and a color for failed trials.
const styleMulti = `<style>
:root{--bg:#ffffff;--fg:#1f2328;--muted:#59636e;--grid:#d1d9e0;--fail:#b42318;--c0:#0b6bcb;--c1:#c2410c;--c2:#00875a;--c3:#b8479b;--c4:#a16207;--c5:#0891b2;--c6:#6d28d9;--c7:#475569}
@media (prefers-color-scheme:dark){:root{--bg:#0d1117;--fg:#e6edf3;--muted:#9198a1;--grid:#3d444d;--fail:#ff7b72;--c0:#58a6ff;--c1:#f0883e;--c2:#3fb950;--c3:#f778ba;--c4:#e3b341;--c5:#39c5cf;--c6:#a371f7;--c7:#8b949e}}
rect.bg{fill:var(--bg)}
text{font-family:-apple-system,BlinkMacSystemFont,"Segoe UI",Helvetica,Arial,sans-serif;fill:var(--fg)}
.m{fill:var(--muted)}.f{fill:var(--fail)}.g{stroke:var(--grid);stroke-width:1}
.s0{fill:var(--c0)}.s1{fill:var(--c1)}.s2{fill:var(--c2)}.s3{fill:var(--c3)}.s4{fill:var(--c4)}.s5{fill:var(--c5)}.s6{fill:var(--c6)}.s7{fill:var(--c7)}
.d0{fill:var(--fg);stroke:var(--c0);stroke-width:1.2}.d1{fill:var(--fg);stroke:var(--c1);stroke-width:1.2}.d2{fill:var(--fg);stroke:var(--c2);stroke-width:1.2}.d3{fill:var(--fg);stroke:var(--c3);stroke-width:1.2}.d4{fill:var(--fg);stroke:var(--c4);stroke-width:1.2}.d5{fill:var(--fg);stroke:var(--c5);stroke-width:1.2}.d6{fill:var(--fg);stroke:var(--c6);stroke-width:1.2}.d7{fill:var(--fg);stroke:var(--c7);stroke-width:1.2}
</style>`

// textEm is the average glyph width in em that wrap and the tests use to estimate how wide
// a line of text is: width = characters x font size x textEm.
const textEm = 0.55

func textWidth(s string, size float64) float64 { return float64(len([]rune(s))) * size * textEm }

// failureLines lists, for one producer count, one line per engine with failed trials,
// each wrapped to fit inside one group's width. An engine whose trials all failed has no
// bar and no series, so the names come from the failures as well as from the data. Failed
// trials are shown, never dropped.
func failureLines(data []series, w int, groupW float64) []string {
	seen := map[string]bool{}
	var names []string
	for _, s := range data {
		seen[s.engine] = true
		names = append(names, s.engine)
	}
	for e := range failedTrials {
		if !seen[e] {
			names = append(names, e)
		}
	}
	sort.SliceStable(names, func(i, j int) bool { return engineRank(names[i]) < engineRank(names[j]) })
	maxChars := int((groupW - 12) / (10 * textEm)) // font size 10, with a margin at each side
	var lines []string
	for _, e := range names {
		if n := failedTrials[e][w]; n > 0 {
			lines = append(lines, wrap([]string{fmt.Sprintf("%s: %d failed", label(e), n)}, maxChars)...)
		}
	}
	return lines
}

// failureRows is the most failure lines any one producer count needs, which decides how far
// the legend and footer move down.
func failureRows(data []series, ws []int, groupW float64) int {
	most := 0
	for _, w := range ws {
		if k := len(failureLines(data, w, groupW)); k > most {
			most = k
		}
	}
	return most
}

// legendColumns picks the most columns (three, two, then one) in which every legend entry,
// a swatch and its label, fits its column when measured with the same estimate as the
// failure notes. Long labels fall back to fewer columns instead of overlapping.
func legendColumns(texts []string, avail float64) int {
	for _, c := range []int{3, 2, 1} {
		colW := avail / float64(c)
		fits := true
		for _, t := range texts {
			if 16+textWidth(t, 11) > colW-8 {
				fits = false
				break
			}
		}
		if fits {
			return c
		}
	}
	return 1
}

// declaredEngines is the charted set in color-slot order: the declared engines (-engines),
// plus any engine that has data but was not declared, ordered by engineOrder. An engine keeps
// its slot, and therefore its color and its place in the legend, even when every trial of it
// failed and it has no bar.
func declaredEngines(data []series) []string {
	seen := map[string]bool{}
	var out []string
	for _, e := range expectedEngines {
		if !seen[e] {
			seen[e] = true
			out = append(out, e)
		}
	}
	for _, s := range data {
		if !seen[s.engine] {
			seen[s.engine] = true
			out = append(out, s.engine)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return engineRank(out[i]) < engineRank(out[j]) })
	return out
}

type legendEntry struct {
	engine string
	lines  []string // the label, wrapped to its column
}

// legendLayout chooses the columns and wraps each entry to its column's width, so an entry
// that is too wide even for one column continues on a second line instead of clipping. It also
// returns each row's height.
func legendLayout(engines []string, avail float64) (cols int, entries []legendEntry, rowHeights []int) {
	var texts []string
	for _, e := range engines {
		texts = append(texts, seriesLabel(e))
	}
	cols = legendColumns(texts, avail)
	maxChars := int((avail/float64(cols) - 16 - 8) / (11 * textEm))
	if maxChars < 8 {
		maxChars = 8
	}
	for i, e := range engines {
		entries = append(entries, legendEntry{e, wrap([]string{texts[i]}, maxChars)})
	}
	for r := 0; r*cols < len(entries); r++ {
		most := 1
		for c := 0; c < cols && r*cols+c < len(entries); c++ {
			if k := len(entries[r*cols+c].lines); k > most {
				most = k
			}
		}
		h := 18
		if most > 1 {
			h = 12*most + 6
		}
		rowHeights = append(rowHeights, h)
	}
	return cols, entries, rowHeights
}

// seriesLabel is the legend text: the engine's label and, when the meta file says it,
// where it ran (in process, or a server and how it was reached).
func seriesLabel(engine string) string {
	if a, ok := engineArch[engine]; ok && a != "" {
		return label(engine) + " (" + a + ")"
	}
	return label(engine)
}

// renderMulti draws the same grouped bars for three or more engines: narrower bars,
// values written vertically above them, a legend with each engine's label and where it
// ran, and a line under each producer count for engines with failed trials.
func renderMulti(c chart, data []series, footer []string) string {
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
	for _, byWorkers := range failedTrials { // a count where every trial failed still has its place
		for w := range byWorkers {
			producers[w] = true
		}
	}
	var ws []int
	for w := range producers {
		ws = append(ws, w)
	}
	sort.Ints(ws)
	step, steps := ticks(maxV)
	top1 := step * float64(steps)
	plotW := float64(width - left - right)
	// The vertical value labels are written upward from above the tallest dot, so the plot area
	// keeps headroom for the longest of them below the subtitle (whose last line is at y=76).
	plotTop := float64(top)
	if need := 92 + textWidth(format(maxV, c.decimals), 10); need > plotTop {
		plotTop = need
	}
	plotH := float64(base) - plotTop
	y := func(v float64) float64 { return float64(base) - v/top1*plotH }

	declared := declaredEngines(data)
	slot := map[string]int{}
	for i, e := range declared {
		slot[e] = i
	}
	n := len(declared)
	cols, entries, rowHeights := legendLayout(declared, float64(width-left-right))
	legendHeight := 0
	for _, rh := range rowHeights {
		legendHeight += rh
	}
	verdict := winners(data, c.lower)
	// The description names the engines that have data, and says which declared engines have
	// none, so it never claims a fixed pair.
	var names, empty []string
	present := map[string]bool{}
	for _, s := range data {
		names = append(names, label(s.engine))
		present[s.engine] = true
	}
	for _, e := range declared {
		if !present[e] {
			empty = append(empty, label(e))
		}
	}
	alt := strings.Replace(c.alt, "for Dostoevsky and embedded JetStream", "for "+strings.Join(names, ", "), 1)
	if len(empty) > 0 {
		alt += " No valid trial: " + strings.Join(empty, ", ") + "."
	}
	footer = wrap(footer, 112)
	groupW := plotW / float64(len(ws))
	failRows := failureRows(data, ws, groupW)
	legendY := base + 50
	if failRows > 1 {
		legendY += 12 * (failRows - 1)
	}
	footerY := legendY + legendHeight + 14
	h := footerY + 15*len(footer) + 10
	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d" role="img" aria-labelledby="t d">`+"\n", width, h, width, h)
	title := c.title
	if c.multiTitle != "" {
		title = c.multiTitle
	}
	fmt.Fprintf(&b, "<title id=\"t\">%s</title><desc id=\"d\">%s</desc>\n%s\n", html.EscapeString(title), html.EscapeString(alt+" "+verdict+notShownSentence()), styleMulti)
	fmt.Fprintf(&b, `<text x="%d" y="26" font-size="18" font-weight="600">%s</text>`+"\n", left, html.EscapeString(title))
	subtitle := []string{c.unit + ". Bars are medians; dots are trials.", strings.TrimSpace(c.note + " " + verdict)}
	if c.explain != "" && losing(data, c.lower) {
		subtitle[1] += " " + c.explain
	}
	for i, line := range wrap(subtitle, 100) {
		fmt.Fprintf(&b, `<text x="%d" y="%d" font-size="12" class="m">%s</text>`+"\n", left, 46+i*15, html.EscapeString(line))
	}
	for i := 0; i <= steps; i++ {
		v := step * float64(i)
		fmt.Fprintf(&b, `<line class="g" x1="%d" x2="%d" y1="%.1f" y2="%.1f"/><text class="m" x="%d" y="%.1f" font-size="11" text-anchor="end">%s</text>`+"\n",
			left, width-right, y(v), y(v), left-8, y(v)+4, format(v, stepDecimals(step)))
	}
	barW := groupW*0.84/float64(n) - 3
	for gi, w := range ws {
		gx := float64(left) + groupW*float64(gi)
		fmt.Fprintf(&b, `<text x="%.1f" y="%d" font-size="13" text-anchor="middle">%d producer%s</text>`+"\n", gx+groupW/2, base+22, w, plural(w))
		for i, line := range failureLines(data, w, groupW) {
			fmt.Fprintf(&b, `<text x="%.1f" y="%d" font-size="10" class="f">%s</text>`+"\n", gx+6, base+36+i*12, html.EscapeString(line))
		}
		for _, s := range data {
			si := slot[s.engine]
			vs := s.values[w]
			if len(vs) == 0 {
				continue
			}
			m := median(vs)
			x := gx + groupW*0.08 + float64(si)*(barW+3)
			tip := fmt.Sprintf("%s, %d producer%s: median %s of %d trials", label(s.engine), w, plural(w), format(m, c.decimals), len(vs))
			if f := failedTrials[s.engine][w]; f > 0 {
				tip += fmt.Sprintf(", %d failed", f)
			}
			fmt.Fprintf(&b, `<rect class="s%d" x="%.1f" y="%.1f" width="%.1f" height="%.1f"><title>%s</title></rect>`+"\n",
				si, x, y(m), barW, float64(base)-y(m), html.EscapeString(tip))
			lx, ly := x+barW/2+4, y(m)-8-dotRoom(vs, y, m)
			fmt.Fprintf(&b, `<text x="%.1f" y="%.1f" transform="rotate(-90 %.1f %.1f)" font-size="10" font-weight="600">%s</text>`+"\n", lx, ly, lx, ly, format(m, c.decimals))
			sorted := append([]float64(nil), vs...)
			sort.Float64s(sorted)
			spacing := barW / float64(len(sorted)+1)
			if spacing > 5 {
				spacing = 5
			}
			for ti, v := range sorted {
				dx := x + barW/2 + (float64(ti)-float64(len(sorted)-1)/2)*spacing
				fmt.Fprintf(&b, `<circle class="d%d" cx="%.1f" cy="%.1f" r="2"/>`+"\n", si, dx, y(v))
			}
		}
	}
	rowTop := make([]int, len(rowHeights))
	for r := 1; r < len(rowHeights); r++ {
		rowTop[r] = rowTop[r-1] + rowHeights[r-1]
	}
	for i, en := range entries {
		lx := left + (i%cols)*(width-left-right)/cols
		ly := legendY + rowTop[i/cols]
		fmt.Fprintf(&b, `<rect class="s%d" x="%d" y="%d" width="11" height="11"/>`+"\n", slot[en.engine], lx, ly)
		for k, line := range en.lines {
			fmt.Fprintf(&b, `<text x="%d" y="%d" font-size="11">%s</text>`+"\n", lx+16, ly+10+12*k, html.EscapeString(line))
		}
	}
	for i, line := range footer {
		fmt.Fprintf(&b, `<text class="m" x="%d" y="%d" font-size="10.5">%s</text>`+"\n", left, footerY+i*15, html.EscapeString(line))
	}
	b.WriteString("</svg>\n")
	return withBackground(b.String())
}

// countFailedLines counts the receipt's FAILED trial lines, whatever their engine or
// producer count: a failure outside the expected schedule is still a failure.
func countFailedLines(trials []trial) int {
	n := 0
	for _, t := range trials {
		if t["failed"] == "1" {
			n++
		}
	}
	return n
}

// requireComparisonMeta checks the labels a multi-engine chart must carry: a label and
// an architecture for every engine, and the qualifiers that must never disappear from
// a chart of those engines.
func requireComparisonMeta(engines []string, meta map[string]string) error {
	var missing []string
	for _, e := range engines {
		if strings.TrimSpace(meta["label."+e]) == "" {
			missing = append(missing, "label."+e)
		}
		if strings.TrimSpace(meta["arch."+e]) == "" {
			missing = append(missing, "arch."+e)
		}
	}
	notes := ""
	for i := 1; ; i++ {
		l, ok := meta[fmt.Sprintf("note.%d", i)]
		if !ok {
			break
		}
		notes += l + "\n"
	}
	need := func(engine, what, text string) {
		for _, e := range engines {
			if e == engine && !strings.Contains(notes, text) {
				missing = append(missing, fmt.Sprintf("a note.N containing %q (%s)", text, what))
			}
		}
	}
	need("rabbitmq", "RabbitMQ's durability qualifier", "durability per vendor default, not verified")
	need("rabbitmq", "the Erlang provenance", "Erlang")
	need("kafka", "Kafka's flush qualifier", "fsync per message inferred")
	need("redpanda", "Redpanda's durability qualifier", "fsync per acks=all batch, documented, not independently observed")
	need("redpanda", "Redpanda's package provenance", "vendor-signed, key not independently rooted")
	if len(missing) > 0 {
		return fmt.Errorf("the meta file lacks what every multi-engine chart must carry: %s", strings.Join(missing, "; "))
	}
	return nil
}

// checkEngineSet compares the engines that actually appear in the receipt (in any line,
// valid or failed) with the expected list. An engine that appears but is neither expected
// nor deliberately ignored is refused, so a chart cannot silently leave one out; an expected
// engine with no line at all is refused, so a chart cannot silently lack one.
func checkEngineSet(trials []trial, expected, ignored []string) error {
	present := map[string]bool{}
	for _, t := range trials {
		if e := t["engine"]; e != "" {
			present[e] = true
		}
	}
	known := map[string]bool{}
	for _, e := range expected {
		known[e] = true
	}
	for _, e := range ignored {
		known[e] = true
	}
	var extra, absent []string
	for e := range present {
		if !known[e] {
			extra = append(extra, e)
		}
	}
	for _, e := range expected {
		if !present[e] {
			absent = append(absent, e)
		}
	}
	sort.Strings(extra)
	sort.Strings(absent)
	var problems []string
	if len(extra) > 0 {
		problems = append(problems, fmt.Sprintf("engines %s appear in the receipt but not in -engines (add them, or leave them out on purpose with -ignore-engines)", strings.Join(extra, ", ")))
	}
	if len(absent) > 0 {
		problems = append(problems, fmt.Sprintf("engines %s are expected but have no line in the receipt", strings.Join(absent, ", ")))
	}
	if len(problems) > 0 {
		return errors.New(strings.Join(problems, "; "))
	}
	return nil
}

// comparisonMode is true when the declared engines include anything but dostoevsky and
// jetstream: the legacy renderer and the legacy meta apply only to a subset of that pair.
func comparisonMode(declared []string) bool {
	for _, e := range declared {
		if e != "dostoevsky" && e != "jetstream" {
			return true
		}
	}
	return false
}

// leftOut returns the engines that appear in the receipt and are ignored rather than
// charted, in engineOrder, so an omission is named on the chart and never silent.
func leftOut(trials []trial, expected, ignored []string) []string {
	present := map[string]bool{}
	for _, t := range trials {
		if e := t["engine"]; e != "" {
			present[e] = true
		}
	}
	var out []string
	for _, e := range ignored {
		if present[e] && !inEngines(e, expected) {
			out = append(out, e)
		}
	}
	sort.SliceStable(out, func(i, j int) bool { return engineRank(out[i]) < engineRank(out[j]) })
	return out
}

func notShownLine() string {
	return "Not shown: " + strings.Join(notShown, ", ") + " (excluded with -ignore-engines)"
}

func notShownSentence() string {
	if len(notShown) == 0 {
		return ""
	}
	return " " + notShownLine() + "."
}
