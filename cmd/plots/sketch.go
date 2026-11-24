package main

import (
	"fmt"
	"html"
	"sort"
	"strings"
)

// renderSketch draws one horizontal bar per sketch configuration: the relative
// error against the exact value, with the sketch's memory cost in the label.
// Receipt lines come from cmd/sketchreport (see docs/sketch-accuracy-results.txt).
func renderSketch(trials []trial, footer []string) string {
	type row struct {
		name  string
		err   float64
		bytes float64
	}
	var rows []row
	for _, t := range trials {
		e, ok1 := t.num("rel_err_pct")
		by, ok2 := t.num("bytes")
		if !ok1 || !ok2 {
			continue
		}
		switch t["kind"] {
		case "hll":
			rows = append(rows, row{fmt.Sprintf("HyperLogLog p=%s, %s distinct keys", t["precision"], commas(t["n"])), e, by})
		case "latency":
			rows = append(rows, row{fmt.Sprintf("Latency histogram p%s upper bound, %s", t["percentile"], t["shape"]), e, by})
		}
	}
	sort.SliceStable(rows, func(i, j int) bool { return rows[i].name < rows[j].name })
	maxV := 0.0
	for _, r := range rows {
		if r.err > maxV {
			maxV = r.err
		}
	}
	top := niceMax(maxV)
	const rowH, labelW = 30, 330
	h := 96 + rowH*len(rows) + 20 + 15*len(footer)
	plotW := float64(width - labelW - right)
	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d" role="img" aria-labelledby="t d">`+"\n", width, h, width, h)
	b.WriteString(`<title id="t">Sketch accuracy</title><desc id="d">Horizontal bars of relative error against the exact value for HyperLogLog estimates and latency histogram percentile upper bounds, each labeled with the sketch memory in bytes.</desc>` + "\n" + style + "\n")
	b.WriteString(`<text x="24" y="26" font-size="18" font-weight="600">Sketch accuracy against exact values</text>` + "\n")
	b.WriteString(`<text x="24" y="46" font-size="12" class="m">Relative error in percent. Lower is better. Memory cost in bytes follows each label.</text>` + "\n")
	for i := 0; i <= 4; i++ {
		v := top * float64(i) / 4
		x := float64(labelW) + plotW*float64(i)/4
		fmt.Fprintf(&b, `<line class="g" x1="%.1f" x2="%.1f" y1="64" y2="%d"/><text class="m" x="%.1f" y="%d" font-size="11" text-anchor="middle">%s%%</text>`+"\n",
			x, x, 64+rowH*len(rows), x, 64+rowH*len(rows)+16, format(v, tickDecimals(top)))
	}
	for i, r := range rows {
		y := 64 + i*rowH
		w := r.err / top * plotW
		fmt.Fprintf(&b, `<text x="%d" y="%d" font-size="12" text-anchor="end">%s (%s B)</text>`+"\n", labelW-8, y+18, html.EscapeString(r.name), commas(fmt.Sprintf("%.0f", r.bytes)))
		fmt.Fprintf(&b, `<rect class="s0" x="%d" y="%d" width="%.1f" height="18"><title>%s: %.3f%%</title></rect>`+"\n", labelW, y+5, w, html.EscapeString(r.name), r.err)
		fmt.Fprintf(&b, `<text x="%.1f" y="%d" font-size="11" font-weight="600">%.3f%%</text>`+"\n", float64(labelW)+w+5, y+18, r.err)
	}
	for i, line := range footer {
		fmt.Fprintf(&b, `<text class="m" x="24" y="%d" font-size="10.5">%s</text>`+"\n", 64+rowH*len(rows)+40+i*15, html.EscapeString(line))
	}
	b.WriteString("</svg>\n")
	return b.String()
}
