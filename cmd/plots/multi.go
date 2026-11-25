package main

import (
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

// failureNote lists, for one producer count, every engine with failed trials. An engine
// whose trials all failed has no bar and no series, so the names come from the failures
// as well as from the data. Failed trials are shown, never dropped.
func failureNote(data []series, w int) string {
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
	var parts []string
	for _, e := range names {
		if n := failedTrials[e][w]; n > 0 {
			parts = append(parts, fmt.Sprintf("%s: %d failed", label(e), n))
		}
	}
	return strings.Join(parts, "; ")
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
	var ws []int
	for w := range producers {
		ws = append(ws, w)
	}
	sort.Ints(ws)
	step, steps := ticks(maxV)
	top1 := step * float64(steps)
	plotW, plotH := float64(width-left-right), float64(base-top)
	y := func(v float64) float64 { return float64(base) - v/top1*plotH }

	n := len(data)
	rows := (n + 2) / 3
	verdict := winners(data, c.lower)
	var names []string
	for _, s := range data {
		names = append(names, label(s.engine))
	}
	alt := strings.Replace(c.alt, "for Dostoevsky and embedded JetStream", "for "+strings.Join(names, ", "), 1)
	footer = wrap(footer, 112)
	legendY := base + 50
	footerY := legendY + 18*rows + 14
	h := footerY + 15*len(footer) + 10
	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d" role="img" aria-labelledby="t d">`+"\n", width, h, width, h)
	fmt.Fprintf(&b, "<title id=\"t\">%s</title><desc id=\"d\">%s</desc>\n%s\n", html.EscapeString(c.title), html.EscapeString(alt+" "+verdict), styleMulti)
	fmt.Fprintf(&b, `<text x="%d" y="26" font-size="18" font-weight="600">%s</text>`+"\n", left, html.EscapeString(c.title))
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
	groupW := plotW / float64(len(ws))
	barW := groupW*0.84/float64(n) - 3
	for gi, w := range ws {
		gx := float64(left) + groupW*float64(gi)
		fmt.Fprintf(&b, `<text x="%.1f" y="%d" font-size="13" text-anchor="middle">%d producer%s</text>`+"\n", gx+groupW/2, base+22, w, plural(w))
		if note := failureNote(data, w); note != "" {
			fmt.Fprintf(&b, `<text x="%.1f" y="%d" font-size="10" text-anchor="middle" class="f">%s</text>`+"\n", gx+groupW/2, base+36, html.EscapeString(note))
		}
		for si, s := range data {
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
	for si, s := range data {
		lx := left + (si%3)*230
		ly := legendY + (si/3)*18
		fmt.Fprintf(&b, `<rect class="s%d" x="%d" y="%d" width="11" height="11"/><text x="%d" y="%d" font-size="11">%s</text>`+"\n", si, lx, ly, lx+16, ly+10, html.EscapeString(seriesLabel(s.engine)))
	}
	for i, line := range footer {
		fmt.Fprintf(&b, `<text class="m" x="%d" y="%d" font-size="10.5">%s</text>`+"\n", left, footerY+i*15, html.EscapeString(line))
	}
	b.WriteString("</svg>\n")
	return withBackground(b.String())
}
