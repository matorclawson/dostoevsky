package main

import (
	"fmt"
	"html"
	"strings"
)

type box struct {
	x, y, w, h int
	title      string
	lines      []string
	class      int
}

// renderArchitecture draws the write and read path of one node. The text and
// layout live here, in code, so the diagram uses the same palette as the charts.
func renderArchitecture() string {
	const w, h = 760, 400
	boxes := []box{
		{24, 70, 150, 110, "Producers", []string{"Publish, PublishBatch", "optional message ID", "key picks the partition"}, 0},
		{204, 70, 170, 110, "Group commit", []string{"idle: write inline", "busy: share one write", "up to 64 calls, 1 MiB"}, 0},
		{404, 70, 160, 110, "Journal", []string{"checksummed frames", "one fdatasync per group", "ack only after sync"}, 1},
		{594, 70, 142, 110, "Partition index", []string{"contiguous offsets", "in RAM: locations", "payloads stay on disk"}, 0},
		{204, 250, 200, 110, "Consumer groups", []string{"Fold and Pipe", "state and offset are one", "journal frame: no split"}, 1},
		{454, 250, 190, 110, "Notifier", []string{"subscribe on any key", "woken after a commit"}, 0},
	}
	var b strings.Builder
	fmt.Fprintf(&b, `<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 %d %d" width="%d" height="%d" role="img" aria-labelledby="t d">`+"\n", w, h, w, h)
	b.WriteString(`<title id="t">Dostoevsky architecture</title><desc id="d">Producers publish to a group commit queue. The queue writes checksummed frames to the journal with one fdatasync per group, and only then acknowledges. The partition index records offsets in memory. Consumer groups read from the index and commit state and offset together as one journal frame. A notifier wakes subscribers after a commit.</desc>` + "\n")
	b.WriteString(style + "\n")
	b.WriteString(`<defs><marker id="arrow" viewBox="0 0 10 10" refX="9" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10z" fill="var(--muted)"/></marker></defs>` + "\n")
	b.WriteString(`<style>.arrow{stroke:var(--muted);stroke-width:1.6;fill:none;marker-end:url(#arrow)}.box{fill:none;stroke-width:2}.b0{stroke:var(--a)}.b1{stroke:var(--b)}</style>` + "\n")
	b.WriteString(`<text x="24" y="30" font-size="18" font-weight="600">One node: the write path and the read path</text>` + "\n")
	b.WriteString(`<text x="24" y="50" font-size="12" class="m">An application embeds this in its own process. A mesh of nodes routes each partition to its owner over HTTP.</text>` + "\n")
	for _, bx := range boxes {
		fmt.Fprintf(&b, `<rect class="box b%d" x="%d" y="%d" width="%d" height="%d" rx="8"/>`+"\n", bx.class, bx.x, bx.y, bx.w, bx.h)
		fmt.Fprintf(&b, `<text x="%d" y="%d" font-size="14" font-weight="600">%s</text>`+"\n", bx.x+12, bx.y+26, html.EscapeString(bx.title))
		for i, l := range bx.lines {
			fmt.Fprintf(&b, `<text class="m" x="%d" y="%d" font-size="11.5">%s</text>`+"\n", bx.x+12, bx.y+50+i*18, html.EscapeString(l))
		}
	}
	b.WriteString(`<path class="arrow" d="M174 125H204"/><path class="arrow" d="M374 125H404"/><path class="arrow" d="M564 125H594"/>` + "\n")
	b.WriteString(`<path class="arrow" d="M665 180V215H304V250"/>` + "\n")
	b.WriteString(`<text class="m" x="320" y="208" font-size="11">read by offset</text>` + "\n")
	b.WriteString(`<path class="arrow" d="M484 180V250"/>` + "\n")
	b.WriteString(`<text class="m" x="492" y="215" font-size="11">after sync</text>` + "\n")
	b.WriteString(`<path class="arrow" d="M404 330H454"/>` + "\n")
	b.WriteString(`<text class="m" x="410" y="322" font-size="11">commit</text>` + "\n")
	b.WriteString(`<text class="m" x="24" y="385" font-size="10.5">Orange borders mark the durability boundary: the journal write and the co-committed checkpoint.</text>` + "\n")
	b.WriteString("</svg>\n")
	return withBackground(b.String())
}
