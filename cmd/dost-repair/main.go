// Command dost-repair inspects a Dostoevsky data directory and, only when asked, repairs a damaged journal.
//
//	dost-repair -data DIR                                inspect; changes nothing
//	dost-repair -data DIR -repair                        copy the bytes after the last good frame aside, then truncate
//	dost-repair -data DIR -repair -discard-later-frames  also when good frames follow the damage (they are lost)
//	dost-repair -data DIR -upgrade                       upgrade a journal written by 0.2.x (one way)
//
// Run it on the host that holds DIR, as the user that owns it, with the application stopped. It needs no root.
// Exit status: 0 the journal is sound (or was repaired), 1 damaged and not repaired, 2 it could not be examined.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	d "github.com/rodriveracom/dostoevsky"
)

func main() { os.Exit(run(os.Args[1:], os.Stdout, os.Stderr)) }

func run(args []string, out, errOut io.Writer) int {
	flags := flag.NewFlagSet("dost-repair", flag.ContinueOnError)
	flags.SetOutput(errOut)
	dir := flags.String("data", "", "the data directory (required)")
	repair := flags.Bool("repair", false, "truncate after the last good frame, keeping a copy of what is removed")
	discard := flags.Bool("discard-later-frames", false, "with -repair: truncate although good frames follow the damage")
	upgrade := flags.Bool("upgrade", false, "upgrade a journal written by 0.2.x, keeping a copy; one way. Back up the directory first")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if *dir == "" || flags.NArg() != 0 || (*discard && !*repair) || (*upgrade && *repair) {
		fmt.Fprintln(errOut, "usage: dost-repair -data DIR [-repair [-discard-later-frames] | -upgrade]")
		return 2
	}
	if *upgrade {
		kept, err := d.UpgradeJournal(*dir)
		switch {
		case err != nil:
			fmt.Fprintln(errOut, "dost-repair:", err)
			return 2
		case kept == "":
			fmt.Fprintln(out, "no upgrade needed")
		default:
			fmt.Fprintf(out, "upgraded: 0.2.x can no longer open this directory; the journal as it was is in %s\n", kept)
		}
		return 0
	}
	var report d.JournalReport
	var err error
	if *repair {
		report, err = d.RepairJournal(*dir, d.RepairOptions{DiscardLaterFrames: *discard})
	} else {
		report, err = d.InspectJournal(*dir)
	}
	if err != nil && !errors.Is(err, d.ErrLaterFrames) && !errors.Is(err, d.ErrNotRepairable) {
		fmt.Fprintln(errOut, "dost-repair:", err)
		return 2
	}
	damage := report.Damage
	if damage == d.DamageNone {
		damage = "none"
	}
	fmt.Fprintf(out, "damage=%s bytes=%d good_bytes=%d good_frames=%d later_good_frames=%d\n",
		damage, report.Size, report.GoodBytes, report.Frames, report.LaterFrames)
	switch {
	case report.Damage == d.DamageNone:
		return 0
	case report.Quarantine != "":
		fmt.Fprintf(out, "repaired: removed %d bytes, kept in %s\n", report.Size-report.GoodBytes, report.Quarantine)
		return 0
	case errors.Is(err, d.ErrLaterFrames):
		fmt.Fprintln(out, "not repaired: good frames follow the damage and truncating would discard them; restore a backup, or add -discard-later-frames to lose them")
	case errors.Is(err, d.ErrNotRepairable):
		fmt.Fprintln(out, "not repaired: the frame is intact but its content is not understood; truncating is not the answer")
	case report.Damage == d.DamageTornTail:
		fmt.Fprintln(out, "an incomplete final frame: opening the directory removes it by itself")
	default:
		fmt.Fprintln(out, "not changed: add -repair to remove the bytes after the last good frame (a copy is kept)")
	}
	return 1
}
