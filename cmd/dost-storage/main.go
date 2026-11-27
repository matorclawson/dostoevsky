// Command dost-storage migrates, verifies and exports existing node directories offline.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	d "github.com/rodriveracom/dostoevsky"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
func run() (err error) {
	mode := flag.String("mode", "verify", "verify, migrate or backup")
	data := flag.String("data", "", "existing node directory")
	config := flag.String("config", "", "JSON file with the stored Node, Nodes and Topics")
	dest := flag.String("dest", "", "new destination for migrate or backup")
	flag.Parse()
	if *data == "" || *config == "" || flag.NArg() != 0 || (*mode != "verify" && *mode != "migrate" && *mode != "backup") || (*mode == "verify") != (*dest == "") {
		return errors.New("supply -mode, -data, -config and a new -dest for migrate or backup")
	}
	fi, err := os.Stat(filepath.Join(*data, "journal.v1"))
	if err != nil {
		return err
	}
	if !fi.Mode().IsRegular() || fi.Size() == 0 {
		return errors.New("source journal is empty or not a regular file")
	}
	raw, err := os.ReadFile(*config)
	if err != nil {
		return err
	}
	var c d.Config
	if err = json.Unmarshal(raw, &c); err != nil {
		return err
	}
	n, err := d.Open(*data, c)
	if err != nil {
		return err
	}
	defer func() { err = errors.Join(err, n.Close()) }()
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	switch *mode {
	case "verify":
		return n.VerifySegments(ctx)
	case "migrate":
		return n.MigrateSegmented(ctx, *dest)
	default:
		return n.Backup(ctx, *dest)
	}
}
