// Command mkresults renders docs/results.md and the README's summary table from the
// JSON files under results/ (spec section 10, call D26). No number is typed by hand.
package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

func run(args []string, stdout, stderr io.Writer) int {
	fl := flag.NewFlagSet("mkresults", flag.ContinueOnError)
	fl.SetOutput(stderr)
	dir := fl.String("results", "results", "directory holding the benchmark JSON files")
	out := fl.String("out", "docs/results.md", "write the full tables here")
	readme := fl.String("readme", "README.md", "replace the block between the results markers in this file")
	if err := fl.Parse(args); err != nil {
		return 2
	}
	in, err := Load(*dir)
	if err != nil {
		fmt.Fprintln(stderr, "mkresults:", err)
		return 1
	}
	if err := os.MkdirAll(filepath.Dir(*out), 0o755); err != nil {
		fmt.Fprintln(stderr, "mkresults:", err)
		return 1
	}
	if err := os.WriteFile(*out, []byte(Render(in)), 0o644); err != nil {
		fmt.Fprintln(stderr, "mkresults:", err)
		return 1
	}
	fmt.Fprintln(stdout, "wrote", *out)
	if *readme == "" {
		return 0
	}
	b, err := os.ReadFile(*readme)
	switch {
	case errors.Is(err, fs.ErrNotExist):
		fmt.Fprintf(stdout, "%s does not exist; left alone\n", *readme)
		return 0
	case err != nil:
		fmt.Fprintln(stderr, "mkresults:", err)
		return 1
	}
	updated, ok := ReplaceMarkers(string(b), Summary(in))
	if !ok {
		fmt.Fprintf(stdout, "%s has no results markers; left alone\n", *readme)
		return 0
	}
	if updated != string(b) {
		if err := os.WriteFile(*readme, []byte(updated), 0o644); err != nil {
			fmt.Fprintln(stderr, "mkresults:", err)
			return 1
		}
	}
	fmt.Fprintln(stdout, "updated", *readme)
	return 0
}
