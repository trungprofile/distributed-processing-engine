// Command placementsim compares kube-scheduler's two NodeResourcesFit scoring
// strategies — MostAllocated (bin-pack) and LeastAllocated (spread) — over a
// fixed node inventory and pod workload described by a YAML fixture.
//
// It needs no cluster and no Kubernetes API: it replays the scheduler's own
// filter-and-score arithmetic in process, prints a markdown comparison and
// writes machine-readable results. Every figure it produces is simulated, and
// the JSON it writes says so in its schema.
package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"

	"github.com/trungprofile/distributed-processing-engine/internal/placement"
)

func main() {
	var (
		fixturePath = flag.String("fixture", "bench/fixtures/gpu-inference.yaml", "YAML node inventory and pod workload")
		outPath     = flag.String("out", "bench/results/placement.json", "where to write the JSON results; empty writes none")
	)
	flag.Parse()

	if err := run(*fixturePath, *outPath); err != nil {
		fmt.Fprintf(os.Stderr, "placementsim: %v\n", err)
		os.Exit(1)
	}
}

func run(fixturePath, outPath string) error {
	data, err := os.ReadFile(fixturePath)
	if err != nil {
		return fmt.Errorf("read fixture: %w", err)
	}

	fixture, nodes, pods, err := placement.ParseFixture(data)
	if err != nil {
		return err
	}

	report := placement.BuildReport(fixture, nodes, pods)
	fmt.Print(report.Markdown())

	if outPath == "" {
		return nil
	}
	blob, err := report.JSON()
	if err != nil {
		return err
	}
	if err := os.MkdirAll(filepath.Dir(outPath), 0o755); err != nil {
		return fmt.Errorf("create results directory: %w", err)
	}
	if err := os.WriteFile(outPath, blob, 0o644); err != nil {
		return fmt.Errorf("write results: %w", err)
	}
	fmt.Printf("\nwrote %s\n", outPath)
	return nil
}
