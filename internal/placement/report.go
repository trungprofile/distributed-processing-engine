package placement

import (
	"encoding/json"
	"fmt"
	"strings"
)

// ClusterSummary describes the inventory a run was measured against, so a
// committed result file is self-contained.
type ClusterSummary struct {
	Nodes            int   `json:"nodes"`
	TotalGPUs        int64 `json:"totalGpus"`
	TotalCPUMillis   int64 `json:"totalCpuMillis"`
	TotalMemoryBytes int64 `json:"totalMemoryBytes"`
}

// WorkloadSummary describes the pods that were scheduled.
type WorkloadSummary struct {
	Pods             int   `json:"pods"`
	GPUsRequested    int64 `json:"gpusRequested"`
	CPUMillisRequest int64 `json:"cpuMillisRequested"`
	MemBytesRequest  int64 `json:"memoryBytesRequested"`
}

// Report is the committed output of one simulator run.
type Report struct {
	// Simulated is always true and is part of the schema on purpose: any
	// figure taken from this file must be labelled simulated wherever it is
	// quoted, because no cluster was involved in producing it.
	Simulated bool `json:"simulated"`

	Fixture     string          `json:"fixture"`
	Description string          `json:"description,omitempty"`
	Weights     Weights         `json:"weights"`
	Cluster     ClusterSummary  `json:"cluster"`
	Workload    WorkloadSummary `json:"workload"`
	Results     []Result        `json:"results"`
}

// BuildReport runs every strategy over the same inventory and workload.
func BuildReport(f *Fixture, nodes []Node, pods []Pod) Report {
	w := f.WeightsOf()

	rep := Report{
		Simulated:   true,
		Fixture:     f.Name,
		Description: f.Description,
		Weights:     w,
		Cluster:     ClusterSummary{Nodes: len(nodes)},
		Workload:    WorkloadSummary{Pods: len(pods)},
	}
	for i := range nodes {
		rep.Cluster.TotalGPUs += nodes[i].Allocatable.GPUs
		rep.Cluster.TotalCPUMillis += nodes[i].Allocatable.CPUMillis
		rep.Cluster.TotalMemoryBytes += nodes[i].Allocatable.MemBytes
	}
	for i := range pods {
		rep.Workload.GPUsRequested += pods[i].Request.GPUs
		rep.Workload.CPUMillisRequest += pods[i].Request.CPUMillis
		rep.Workload.MemBytesRequest += pods[i].Request.MemBytes
	}

	// Fixed strategy order so the JSON and the table never reorder between
	// runs, which keeps the committed result file diffable.
	for _, s := range []Strategy{MostAllocated, LeastAllocated} {
		rep.Results = append(rep.Results, Simulate(nodes, pods, s, w))
	}
	return rep
}

// JSON renders the report for bench/results/placement.json. Indented and with
// a trailing newline so the committed file is reviewable in a diff.
func (r Report) JSON() ([]byte, error) {
	blob, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		return nil, fmt.Errorf("encode report: %w", err)
	}
	return append(blob, '\n'), nil
}

// Markdown renders the comparison table printed to stdout and pasted into
// bench/README.md.
func (r Report) Markdown() string {
	var b strings.Builder

	fmt.Fprintf(&b, "# Placement simulation (simulated, no cluster)\n\n")
	fmt.Fprintf(&b, "Fixture: `%s`\n", r.Fixture)
	if r.Description != "" {
		fmt.Fprintf(&b, "\n%s\n", r.Description)
	}
	fmt.Fprintf(&b, "\nCluster: %d nodes, %d GPUs, %s cpu, %s memory\n",
		r.Cluster.Nodes, r.Cluster.TotalGPUs,
		cores(r.Cluster.TotalCPUMillis), gib(r.Cluster.TotalMemoryBytes))
	fmt.Fprintf(&b, "Workload: %d pods requesting %d GPUs, %s cpu, %s memory\n",
		r.Workload.Pods, r.Workload.GPUsRequested,
		cores(r.Workload.CPUMillisRequest), gib(r.Workload.MemBytesRequest))
	fmt.Fprintf(&b, "Score weights: gpu %d, cpu %d, memory %d\n\n",
		r.Weights.GPU, r.Weights.CPU, r.Weights.Memory)

	fmt.Fprintf(&b, "| Strategy | Intent | Nodes used | GPU utilization | CPU utilization | Unschedulable | Nodes with all GPUs free | Largest free GPU block |\n")
	fmt.Fprintf(&b, "| --- | --- | --- | --- | --- | --- | --- | --- |\n")
	for _, res := range r.Results {
		fmt.Fprintf(&b, "| %s | %s | %d / %d | %s | %s | %d | %d | %d |\n",
			res.Strategy, res.Intent,
			res.NodesUsed, res.NodesTotal,
			pct(res.GPUUtilization), pct(res.CPUUtilization),
			res.PodsUnschedulable,
			res.NodesFullyFreeGPU, res.LargestFreeGPUBlock)
	}
	return b.String()
}

func pct(v float64) string { return fmt.Sprintf("%.1f%%", v*100) }
func cores(m int64) string { return fmt.Sprintf("%.1f", float64(m)/1000) }
func gib(by int64) string  { return fmt.Sprintf("%.0fGi", float64(by)/(1<<30)) }
