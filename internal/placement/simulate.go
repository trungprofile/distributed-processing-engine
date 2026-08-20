package placement

import "sort"

// maxNodeScore matches framework.MaxNodeScore in kube-scheduler: every scoring
// plugin normalises onto 0..100 before weights are applied.
const maxNodeScore int64 = 100

// Strategy is a NodeResourcesFit scoring strategy.
type Strategy string

const (
	// MostAllocated prefers the fullest node that still fits, i.e. bin-packing.
	MostAllocated Strategy = "MostAllocated"
	// LeastAllocated prefers the emptiest node, i.e. spreading. This is what
	// the default scheduler does out of the box.
	LeastAllocated Strategy = "LeastAllocated"
)

// NodeResult is one node's state at the end of a run.
type NodeResult struct {
	Name           string  `json:"name"`
	Pods           int     `json:"pods"`
	GPUsAllocated  int64   `json:"gpusAllocated"`
	GPUsCapacity   int64   `json:"gpusCapacity"`
	CPUAllocated   int64   `json:"cpuMillisAllocated"`
	CPUCapacity    int64   `json:"cpuMillisCapacity"`
	MemAllocated   int64   `json:"memoryBytesAllocated"`
	MemCapacity    int64   `json:"memoryBytesCapacity"`
	GPUUtilization float64 `json:"gpuUtilization"`
}

// Result is one strategy's outcome over the whole workload.
type Result struct {
	Strategy Strategy `json:"strategy"`
	// Intent names what the strategy is for in the operator's vocabulary.
	Intent string `json:"intent"`

	PodsScheduled     int `json:"podsScheduled"`
	PodsUnschedulable int `json:"podsUnschedulable"`
	// Unschedulable lists the pods that found no node, in placement order.
	Unschedulable []string `json:"unschedulablePods,omitempty"`

	NodesUsed  int `json:"nodesUsed"`
	NodesTotal int `json:"nodesTotal"`
	// NodesWithFreeGPUs counts nodes that ended the run with every GPU free.
	// This is the number that matters for a later large job: a cluster with
	// GPUs scattered one-per-node cannot place a pod that wants four.
	NodesFullyFreeGPU int `json:"nodesWithAllGpusFree"`
	// LargestFreeGPUBlock is the most GPUs still free on any single node.
	LargestFreeGPUBlock int64 `json:"largestFreeGpuBlock"`

	GPUUtilization    float64 `json:"gpuUtilization"`
	CPUUtilization    float64 `json:"cpuUtilization"`
	MemoryUtilization float64 `json:"memoryUtilization"`

	Nodes []NodeResult `json:"nodes"`
}

// Simulate schedules every pod in order under one strategy.
//
// The model is a sequential scheduling queue, which is what kube-scheduler
// runs: one pod at a time, filtered against current allocations, scored, and
// bound to the winner before the next pod is considered. Ties break on node
// name — the real scheduler picks randomly among equal top scorers, and
// substituting a deterministic rule is the one deliberate divergence, without
// which the same input would not produce the same output twice.
func Simulate(nodes []Node, pods []Pod, strategy Strategy, w Weights) Result {
	// Work on a copy so a caller can run both strategies over one inventory.
	state := make([]Node, len(nodes))
	copy(state, nodes)
	for i := range state {
		state[i].Allocated = Resources{}
		state[i].Pods = nil
	}

	res := Result{
		Strategy:   strategy,
		Intent:     intentOf(strategy),
		NodesTotal: len(state),
	}

	for _, pod := range pods {
		winner := -1
		var best int64 = -1
		for i := range state {
			if !state[i].fits(pod.Request) {
				continue
			}
			score := scoreNode(&state[i], pod.Request, strategy, w)
			// Strictly greater keeps the first node in name order on a tie.
			if score > best {
				best, winner = score, i
			}
		}
		if winner < 0 {
			res.PodsUnschedulable++
			res.Unschedulable = append(res.Unschedulable, pod.Name)
			continue
		}
		state[winner].place(pod.Name, pod.Request)
		res.PodsScheduled++
	}

	summarise(&res, state)
	return res
}

// intentOf gives each strategy the name the operator's placement policy uses.
func intentOf(s Strategy) string {
	if s == MostAllocated {
		return "bin-pack"
	}
	return "spread"
}

// scoreNode reproduces resourceAllocationScorer: score each weighted resource
// on 0..100, then take the weighted mean.
//
// A resource with zero allocatable capacity is skipped entirely, weight and
// all — exactly as upstream does. That detail matters here: without it, a
// GPU-less node would score zero on the heavily weighted GPU term and be
// pushed to the back of the queue for CPU-only pods.
func scoreNode(n *Node, req Resources, strategy Strategy, w Weights) int64 {
	var total, weightSum int64

	add := func(weight, allocatable, requested int64) {
		if allocatable == 0 || weight == 0 {
			return
		}
		total += weight * resourceScore(requested, allocatable, strategy)
		weightSum += weight
	}

	add(w.CPU, n.Allocatable.CPUMillis, n.Allocated.CPUMillis+req.CPUMillis)
	add(w.Memory, n.Allocatable.MemBytes, n.Allocated.MemBytes+req.MemBytes)
	add(w.GPU, n.Allocatable.GPUs, n.Allocated.GPUs+req.GPUs)

	if weightSum == 0 {
		return 0
	}
	return total / weightSum
}

// resourceScore is upstream's mostRequestedScore / leastRequestedScore.
func resourceScore(requested, capacity int64, strategy Strategy) int64 {
	if capacity == 0 || requested > capacity {
		return 0
	}
	if strategy == MostAllocated {
		return requested * maxNodeScore / capacity
	}
	return (capacity - requested) * maxNodeScore / capacity
}

// summarise folds per-node state into the reported totals.
func summarise(res *Result, state []Node) {
	var gpuAlloc, gpuCap, cpuAlloc, cpuCap, memAlloc, memCap int64

	res.Nodes = make([]NodeResult, 0, len(state))
	for i := range state {
		n := &state[i]
		gpuAlloc += n.Allocated.GPUs
		gpuCap += n.Allocatable.GPUs
		cpuAlloc += n.Allocated.CPUMillis
		cpuCap += n.Allocatable.CPUMillis
		memAlloc += n.Allocated.MemBytes
		memCap += n.Allocatable.MemBytes

		if n.used() {
			res.NodesUsed++
		}
		free := n.Allocatable.GPUs - n.Allocated.GPUs
		if n.Allocatable.GPUs > 0 && n.Allocated.GPUs == 0 {
			res.NodesFullyFreeGPU++
		}
		if free > res.LargestFreeGPUBlock {
			res.LargestFreeGPUBlock = free
		}

		res.Nodes = append(res.Nodes, NodeResult{
			Name:           n.Name,
			Pods:           len(n.Pods),
			GPUsAllocated:  n.Allocated.GPUs,
			GPUsCapacity:   n.Allocatable.GPUs,
			CPUAllocated:   n.Allocated.CPUMillis,
			CPUCapacity:    n.Allocatable.CPUMillis,
			MemAllocated:   n.Allocated.MemBytes,
			MemCapacity:    n.Allocatable.MemBytes,
			GPUUtilization: ratio(n.Allocated.GPUs, n.Allocatable.GPUs),
		})
	}

	sort.Slice(res.Nodes, func(i, j int) bool { return res.Nodes[i].Name < res.Nodes[j].Name })

	res.GPUUtilization = ratio(gpuAlloc, gpuCap)
	res.CPUUtilization = ratio(cpuAlloc, cpuCap)
	res.MemoryUtilization = ratio(memAlloc, memCap)
}

// ratio divides and rounds to four decimals so the JSON output is stable
// across platforms rather than carrying float noise.
func ratio(part, whole int64) float64 {
	if whole == 0 {
		return 0
	}
	scaled := float64(part) / float64(whole)
	return float64(int64(scaled*1e4+0.5)) / 1e4
}
