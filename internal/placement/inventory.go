// Package placement simulates kube-scheduler's NodeResourcesFit scoring over a
// fixed node inventory and pod workload, so the cost of bin-packing versus
// spreading GPU work can be measured without a cluster.
//
// It reimplements the two scoring strategies the real scheduler ships —
// MostAllocated and LeastAllocated — rather than approximating them, because
// the point of the exercise is to predict what the profile in
// deploy/scheduler would actually do. What it does not reimplement is the rest
// of the scheduler: affinity, taints, preemption, priority and the plugins
// that run alongside NodeResourcesFit are all out of scope, and results here
// are labelled simulated everywhere they appear.
package placement

import (
	"fmt"
	"sort"

	"k8s.io/apimachinery/pkg/api/resource"
	"sigs.k8s.io/yaml"
)

// Resources is one node's capacity or one pod's request, in the units the
// scheduler itself uses: milli-cores, bytes, and whole GPUs.
type Resources struct {
	CPUMillis int64
	MemBytes  int64
	GPUs      int64
}

// Node is one schedulable machine.
type Node struct {
	Name        string
	Allocatable Resources
	// Allocated is what has been placed on this node so far in the run.
	Allocated Resources
	// Pods lists the pod names placed here, in placement order.
	Pods []string
}

// fits reports whether a pod's request still fits in this node's remaining
// allocatable capacity. This is the NodeResourcesFit *filter*: a node that
// fails it is not scored at all.
func (n *Node) fits(req Resources) bool {
	return n.Allocated.CPUMillis+req.CPUMillis <= n.Allocatable.CPUMillis &&
		n.Allocated.MemBytes+req.MemBytes <= n.Allocatable.MemBytes &&
		n.Allocated.GPUs+req.GPUs <= n.Allocatable.GPUs
}

// place commits a pod to this node.
func (n *Node) place(name string, req Resources) {
	n.Allocated.CPUMillis += req.CPUMillis
	n.Allocated.MemBytes += req.MemBytes
	n.Allocated.GPUs += req.GPUs
	n.Pods = append(n.Pods, name)
}

// used reports whether anything was placed here.
func (n *Node) used() bool { return len(n.Pods) > 0 }

// Pod is one unit of work to schedule.
type Pod struct {
	Name    string
	Request Resources
}

// Weights are the per-resource weights from the scheduler profile's
// scoringStrategy. They must match deploy/scheduler/binpack-profile.yaml for
// the simulation to predict that profile's behaviour.
type Weights struct {
	CPU    int64
	Memory int64
	GPU    int64
}

// DefaultWeights mirror the shipped profile: GPUs dominate because a stranded
// fraction of a GPU node is the expensive kind of waste.
var DefaultWeights = Weights{CPU: 1, Memory: 1, GPU: 10}

// Fixture is the on-disk description of a simulation input.
type Fixture struct {
	Name        string        `json:"name"`
	Description string        `json:"description,omitempty"`
	Weights     *FixtureScore `json:"weights,omitempty"`
	Nodes       []NodeGroup   `json:"nodes"`
	Pods        []PodGroup    `json:"pods"`
}

// FixtureScore is the optional weight override in a fixture.
type FixtureScore struct {
	CPU    int64 `json:"cpu"`
	Memory int64 `json:"memory"`
	GPU    int64 `json:"gpu"`
}

// NodeGroup expands into Count identical nodes named "<name>-<i>".
type NodeGroup struct {
	Name   string `json:"name"`
	Count  int    `json:"count"`
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
	GPU    int64  `json:"gpu,omitempty"`
}

// PodGroup expands into Count identical pods named "<name>-<i>".
type PodGroup struct {
	Name   string `json:"name"`
	Count  int    `json:"count"`
	CPU    string `json:"cpu"`
	Memory string `json:"memory"`
	GPU    int64  `json:"gpu,omitempty"`
}

// ParseFixture reads a YAML fixture and expands it into nodes and pods.
//
// Expansion order is the fixture's own order, and every name carries its
// index, so the same file always yields the same slices — which is what makes
// the whole simulation reproducible.
func ParseFixture(data []byte) (*Fixture, []Node, []Pod, error) {
	var f Fixture
	if err := yaml.Unmarshal(data, &f); err != nil {
		return nil, nil, nil, fmt.Errorf("parse fixture: %w", err)
	}
	if len(f.Nodes) == 0 {
		return nil, nil, nil, fmt.Errorf("fixture %q declares no nodes", f.Name)
	}
	if len(f.Pods) == 0 {
		return nil, nil, nil, fmt.Errorf("fixture %q declares no pods", f.Name)
	}

	var nodes []Node
	for _, g := range f.Nodes {
		res, err := parseResources(g.CPU, g.Memory, g.GPU)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("node group %q: %w", g.Name, err)
		}
		for i := 0; i < g.Count; i++ {
			nodes = append(nodes, Node{
				Name:        fmt.Sprintf("%s-%d", g.Name, i),
				Allocatable: res,
			})
		}
	}

	var pods []Pod
	for _, g := range f.Pods {
		res, err := parseResources(g.CPU, g.Memory, g.GPU)
		if err != nil {
			return nil, nil, nil, fmt.Errorf("pod group %q: %w", g.Name, err)
		}
		for i := 0; i < g.Count; i++ {
			pods = append(pods, Pod{
				Name:    fmt.Sprintf("%s-%d", g.Name, i),
				Request: res,
			})
		}
	}

	// Node order is normalised by name so a fixture that lists its groups in a
	// different order still produces the same tie-breaks.
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Name < nodes[j].Name })
	return &f, nodes, pods, nil
}

// WeightsOf returns the fixture's weights, or the shipped defaults.
func (f *Fixture) WeightsOf() Weights {
	if f.Weights == nil {
		return DefaultWeights
	}
	return Weights{CPU: f.Weights.CPU, Memory: f.Weights.Memory, GPU: f.Weights.GPU}
}

// parseResources converts Kubernetes quantity strings into scheduler units.
func parseResources(cpu, mem string, gpu int64) (Resources, error) {
	var out Resources
	if cpu != "" {
		q, err := resource.ParseQuantity(cpu)
		if err != nil {
			return out, fmt.Errorf("cpu %q: %w", cpu, err)
		}
		out.CPUMillis = q.MilliValue()
	}
	if mem != "" {
		q, err := resource.ParseQuantity(mem)
		if err != nil {
			return out, fmt.Errorf("memory %q: %w", mem, err)
		}
		out.MemBytes = q.Value()
	}
	if gpu < 0 {
		return out, fmt.Errorf("gpu count %d is negative", gpu)
	}
	out.GPUs = gpu
	return out, nil
}
