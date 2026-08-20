package placement

import (
	"bytes"
	"testing"
)

func nodes(spec ...Node) []Node { return spec }

func node(name string, cpuMillis, memBytes, gpus int64) Node {
	return Node{Name: name, Allocatable: Resources{CPUMillis: cpuMillis, MemBytes: memBytes, GPUs: gpus}}
}

func pod(name string, cpuMillis, memBytes, gpus int64) Pod {
	return Pod{Name: name, Request: Resources{CPUMillis: cpuMillis, MemBytes: memBytes, GPUs: gpus}}
}

func pods(n int, name string, cpuMillis, memBytes, gpus int64) []Pod {
	out := make([]Pod, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, pod(name+"-"+itoa(i), cpuMillis, memBytes, gpus))
	}
	return out
}

func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	var buf [8]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	return string(buf[pos:])
}

const gibibyte = 1 << 30

// TestAllPodsFitBinPackUsesFewerNodes is the whole reason the bin-packing
// profile exists: identical utilization, very different fragmentation.
func TestAllPodsFitBinPackUsesFewerNodes(t *testing.T) {
	inventory := nodes(
		node("gpu-0", 32000, 256*gibibyte, 4),
		node("gpu-1", 32000, 256*gibibyte, 4),
		node("gpu-2", 32000, 256*gibibyte, 4),
		node("gpu-3", 32000, 256*gibibyte, 4),
	)
	workload := pods(8, "worker", 4000, 16*gibibyte, 1)

	packed := Simulate(inventory, workload, MostAllocated, DefaultWeights)
	spread := Simulate(inventory, workload, LeastAllocated, DefaultWeights)

	if packed.PodsUnschedulable != 0 || spread.PodsUnschedulable != 0 {
		t.Fatalf("pods went unschedulable in a cluster with room: packed=%d spread=%d",
			packed.PodsUnschedulable, spread.PodsUnschedulable)
	}
	if packed.NodesUsed != 2 {
		t.Fatalf("bin-pack used %d nodes for 8 single-GPU pods on 4-GPU nodes, want 2", packed.NodesUsed)
	}
	if spread.NodesUsed != 4 {
		t.Fatalf("spread used %d nodes, want all 4", spread.NodesUsed)
	}
	if packed.GPUUtilization != spread.GPUUtilization {
		t.Fatalf("GPU utilization differs (%v vs %v); both placed every pod, so it must not",
			packed.GPUUtilization, spread.GPUUtilization)
	}
	if packed.LargestFreeGPUBlock != 4 || spread.LargestFreeGPUBlock != 2 {
		t.Fatalf("largest free GPU block: packed=%d (want 4), spread=%d (want 2)",
			packed.LargestFreeGPUBlock, spread.LargestFreeGPUBlock)
	}
}

// TestSomePodsUnschedulable asserts the filter is real: demand beyond capacity
// is reported, not silently packed in.
func TestSomePodsUnschedulable(t *testing.T) {
	inventory := nodes(
		node("gpu-0", 32000, 64*gibibyte, 2),
		node("gpu-1", 32000, 64*gibibyte, 2),
	)
	workload := pods(6, "worker", 1000, 4*gibibyte, 1)

	for _, strategy := range []Strategy{MostAllocated, LeastAllocated} {
		res := Simulate(inventory, workload, strategy, DefaultWeights)
		if res.PodsScheduled != 4 {
			t.Fatalf("%s scheduled %d pods onto 4 GPUs, want 4", strategy, res.PodsScheduled)
		}
		if res.PodsUnschedulable != 2 {
			t.Fatalf("%s reported %d unschedulable, want 2", strategy, res.PodsUnschedulable)
		}
		if len(res.Unschedulable) != 2 || res.Unschedulable[0] != "worker-4" {
			t.Fatalf("%s unschedulable list = %v, want the last two pods in order", strategy, res.Unschedulable)
		}
		if res.GPUUtilization != 1 {
			t.Fatalf("%s left GPUs idle while pods were unschedulable: utilization %v", strategy, res.GPUUtilization)
		}
	}
}

// TestSingleNodeCluster covers the degenerate inventory: with one candidate
// the strategies cannot differ, and the node must still fill until it is full.
func TestSingleNodeCluster(t *testing.T) {
	inventory := nodes(node("only", 8000, 32*gibibyte, 2))
	workload := pods(3, "worker", 2000, 8*gibibyte, 1)

	for _, strategy := range []Strategy{MostAllocated, LeastAllocated} {
		res := Simulate(inventory, workload, strategy, DefaultWeights)
		if res.NodesUsed != 1 || res.NodesTotal != 1 {
			t.Fatalf("%s used %d of %d nodes, want 1 of 1", strategy, res.NodesUsed, res.NodesTotal)
		}
		if res.PodsScheduled != 2 || res.PodsUnschedulable != 1 {
			t.Fatalf("%s scheduled %d and rejected %d, want 2 and 1",
				strategy, res.PodsScheduled, res.PodsUnschedulable)
		}
		if res.NodesFullyFreeGPU != 0 {
			t.Fatalf("%s reported %d nodes with all GPUs free, want 0", strategy, res.NodesFullyFreeGPU)
		}
	}
}

// TestZeroGPUPodsIgnoreGPUScoring covers the subtle upstream rule this
// simulator has to get right — a resource a node does not have is dropped from
// the weighted mean entirely, weight included — and the consequence of getting
// it right, which is not the intuitive one.
//
// With a heavily weighted GPU term, an untouched GPU node scores 100 on that
// term under LeastAllocated. So the spread strategy pulls CPU-only pods *onto*
// GPU nodes, occupying cpu and memory that GPU work will later need. Bin-pack
// inverts it: an empty GPU term scores 0, so CPU-only pods land on CPU-only
// nodes and the GPU fleet is left alone.
//
// That asymmetry is a real property of weighting GPUs in a scoring strategy,
// and it is the second reason the bin-packing profile exists.
func TestZeroGPUPodsIgnoreGPUScoring(t *testing.T) {
	inventory := nodes(
		node("cpu-0", 16000, 64*gibibyte, 0),
		node("gpu-0", 16000, 64*gibibyte, 4),
	)
	workload := pods(4, "stream", 2000, 4*gibibyte, 0)

	spread := Simulate(inventory, workload, LeastAllocated, DefaultWeights)
	if spread.PodsUnschedulable != 0 {
		t.Fatalf("CPU-only pods went unschedulable on CPU-capable nodes: %v", spread.Unschedulable)
	}
	if spread.GPUUtilization != 0 {
		t.Fatalf("GPU utilization = %v for a workload requesting no GPUs", spread.GPUUtilization)
	}
	if got := nodeResult(t, spread, "gpu-0").Pods; got != 4 {
		t.Fatalf("spread put %d CPU-only pods on the GPU node, want all 4: an empty GPU "+
			"term scores 100 under LeastAllocated", got)
	}

	// Bin-pack keeps them off: with nothing allocated the GPU term scores 0,
	// so the CPU-only node wins on the weighted mean.
	packed := Simulate(inventory, workload, MostAllocated, DefaultWeights)
	if got := nodeResult(t, packed, "cpu-0").Pods; got != 4 {
		t.Fatalf("bin-pack put %d CPU-only pods on the CPU-only node, want all 4", got)
	}
	if got := nodeResult(t, packed, "gpu-0").Pods; got != 0 {
		t.Fatalf("bin-pack leaked %d CPU-only pods onto the GPU node", got)
	}
}

func nodeResult(t *testing.T, res Result, name string) NodeResult {
	t.Helper()
	for _, n := range res.Nodes {
		if n.Name == name {
			return n
		}
	}
	t.Fatalf("no node named %q in the result", name)
	return NodeResult{}
}

// TestDeterminism is the property the committed result file depends on: the
// same input must produce byte-identical output, run after run.
func TestDeterminism(t *testing.T) {
	fixture := []byte(`
name: determinism
nodes:
  - name: gpu-node
    count: 6
    cpu: "32"
    memory: 256Gi
    gpu: 4
  - name: cpu-node
    count: 3
    cpu: "16"
    memory: 64Gi
pods:
  - name: inference
    count: 15
    cpu: "4"
    memory: 16Gi
    gpu: 1
  - name: stream
    count: 9
    cpu: "2"
    memory: 4Gi
`)

	var first []byte
	for i := 0; i < 8; i++ {
		f, ns, ps, err := ParseFixture(fixture)
		if err != nil {
			t.Fatalf("parse: %v", err)
		}
		blob, err := BuildReport(f, ns, ps).JSON()
		if err != nil {
			t.Fatalf("render: %v", err)
		}
		if first == nil {
			first = blob
			continue
		}
		if !bytes.Equal(first, blob) {
			t.Fatalf("run %d produced different output; the simulation is not deterministic", i)
		}
	}
}

// TestSimulateDoesNotMutateInput lets one inventory be reused across
// strategies, which is what BuildReport relies on.
func TestSimulateDoesNotMutateInput(t *testing.T) {
	inventory := nodes(node("gpu-0", 32000, 64*gibibyte, 4))
	workload := pods(2, "worker", 1000, gibibyte, 1)

	Simulate(inventory, workload, MostAllocated, DefaultWeights)

	if inventory[0].Allocated != (Resources{}) {
		t.Fatalf("input node was mutated: %+v", inventory[0].Allocated)
	}
	if len(inventory[0].Pods) != 0 {
		t.Fatalf("input node accumulated %d pods", len(inventory[0].Pods))
	}
}

func TestParseFixtureRejectsBadInput(t *testing.T) {
	cases := map[string]string{
		"no nodes":         "name: x\npods:\n  - name: p\n    count: 1\n    cpu: \"1\"\n",
		"no pods":          "name: x\nnodes:\n  - name: n\n    count: 1\n    cpu: \"1\"\n",
		"bad quantity":     "name: x\nnodes:\n  - name: n\n    count: 1\n    cpu: \"four\"\npods:\n  - name: p\n    count: 1\n    cpu: \"1\"\n",
		"negative gpu":     "name: x\nnodes:\n  - name: n\n    count: 1\n    cpu: \"1\"\n    gpu: -1\npods:\n  - name: p\n    count: 1\n    cpu: \"1\"\n",
		"not yaml at all":  "\t- [unbalanced\n",
		"empty everything": "",
	}
	for name, doc := range cases {
		t.Run(name, func(t *testing.T) {
			if _, _, _, err := ParseFixture([]byte(doc)); err == nil {
				t.Fatal("expected an error, got none")
			}
		})
	}
}

// TestFixtureWeightsOverrideDefaults asserts a fixture can model a scheduler
// profile tuned differently from the shipped one.
func TestFixtureWeightsOverrideDefaults(t *testing.T) {
	f, _, _, err := ParseFixture([]byte(`
name: weighted
weights:
  gpu: 3
  cpu: 2
  memory: 1
nodes:
  - name: n
    count: 1
    cpu: "8"
    memory: 8Gi
pods:
  - name: p
    count: 1
    cpu: "1"
    memory: 1Gi
`))
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := f.WeightsOf(); got != (Weights{CPU: 2, Memory: 1, GPU: 3}) {
		t.Fatalf("weights = %+v, want the fixture's override", got)
	}
}

// TestResourceScoreMatchesUpstream pins the two scorers against values taken
// from kube-scheduler's own arithmetic, including its integer truncation.
func TestResourceScoreMatchesUpstream(t *testing.T) {
	cases := []struct {
		requested, capacity int64
		strategy            Strategy
		want                int64
	}{
		{0, 100, MostAllocated, 0},
		{50, 100, MostAllocated, 50},
		{100, 100, MostAllocated, 100},
		{33, 100, MostAllocated, 33},
		{1, 3, MostAllocated, 33}, // truncates, as upstream does
		{0, 100, LeastAllocated, 100},
		{50, 100, LeastAllocated, 50},
		{100, 100, LeastAllocated, 0},
		{101, 100, MostAllocated, 0}, // over capacity scores zero, not negative
		{101, 100, LeastAllocated, 0},
		{5, 0, MostAllocated, 0}, // no capacity, no score
	}
	for _, tc := range cases {
		if got := resourceScore(tc.requested, tc.capacity, tc.strategy); got != tc.want {
			t.Errorf("resourceScore(%d, %d, %s) = %d, want %d",
				tc.requested, tc.capacity, tc.strategy, got, tc.want)
		}
	}
}
