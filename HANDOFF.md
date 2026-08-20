# Handoff

Written for the repository owner, not linked from the README.

Branch: `feat/kubernetes-operator`, plus a second branch at the same commit —
see deviation 1. `main` is untouched.

**Neither branch reached GitHub.** The push was refused with
`403 Resource not accessible by integration`: the environment this ran in had
read but not write access to the repository. The 17 commits exist only in the
working clone. See deviation 1 for what to do about it — this is the first
thing to handle, because nothing else in this document matters if the commits
are lost.

---

## 1. What was built, by priority tier

### P0 — everything a 60-second skim touches

- `go build ./...`, `go vet ./...`, `gofmt -l` all clean.
- `go test ./... -race` green, including every pre-existing test. CI matrix
  narrowed to Go 1.24 only (deviation 2).
- `README.md` rewritten: title, one-liner, badges, results table with a Status
  section, extended ASCII architecture diagram, two quickstarts, four new
  "how it works" subsections, three new design tradeoffs, updated layout,
  attribution line.
- `docs/DESIGN.md` written — ~9 sections covering the reconcile loop, the
  autoscaling formula and why lag rather than CPU, the drain protocol as a
  sequence diagram, six failure modes, the placement boundary, and what is
  deliberately out of scope.
- Commit history is 15 conventional commits in build order, none carrying any
  tool or co-author attribution.

### P1 — real code, not scaffolding

- `api/v1alpha1/processingjob_types.go` — every spec and status field from the
  brief, kubebuilder markers for defaults, validation, the status subresource
  and the five printer columns. Plus `defaults.go` (constants mirroring the
  markers) and `Defaulted()`, so the controller resolves the same values
  whether an object came from the API server or a test.
- `internal/controller/` — reconcile loop across five files
  (`processingjob_controller.go`, `statefulset.go`, `autoscale.go`, `drain.go`,
  `coordinator.go`, `equality.go`). No stubs, no `TODO`, no `panic`.
- `cmd/placementsim` + `internal/placement` — **ran, output committed**, unit
  tests passing.
- `cmd/operator/main.go` — manager, leader election, probes on `:8081`, metrics
  on `:8080`, `--zap-log-level`, optional namespace-scoped cache.

### P2 — present and credible, marked honestly

- `deploy/helm/dpe` — passes `helm lint`; `helm template` verified across five
  value permutations (defaults; external Redis; missing external Redis, which
  correctly fails the render; no persistence; no RBAC/SA with a name override).
  Never installed on a cluster.
- `test/e2e` — five tests behind the `e2e` build tag, verified to compile with
  `go vet -tags e2e`. Never executed.
- `deploy/scheduler/binpack-profile.yaml` — valid YAML, parsed and
  structurally checked. Never installed.
- `bench/operator-latency.sh`, `bench/throughput.sh` — `bash -n` clean. Never
  executed.
- `.github/workflows/e2e.yml` — `workflow_dispatch` only, deliberately.

---

## 2. Numbers

### Measured / simulated, with sources

| Figure | Value | Source file |
| --- | --- | --- |
| Placement, bin-pack (**simulated**) | 6 of 12 nodes used, 75.0% GPU utilization, 2 nodes with all GPUs free, largest free block 4 | `bench/results/placement.json` |
| Placement, spread (**simulated**) | 12 of 12 nodes used, 75.0% GPU utilization, 0 nodes with all GPUs free, largest free block 1 | `bench/results/placement.json` |

The four pre-existing engine figures (1.2K → 8.9K records/sec, 92% efficiency,
0 records lost, <1.8s recovery) are unchanged from before this work and remain
attributed to Docker Compose / `make test` in the README table.

### Not measured, and what each needs

Every one of these is blocked by the same thing: **the build environment has no
Docker daemon** (`docker info` fails), so kind could not create a cluster, no
image could be built or loaded, and the chart could not be installed.

| Figure | Harness, already written | Needs |
| --- | --- | --- |
| Reclaim latency after pod eviction (p50/p95, 20 runs) | `bench/operator-latency.sh reclaim` | Docker + kind + kubectl/jq/grpcurl, `make kind-up` |
| Scale-up latency (p50/p95, 10 runs) | `bench/operator-latency.sh scaleup` | same |
| Scale-down safety (zero loss) | `make e2e` → `TestSafeScaleDown` | same |
| Throughput under the operator | `bench/throughput.sh` | same, plus room for 8 workers |

None of these appears in the README results table. `bench/README.md` carries
the full reproduction steps and the parameters that must be recorded alongside
any run — in particular `min-idle`, which dominates reclaim latency and makes
any comparison meaningless if it differs between the two runs being compared.

---

## 3. Deviations from the brief

1. **Nothing was pushed, and there are two branch names.** The brief specifies
   `feat/kubernetes-operator`; the environment this ran in mandated a
   different, pre-assigned branch name and forbade pushing elsewhere. Both
   branches were created locally at the same commit so either can be published.

   Neither could be pushed: `git push` returns
   `403 Resource not accessible by integration`, as does the GitHub API with
   the environment's injected credentials. Read access worked throughout — the
   clone and `git ls-remote` both succeed — so this is a write-scope
   restriction on the session, not a repository or network problem. Nothing was
   force-pushed, `main` was not touched, and no existing history was rewritten.

   To publish, from a checkout that has push rights:

   ```sh
   git fetch <path-or-bundle> feat/kubernetes-operator:feat/kubernetes-operator
   git push -u origin feat/kubernetes-operator
   git branch -D <the-other-branch-name>   # only one needs to exist
   ```

   If you are recovering this from a bundle file, `git clone dpe-operator.bundle`
   or `git fetch dpe-operator.bundle 'refs/heads/*:refs/heads/*'` restores all 17
   commits with their messages and authorship intact.

2. **CI matrix narrowed to Go 1.24.** The brief asks for `go 1.24` in `go.mod`
   while the existing matrix tested 1.22 and 1.24. With a 1.24 directive, the
   1.22 job either fails or silently downloads the 1.24 toolchain and tests the
   same thing twice. Dropping 1.22 keeps the badge honest. The Go badge in the
   README moved to 1.24+ to match.

3. **`internal/placement` holds the simulator's logic; `cmd/placementsim` is a
   thin CLI over it.** The brief says "build `cmd/placementsim`". The split
   matches the existing repository convention (all four other `cmd/` binaries
   are thin wrappers over `internal/`) and is what lets the logic be unit
   tested. Behaviour and CLI surface are as specified.

4. **`spec.coordinatorAddr` added to the CRD**, which the brief's field table
   does not list. The controller has to dial `GetClusterStats` somewhere; it
   defaults to `dpe-coordinator.<namespace>.svc:9090` when unset, which is what
   the Helm chart installs, so the field is optional in practice.

5. **`RemoveConsumer` refuses to delete a consumer holding pending entries**,
   rather than issuing a bare `XGROUP DELCONSUMER`. Redis discards a deleted
   consumer's PEL outright — those entries become neither ackable nor
   reclaimable. An unguarded implementation would have been a record-loss bug
   in the one place the whole project claims not to have one.

6. **`TestZeroGPUPodsIgnoreGPUScoring` asserts the opposite of the intuitive
   result.** With GPUs weighted heavily, `LeastAllocated` pulls CPU-only pods
   *onto* GPU nodes (an untouched GPU term scores 100). That is genuine
   upstream behaviour, verified against kube-scheduler's scorer arithmetic; the
   test and `docs/DESIGN.md` §7 record it as a finding rather than a bug.

7. **A second example, `examples/processingjob-gpu.yaml`**, showing the GPU
   limit and the bin-packing scheduler profile together. Not requested; it is
   the manifest a reader will look for after reading the placement section.

---

## 4. What is unfinished, ranked by what a reviewer notices first

1. **No operator reconcile has ever run against a real API server.** The logic
   is tested against controller-runtime's fake client, which does not enforce
   admission, defaulting, or field immutability. The most likely first-run
   failure is something the fake client tolerates and a real one rejects.
2. **No cluster-dependent benchmark exists.** Four rows in `bench/results/` are
   absent. A reviewer who scans the results table sees one simulated row and
   four engine rows from before this work.
3. **The Helm chart has never been installed.** Lint and template are much
   weaker guarantees than an install — probe timings, the Redis PVC, and RBAC
   sufficiency are all unverified in practice.
4. **The e2e suite has never executed.** It compiles and is genuinely written,
   but "compiles" says nothing about whether its waits and thresholds are
   calibrated for a real kind cluster.
5. **The `e2e` workflow has never run**, which is why it is dispatch-only.
6. **The bin-packing scheduler profile has never been installed.** Its schema
   is correct for `kubescheduler.config.k8s.io/v1`, but the plugin
   enable/disable lists are the kind of thing a real scheduler rejects loudly.

---

## 5. The three riskiest things to check before showing this to anyone

1. **Run `make kind-up && make e2e` once.** Everything in §4 collapses into
   this one command. If it passes, most of the Status section can be rewritten
   in the strong form; if it fails, you will find out from your own terminal
   rather than from an interviewer's screen share. Budget an hour — first runs
   of an unexecuted e2e suite rarely pass clean, and the likely failures are
   timing calibration, not logic.

2. **Confirm the README's Status section still matches reality after you touch
   anything.** It is the single highest-risk paragraph in the repository: it is
   what converts "unfinished" into "honest work in progress", and a stale
   version of it converts the same work into overclaiming. Nothing else in the
   README is load-bearing in that way.

3. **Re-read `git log --format='%an <%ae>'` and every commit body.** The whole
   history should be one author with no tool attribution. This was checked, but
   it is the one defect that cannot be fixed after the repository is public
   without rewriting history.

---

## 6. Claims a hiring manager could challenge

**"You said you built a Kubernetes operator, but nothing ran on Kubernetes."**
Correct, and the README says so in the Status section rather than burying it.
The controller is complete, reviewable code with unit tests against a fake API
server; the honest description is "written and unit-tested, not yet exercised
on a cluster", which is what the README says.

**"Your GPU numbers are simulated. Isn't that just made up?"**
It is a simulation of a specific, documented thing: kube-scheduler's
`NodeResourcesFit` filter and its `mostRequestedScore` / `leastRequestedScore`
functions, reimplemented including their integer truncation and their rule that
a resource a node lacks is dropped from the weighted mean. It models nothing
else — no affinity, taints, preemption, or the other scoring plugins — and it
breaks scoring ties by name where upstream breaks them randomly. Every place
the number appears carries the word "simulated".

**"Bin-packing didn't improve utilization at all — 75% either way."**
Right, and that is the finding rather than a disappointment. Both strategies
placed every pod, so neither fit more work in. What changed is fragmentation:
bin-pack left a contiguous 4-GPU block, spread left eight stranded single GPUs.
A later 4-GPU job schedules in the first cluster and not in the second. Quoting
a utilization improvement here would have been the dishonest version.

**"Why not a HorizontalPodAutoscaler with an external metrics adapter? That's
the standard shape."**
Because an HPA scales by patching `spec.replicas`, and that is exactly the
operation that must not happen without draining the victim first. Two writers
of that field is the bug the drain protocol exists to prevent. Owning the
replica count is the price of owning scale-in safety — noted in
`docs/DESIGN.md` §8 as a deliberate tradeoff, not an oversight.

**"You called this GPU-aware placement but you didn't write a scheduler plugin."**
Deliberately, and the README and DESIGN both say so in those words. What is
shipped is a `KubeSchedulerConfiguration` profile plus the pass-through that
gets `nvidia.com/gpu` onto the pod spec. A plugin would allow scoring on
consumer-group state, at the cost of a scheduler binary to build and keep in
step with Kubernetes releases; nothing here needs that.

**"Is the StatefulSet choice real, or just a preference?"**
It is forced by the engine. A Redis consumer group tracks pending entries per
consumer *name*, and the worker takes its name from the pod hostname. Stable
names mean a restarted pod recovers its own in-flight records; generated names
mean every restart orphans work for a full lease and leaves a dead consumer
behind. The scale-in ordinal being predictable is a second consequence, and it
is what makes drain-before-scale possible at all.

**"How do I know the drain path actually works? You couldn't run it."**
The drain protocol's decision logic is covered by unit tests against a fake API
server: the replica count provably does not move until the victim leaves the
registry, a drain that outruns its budget provably leaves the pod running, and
backlog returning mid-drain provably abandons the scale-in. What is untested is
the integration — that a real worker pod, receiving a real drain broadcast,
deregisters within the budget. The engine's own drain path is separately
covered by `TestDrainCommitsInFlightWork`, which does run, against real Redis.
