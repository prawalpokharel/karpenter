/*
Copyright The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package disruption_test

// Candidate ordering study for kubernetes-sigs/karpenter#3141.
//
// The study replays one seeded cluster and workload through several disruption candidate orderings. Candidate
// construction, budgets, scheduling simulation, consolidation price filtering and today's four method objects are
// Karpenter's own code, run against envtest. What the study models (virtual time per simulation, instant command
// execution, drain times, pod readiness and PDB status) is listed in the study's PLAN.md.
//
// It only runs when ORDERING_STUDY_CONFIG points at a JSON config:
//
//	ORDERING_STUDY_CONFIG=cfg.json go test ./pkg/controllers/disruption -run TestCandidateOrderingStudy -timeout 0

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/awslabs/operatorpkg/status"
	. "github.com/onsi/gomega"
	"github.com/samber/lo"
	corev1 "k8s.io/api/core/v1"
	policyv1 "k8s.io/api/policy/v1"
	schedulingv1 "k8s.io/api/scheduling/v1"
	"k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/resource"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/apimachinery/pkg/util/intstr"
	clocktesting "k8s.io/utils/clock/testing"
	"sigs.k8s.io/controller-runtime/pkg/cache/informertest"
	"sigs.k8s.io/controller-runtime/pkg/client"

	kwokcloudprovider "sigs.k8s.io/karpenter/kwok/cloudprovider"
	kwokoptions "sigs.k8s.io/karpenter/kwok/options"
	coreapis "sigs.k8s.io/karpenter/pkg/apis"
	v1 "sigs.k8s.io/karpenter/pkg/apis/v1"
	"sigs.k8s.io/karpenter/pkg/cloudprovider"
	"sigs.k8s.io/karpenter/pkg/cloudprovider/fake"
	"sigs.k8s.io/karpenter/pkg/controllers/disruption"
	"sigs.k8s.io/karpenter/pkg/controllers/dynamicresources/deviceallocation"
	"sigs.k8s.io/karpenter/pkg/controllers/provisioning"
	"sigs.k8s.io/karpenter/pkg/controllers/state"
	"sigs.k8s.io/karpenter/pkg/controllers/state/informer"
	"sigs.k8s.io/karpenter/pkg/metrics"
	"sigs.k8s.io/karpenter/pkg/operator/injection"
	"sigs.k8s.io/karpenter/pkg/operator/options"
	"sigs.k8s.io/karpenter/pkg/state/cost"
	"sigs.k8s.io/karpenter/pkg/state/nodepoolbackoff"
	"sigs.k8s.io/karpenter/pkg/state/prediction"
	"sigs.k8s.io/karpenter/pkg/state/virtualpods"
	"sigs.k8s.io/karpenter/pkg/test"
	"sigs.k8s.io/karpenter/pkg/test/expectations"
	"sigs.k8s.io/karpenter/pkg/test/v1alpha1"
)

const (
	studyCriticalPriority = int32(1_000_000)
	studyBatchPriority    = int32(-100)
	studyReadyDelay       = 45 * time.Second
	studyDeleteDrain      = 60 * time.Second
	studyReplaceDrain     = 150 * time.Second
	studyPDBRound         = 60 * time.Second
	studyValidationDelay  = 15 * time.Second
	studyPollPeriod       = 10 * time.Second
	studyNamespace        = "default"
	studyAppLabel         = "app"
	studyMaxLoops         = 20000
)

var studyEpoch = time.Date(2026, 1, 5, 12, 0, 0, 0, time.UTC)

// ---------------------------------------------------------------- config and scenarios

type studyConfig struct {
	Scenario   string   `json:"scenario"`
	Seed       int64    `json:"seed"`
	Arms       []string `json:"arms"`
	OutDir     string   `json:"out_dir"`
	HorizonMin float64  `json:"horizon_min"`
	Replicate  int      `json:"replicate"`
	// SnapshotOut builds the seed's start state with Karpenter's provisioner and saves it; no arms run.
	// SnapshotIn restores that saved start state, so every arm of a seed starts from the identical cluster.
	SnapshotOut string `json:"snapshot_out"`
	SnapshotIn  string `json:"snapshot_in"`
}

type studySnapNode struct {
	Labels       map[string]string `json:"labels"`
	Annotations  map[string]string `json:"annotations"`
	Spec         v1.NodeClaimSpec  `json:"spec"`
	InstanceType string            `json:"instance_type"`
	Zone         string            `json:"zone"`
	CapacityType string            `json:"capacity_type"`
}

type studySnapPod struct {
	Svc  int `json:"svc"`
	Node int `json:"node"`
}

type studySnapshot struct {
	Scenario string          `json:"scenario"`
	Seed     int64           `json:"seed"`
	Nodes    []studySnapNode `json:"nodes"`
	Pods     []studySnapPod  `json:"pods"`
}

type studyScenario struct {
	AppPools     int
	Services     int
	MemServices  int
	BatchSvcs    int
	CriticalFrac float64
	SSim         time.Duration
	DriftAtMin   float64
	WaveSize     int
}

func studyScenarios() map[string]studyScenario {
	s1 := studyScenario{AppPools: 1, Services: 70, MemServices: 8, BatchSvcs: 10, CriticalFrac: 0.15, SSim: 250 * time.Millisecond, DriftAtMin: -1, WaveSize: 3}
	s2 := s1
	s2.DriftAtMin = 10
	s3 := studyScenario{AppPools: 10, Services: 340, MemServices: 40, BatchSvcs: 50, CriticalFrac: 0.15, SSim: 840 * time.Millisecond, DriftAtMin: -1, WaveSize: 3}
	s4 := s1
	s4.CriticalFrac = 0.40
	smoke := studyScenario{AppPools: 1, Services: 14, MemServices: 2, BatchSvcs: 3, CriticalFrac: 0.3, SSim: 250 * time.Millisecond, DriftAtMin: 5, WaveSize: 3}
	return map[string]studyScenario{"S1": s1, "S2": s2, "S3": s3, "S4": s4, "SMOKE": smoke}
}

type studyService struct {
	Name     string
	Pool     string
	Kind     string // critical, standard, batch
	CPUMilli int64
	MemMi    int64
	Desired  int
	PDB      string // "", "maxu1", "min75"
	Spread   bool
	CostAnno bool
	FinishAt time.Duration // batch only, <0 never
}

type studyEvent struct {
	At    time.Duration
	Kind  string // scale, finish, drift
	Svc   int
	Delta int
	Frac  float64
}

type studyWorkload struct {
	Pools    []string
	Services []*studyService
	Events   []studyEvent
	Waves    [][]int
}

func studyMakeWorkload(sc studyScenario, seed int64) studyWorkload {
	rng := rand.New(rand.NewSource(seed*7919 + 17))
	w := studyWorkload{}
	appPools := []string{"apps"}
	if sc.AppPools > 1 {
		appPools = lo.Times(sc.AppPools, func(i int) string { return fmt.Sprintf("apps-%d", i) })
	}
	w.Pools = append(append([]string{}, appPools...), "mem", "batch")
	cpuChoices := []int64{250, 500, 500, 1000, 1000, 1000, 2000, 4000}
	memPerCPU := []int64{1024, 2048, 2048, 4096}
	newSvc := func(i int, pool string, critical bool) *studyService {
		s := &studyService{Name: fmt.Sprintf("svc-%03d", i), Pool: pool, Kind: "standard", FinishAt: -1}
		if critical {
			s.Kind = "critical"
		}
		s.CPUMilli = cpuChoices[rng.Intn(len(cpuChoices))]
		s.MemMi = s.CPUMilli * memPerCPU[rng.Intn(len(memPerCPU))] / 1000
		if critical {
			if rng.Float64() < 0.15 {
				s.Desired = 1
			} else {
				s.Desired = 2 + rng.Intn(5)
			}
		} else {
			s.Desired = 1 + rng.Intn(8)
		}
		if critical && s.Desired >= 2 {
			s.Spread = true
			s.PDB = "maxu1"
		} else if !critical && s.Desired >= 3 && rng.Float64() < 0.5 {
			s.PDB = lo.Ternary(rng.Float64() < 0.5, "maxu1", "min75")
		}
		s.CostAnno = rng.Float64() < 0.10
		return s
	}
	idx := 0
	for range sc.Services {
		w.Services = append(w.Services, newSvc(idx, appPools[rng.Intn(len(appPools))], rng.Float64() < sc.CriticalFrac))
		idx++
	}
	for range sc.MemServices {
		s := newSvc(idx, "mem", rng.Float64() < sc.CriticalFrac)
		s.MemMi = 8192 + int64(rng.Intn(3))*4096
		w.Services = append(w.Services, s)
		idx++
	}
	for range sc.BatchSvcs {
		s := &studyService{Name: fmt.Sprintf("svc-%03d", idx), Pool: "batch", Kind: "batch",
			CPUMilli: []int64{1000, 2000, 4000}[rng.Intn(3)], Desired: 2 + rng.Intn(9)}
		s.MemMi = s.CPUMilli * 2
		s.FinishAt = time.Duration((5 + rng.Float64()*85) * float64(time.Minute))
		w.Services = append(w.Services, s)
		idx++
	}
	order := rng.Perm(len(w.Services))
	for i := 0; i < len(order); i += sc.WaveSize {
		w.Waves = append(w.Waves, order[i:min(i+sc.WaveSize, len(order))])
	}
	for i, s := range w.Services {
		if s.Kind == "batch" {
			w.Events = append(w.Events, studyEvent{At: s.FinishAt, Kind: "finish", Svc: i})
			continue
		}
		if rng.Float64() < 0.5 {
			w.Events = append(w.Events, studyEvent{At: time.Duration(rng.Float64() * float64(60*time.Minute)), Kind: "scale", Svc: i, Frac: 0.4 + 0.2*rng.Float64()})
		}
		if rng.Float64() < 0.2 {
			w.Events = append(w.Events, studyEvent{At: time.Duration((30 + rng.Float64()*60) * float64(time.Minute)), Kind: "scale", Svc: i, Delta: 1 + rng.Intn(3)})
		}
	}
	if sc.DriftAtMin >= 0 {
		w.Events = append(w.Events, studyEvent{At: time.Duration(sc.DriftAtMin * float64(time.Minute)), Kind: "drift"})
	}
	sort.SliceStable(w.Events, func(i, j int) bool { return w.Events[i].At < w.Events[j].At })
	return w
}

// ---------------------------------------------------------------- simulation counting and virtual time

// studyCountingClient counts listings of unscheduled pods. SimulateScheduling lists them exactly once per call
// (Provisioner.GetPendingPods), and nothing else in a disruption loop does, so the count is the number of
// scheduling simulations.
type studyCountingClient struct {
	client.Client
	pendingLists atomic.Int64
}

func (c *studyCountingClient) List(ctx context.Context, list client.ObjectList, opts ...client.ListOption) error {
	if _, ok := list.(*corev1.PodList); ok {
		lopts := &client.ListOptions{}
		lopts.ApplyOptions(opts)
		if lopts.FieldSelector != nil && lopts.FieldSelector.String() == "spec.nodeName=" {
			c.pendingLists.Add(1)
		}
	}
	return c.Client.List(ctx, list, opts...)
}

// studyVirtualClock reads the fake clock plus s_sim for every simulation since the loop started. Method objects
// get this clock, so SingleNodeConsolidation's own 3 minute timeout check fires after 180 s / s_sim simulations.
type studyVirtualClock struct {
	*clocktesting.FakeClock
	counter *studyCountingClient
	mark    int64
	sSim    time.Duration
}

func (v *studyVirtualClock) simsSinceMark() int64 { return v.counter.pendingLists.Load() - v.mark }
func (v *studyVirtualClock) Now() time.Time {
	return v.FakeClock.Now().Add(time.Duration(v.simsSinceMark()) * v.sSim)
}
func (v *studyVirtualClock) Since(t time.Time) time.Duration { return v.Now().Sub(t) }

// ---------------------------------------------------------------- run state

type studyNode struct {
	Name, ProviderID, NodeClaim, Pool string
	Seq                               int
	Price                             float64
	Start, End                        time.Duration
	DriftedAt                         time.Duration
	Deleting                          bool
}

type studyPod struct {
	Key     types.NamespacedName
	Svc     int
	Node    string
	ReadyAt time.Duration
	Index   int
	Evicted int
	Pending bool
}

type studyPDB struct {
	Name    string
	Svc     int
	Kind    string
	Allowed int32
}

type studyCommandLog struct {
	TMin       float64 `json:"t_min"`
	Reason     string  `json:"reason"`
	Decision   string  `json:"decision"`
	Nodes      int     `json:"nodes"`
	Pods       int     `json:"pods"`
	Critical   int     `json:"critical"`
	Savings    float64 `json:"est_savings_per_hour"`
	Sims       int64   `json:"sims"`
	PDBStalled bool    `json:"pdb_stalled"`
}

type studyResult struct {
	Scenario              string            `json:"scenario"`
	Seed                  int64             `json:"seed"`
	Arm                   string            `json:"arm"`
	Replicate             int               `json:"replicate"`
	HorizonMin            float64           `json:"horizon_min"`
	CostDollars           float64           `json:"cost_dollars"`
	StartCostPerHour      float64           `json:"start_cost_per_hour"`
	FinalCostPerHour      float64           `json:"final_cost_per_hour"`
	StartNodes            int               `json:"start_nodes"`
	EndNodes              int               `json:"end_nodes"`
	StartPods             int               `json:"start_pods"`
	PodsEvicted           int               `json:"pods_evicted"`
	CriticalPodsEvicted   int               `json:"critical_pods_evicted"`
	PodsEvicted2Plus      int               `json:"pods_evicted_2plus"`
	PodsUnplaced          int               `json:"pods_unplaced"`
	CriticalOutages       int               `json:"critical_outages"`
	CriticalOutageSeconds float64           `json:"critical_outage_seconds"`
	PDBStalledCommands    int               `json:"pdb_stalled_commands"`
	PDBBlockedNodeMinutes float64           `json:"pdb_blocked_node_minutes"`
	Commands              int               `json:"commands"`
	CommandsByReason      map[string]int    `json:"commands_by_reason"`
	NodesDisrupted        int               `json:"nodes_disrupted"`
	Simulations           int64             `json:"simulations"`
	Loops                 int               `json:"loops"`
	Timeouts              int               `json:"timeouts"`
	SimWallSeconds        float64           `json:"sim_wall_seconds"`
	DriftedTotal          int               `json:"drifted_total"`
	DriftedResolved       int               `json:"drifted_resolved"`
	DriftedLeft           int               `json:"drifted_left"`
	DriftResolveMeanMin   float64           `json:"drift_resolve_mean_min"`
	DriftResolveMaxMin    float64           `json:"drift_resolve_max_min"`
	DriftLeftMaxAgeMin    float64           `json:"drift_left_max_age_min"`
	PendingAtEnd          int               `json:"pending_at_end"`
	WallSeconds           float64           `json:"wall_seconds"`
	CostCurve             [][2]float64      `json:"cost_curve"`
	CommandLog            []studyCommandLog `json:"command_log"`
}

type studyScheduled struct {
	At  time.Duration
	Seq int
	Fn  func(time.Duration)
}

type studyRun struct {
	t       *testing.T
	ctx     context.Context
	arm     string
	sc      studyScenario
	wl      studyWorkload
	seed    int64
	horizon time.Duration
	rng     *rand.Rand

	env          *test.Environment
	client       client.Client
	counter      *studyCountingClient
	clk          *clocktesting.FakeClock
	vclk         *studyVirtualClock
	cp           *fake.CloudProvider
	cluster      *state.Cluster
	clusterCost  *cost.ClusterCost
	prov         *provisioning.Provisioner
	recorder     *test.EventRecorder
	queue        *disruption.Queue
	nodeCtrl     *informer.NodeController
	ncCtrl       *informer.NodeClaimController
	emptiness    *disruption.Emptiness
	drift        *disruption.Drift
	multi        *disruption.MultiNodeConsolidation
	single       *disruption.SingleNodeConsolidation
	fixedMethods []disruption.Method
	prices       map[string]*cloudprovider.InstanceType

	now              time.Duration
	nodes            map[string]*studyNode // by node name
	byPID            map[string]*studyNode
	nodeSeq          int
	pods             map[types.NamespacedName]*studyPod
	svcPods          map[int][]*studyPod
	pdbs             map[int]*studyPDB
	outageAt         map[int]time.Duration
	sched            []studyScheduled
	schedSeq         int
	evaluated        map[string]bool // persistent cursor arm
	res              studyResult
	driftTimes       []float64
	lastBlockedCount int
	lastBlockedAt    time.Duration
	snapSpecs        map[string]studySnapNode
	removed          map[int]int
	jitter           time.Duration
	unifiedState     time.Time
}

// ---------------------------------------------------------------- entry point

func TestCandidateOrderingStudy(t *testing.T) {
	cfgPath := os.Getenv("ORDERING_STUDY_CONFIG")
	if cfgPath == "" {
		t.Skip("ORDERING_STUDY_CONFIG not set")
	}
	RegisterTestingT(t)
	raw, err := os.ReadFile(cfgPath)
	Expect(err).ToNot(HaveOccurred())
	var cfg studyConfig
	Expect(json.Unmarshal(raw, &cfg)).To(Succeed())
	if cfg.HorizonMin == 0 {
		cfg.HorizonMin = 120
	}
	sc, ok := studyScenarios()[cfg.Scenario]
	Expect(ok).To(BeTrue(), "unknown scenario %s", cfg.Scenario)
	Expect(os.MkdirAll(cfg.OutDir, 0o755)).To(Succeed())
	wl := studyMakeWorkload(sc, cfg.Seed)
	if cfg.SnapshotOut != "" {
		snap := studyBuildSnapshot(t, cfg, sc, wl)
		data, err := json.Marshal(snap)
		Expect(err).ToNot(HaveOccurred())
		Expect(os.WriteFile(cfg.SnapshotOut+".tmp", data, 0o644)).To(Succeed())
		Expect(os.Rename(cfg.SnapshotOut+".tmp", cfg.SnapshotOut)).To(Succeed())
		t.Logf("snapshot %s seed %d: %d nodes, %d pods", cfg.Scenario, cfg.Seed, len(snap.Nodes), len(snap.Pods))
		return
	}
	var snap *studySnapshot
	if cfg.SnapshotIn != "" {
		raw, err := os.ReadFile(cfg.SnapshotIn)
		Expect(err).ToNot(HaveOccurred())
		snap = &studySnapshot{}
		Expect(json.Unmarshal(raw, snap)).To(Succeed())
		Expect(snap.Scenario == cfg.Scenario && snap.Seed == cfg.Seed).To(BeTrue(), "snapshot does not match config")
	}
	for _, arm := range cfg.Arms {
		out := filepath.Join(cfg.OutDir, fmt.Sprintf("%s_s%03d_r%d_%s.json", cfg.Scenario, cfg.Seed, cfg.Replicate, arm))
		if _, err := os.Stat(out); err == nil {
			t.Logf("skip %s (exists)", out)
			continue
		}
		start := time.Now()
		res := studyRunArm(t, cfg, sc, wl, arm, snap)
		res.WallSeconds = time.Since(start).Seconds()
		data, err := json.MarshalIndent(res, "", " ")
		Expect(err).ToNot(HaveOccurred())
		Expect(os.WriteFile(out+".tmp", data, 0o644)).To(Succeed())
		Expect(os.Rename(out+".tmp", out)).To(Succeed())
		t.Logf("%s %s seed %d: cost $%.2f, evicted %d (critical %d), commands %d, sims %d, wall %.0fs",
			cfg.Scenario, arm, cfg.Seed, res.CostDollars, res.PodsEvicted, res.CriticalPodsEvicted, res.Commands, res.Simulations, res.WallSeconds)
	}
}

func studyCopyWorkload(wl studyWorkload) studyWorkload {
	wl.Services = lo.Map(wl.Services, func(s *studyService, _ int) *studyService { c := *s; return &c })
	return wl
}

// studyBuildSnapshot places the seed's services with Karpenter's own provisioner, in random waves, and records
// what it launched and where every pod landed.
func studyBuildSnapshot(t *testing.T, cfg studyConfig, sc studyScenario, wl studyWorkload) *studySnapshot {
	r := &studyRun{t: t, arm: "SNAPSHOT", sc: sc, wl: studyCopyWorkload(wl), seed: cfg.Seed, rng: rand.New(rand.NewSource(cfg.Seed))}
	r.setup()
	defer func() { Expect(r.env.Stop()).To(Succeed()) }()
	r.snapSpecs = map[string]studySnapNode{}
	r.populateCommon()
	ctx := injection.WithControllerName(r.ctx, "study-provisioner")
	for _, wave := range r.wl.Waves {
		for _, si := range wave {
			for range r.wl.Services[si].Desired {
				r.createPod(ctx, si, "", -time.Hour)
			}
		}
		r.provision(ctx, -time.Hour)
	}
	snap := &studySnapshot{Scenario: cfg.Scenario, Seed: cfg.Seed}
	index := map[string]int{}
	for i, n := range r.liveNodes() {
		index[n.Name] = i
		snap.Nodes = append(snap.Nodes, r.snapSpecs[n.Name])
	}
	pods := lo.Values(r.pods)
	sort.Slice(pods, func(i, j int) bool {
		if pods[i].Svc != pods[j].Svc {
			return pods[i].Svc < pods[j].Svc
		}
		return pods[i].Index < pods[j].Index
	})
	for _, p := range pods {
		Expect(p.Pending).To(BeFalse(), "pod %s left pending by the provisioner", p.Key.Name)
		snap.Pods = append(snap.Pods, studySnapPod{Svc: p.Svc, Node: index[p.Node]})
	}
	return snap
}

func studyRunArm(t *testing.T, cfg studyConfig, sc studyScenario, wl studyWorkload, arm string, snap *studySnapshot) studyResult {
	// Events change services' desired replicas, so every arm replays its own copy of the workload.
	wl = studyCopyWorkload(wl)
	r := &studyRun{t: t, arm: arm, sc: sc, wl: wl, seed: cfg.Seed, horizon: time.Duration(cfg.HorizonMin * float64(time.Minute)),
		rng: rand.New(rand.NewSource(cfg.Seed*104729 + 3))}
	r.res = studyResult{Scenario: cfg.Scenario, Seed: cfg.Seed, Arm: arm, Replicate: cfg.Replicate, HorizonMin: cfg.HorizonMin, CommandsByReason: map[string]int{}}
	r.setup()
	defer func() { Expect(r.env.Stop()).To(Succeed()) }()
	r.populate(snap)
	r.loop()
	r.finish()
	return r.res
}

// ---------------------------------------------------------------- environment

func (r *studyRun) setup() {
	r.env = test.NewEnvironment(test.WithCRDs(coreapis.CRDs...), test.WithCRDs(v1alpha1.CRDs...))
	ctx := options.ToContext(context.Background(), test.Options())
	ctx = kwokoptions.ToContext(ctx, &kwokoptions.Options{})
	r.ctx = ctx
	r.clk = r.env.Clock
	r.clk.SetTime(studyEpoch)
	r.counter = &studyCountingClient{Client: r.env.Client}
	r.client = r.env.Client
	r.vclk = &studyVirtualClock{FakeClock: r.clk, counter: r.counter, sSim: r.sc.SSim}

	its, err := kwokcloudprovider.ConstructInstanceTypes(ctx)
	Expect(err).ToNot(HaveOccurred())
	r.cp = fake.NewCloudProvider()
	r.cp.Reset()
	r.cp.InstanceTypes = its
	r.prices = lo.SliceToMap(its, func(it *cloudprovider.InstanceType) (string, *cloudprovider.InstanceType) { return it.Name, it })

	r.clusterCost = cost.NewClusterCost(ctx, r.cp, r.client)
	pricing := informer.NewPricingController(r.client, r.cp, r.clusterCost)
	expectations.ExpectSingletonReconciled(ctx, pricing)
	r.recorder = test.NewEventRecorder()
	r.cluster = state.NewCluster(r.clk, r.client, r.cp)
	r.nodeCtrl = informer.NewNodeController(r.client, r.cluster)
	r.ncCtrl = informer.NewNodeClaimController(r.client, r.cp, r.cluster, r.clusterCost)
	store := prediction.NewStore(r.client)
	metaScheme := runtime.NewScheme()
	Expect(metav1.AddMetaToScheme(metaScheme)).To(Succeed())
	Expect(store.RegisterTargetKinds(ctx, &informertest.FakeInformers{Scheme: metaScheme}, prediction.SupportedTargets.UnsortedList()...)).To(Succeed())
	// The provisioner gets the counting client: SimulateScheduling reaches the API for pending pods through it.
	r.prov = provisioning.NewProvisioner(r.counter, r.recorder, r.cp, r.cluster, r.clk, deviceallocation.NewController(r.client),
		virtualpods.NewVirtualPodCache(r.client), store)
	backoff := nodepoolbackoff.NewState(r.clk)
	r.queue = disruption.NewQueue(r.client, r.recorder, r.cluster, r.clk, r.prov, backoff)

	c := disruption.MakeConsolidation(r.vclk, r.cluster, r.client, r.prov, r.cp, r.recorder, r.queue)
	r.emptiness = disruption.NewEmptiness(c, disruption.WithValidator(NopValidator{}))
	r.drift = disruption.NewDrift(r.client, r.cluster, r.prov, r.recorder, r.vclk, backoff)
	r.multi = disruption.NewMultiNodeConsolidation(c, disruption.WithValidator(NopValidator{}))
	r.single = disruption.NewSingleNodeConsolidation(c, disruption.WithValidator(NopValidator{}))
	r.fixedMethods = []disruption.Method{r.emptiness, r.drift, r.multi, r.single}

	r.nodes = map[string]*studyNode{}
	r.byPID = map[string]*studyNode{}
	r.pods = map[types.NamespacedName]*studyPod{}
	r.svcPods = map[int][]*studyPod{}
	r.pdbs = map[int]*studyPDB{}
	r.outageAt = map[int]time.Duration{}
	r.evaluated = map[string]bool{}
	r.removed = map[int]int{}
}

func (r *studyRun) nodePool(name string) *v1.NodePool {
	reqs := []v1.NodeSelectorRequirementWithMinValues{
		{Key: corev1.LabelOSStable, Operator: corev1.NodeSelectorOpIn, Values: []string{string(corev1.Linux)}},
		{Key: corev1.LabelArchStable, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.ArchitectureAmd64, v1.ArchitectureArm64}},
		{Key: "karpenter.kwok.sh/instance-cpu", Operator: corev1.NodeSelectorOpIn, Values: []string{"2", "4", "8", "16"}},
	}
	var taints []corev1.Taint
	switch {
	case name == "batch":
		reqs = append(reqs, v1.NodeSelectorRequirementWithMinValues{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeSpot, v1.CapacityTypeOnDemand}})
		taints = []corev1.Taint{{Key: "workload", Value: "batch", Effect: corev1.TaintEffectNoSchedule}}
	case name == "mem":
		reqs = append(reqs,
			v1.NodeSelectorRequirementWithMinValues{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeOnDemand}},
			v1.NodeSelectorRequirementWithMinValues{Key: "karpenter.kwok.sh/instance-family", Operator: corev1.NodeSelectorOpIn, Values: []string{"m"}})
	default:
		reqs = append(reqs, v1.NodeSelectorRequirementWithMinValues{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{v1.CapacityTypeOnDemand}})
	}
	np := test.NodePool(v1.NodePool{
		ObjectMeta: metav1.ObjectMeta{Name: name},
		Spec: v1.NodePoolSpec{
			Disruption: v1.Disruption{
				ConsolidateAfter:    v1.MustParseNillableDuration("0s"),
				ConsolidationPolicy: v1.ConsolidationPolicyWhenEmptyOrUnderutilized,
				Budgets:             []v1.Budget{{Nodes: "10%"}},
			},
			Limits: v1.Limits(corev1.ResourceList{corev1.ResourceCPU: resource.MustParse("100000")}),
		},
	})
	np.Spec.Template.Spec.Requirements = reqs
	np.Spec.Template.Spec.Taints = taints
	np.Spec.Template.Spec.ExpireAfter = v1.MustParseNillableDuration("Never")
	return np
}

// ---------------------------------------------------------------- initial cluster

// populate restores the seed's snapshot when there is one, otherwise places the services with the provisioner.
func (r *studyRun) populate(snap *studySnapshot) {
	ctx := injection.WithControllerName(r.ctx, "study-provisioner")
	r.populateCommon()
	if snap != nil {
		names := make([]string, len(snap.Nodes))
		for i, n := range snap.Nodes {
			nc := &v1.NodeClaim{
				ObjectMeta: metav1.ObjectMeta{Name: fmt.Sprintf("snap-%04d", i), Labels: n.Labels, Annotations: n.Annotations},
				Spec:       *n.Spec.DeepCopy(),
			}
			nc.Spec.Requirements = append(nc.Spec.Requirements,
				v1.NodeSelectorRequirementWithMinValues{Key: corev1.LabelInstanceTypeStable, Operator: corev1.NodeSelectorOpIn, Values: []string{n.InstanceType}},
				v1.NodeSelectorRequirementWithMinValues{Key: corev1.LabelTopologyZone, Operator: corev1.NodeSelectorOpIn, Values: []string{n.Zone}},
				v1.NodeSelectorRequirementWithMinValues{Key: v1.CapacityTypeLabelKey, Operator: corev1.NodeSelectorOpIn, Values: []string{n.CapacityType}})
			Expect(r.client.Create(ctx, nc)).To(Succeed())
			names[i] = r.launch(ctx, nc.Name, -time.Hour)
			sn := r.nodes[names[i]]
			Expect(r.prices[n.InstanceType] != nil && sn != nil).To(BeTrue())
		}
		for _, p := range snap.Pods {
			r.createPod(ctx, p.Svc, names[p.Node], -time.Hour)
		}
	} else {
		for _, wave := range r.wl.Waves {
			for _, si := range wave {
				for range r.wl.Services[si].Desired {
					r.createPod(ctx, si, "", -time.Hour)
				}
			}
			r.provision(ctx, -time.Hour)
		}
	}
	r.refreshPDBs(ctx)
	r.res.StartNodes = len(r.liveNodes())
	r.res.StartPods = len(r.pods)
	r.res.StartCostPerHour = r.costRate()
	for _, e := range r.wl.Events {
		e := e
		r.schedule(e.At, func(at time.Duration) { r.applyEvent(e, at) })
	}
}

// populateCommon creates the priority classes, NodePools and PDBs.
func (r *studyRun) populateCommon() {
	ctx := injection.WithControllerName(r.ctx, "study-provisioner")
	for _, pc := range []struct {
		name  string
		value int32
	}{{"critical", studyCriticalPriority}, {"standard", 0}, {"batch", studyBatchPriority}} {
		Expect(r.client.Create(ctx, &schedulingv1.PriorityClass{ObjectMeta: metav1.ObjectMeta{Name: pc.name}, Value: pc.value})).To(Succeed())
	}
	for _, p := range r.wl.Pools {
		expectations.ExpectApplied(ctx, r.client, r.nodePool(p))
	}
	for i, s := range r.wl.Services {
		if s.PDB == "" {
			continue
		}
		opts := test.PDBOptions{ObjectMeta: metav1.ObjectMeta{Name: s.Name + "-pdb", Namespace: studyNamespace}, Labels: map[string]string{studyAppLabel: s.Name}}
		if s.PDB == "maxu1" {
			opts.MaxUnavailable = lo.ToPtr(intstr.FromInt32(1))
		} else {
			opts.MinAvailable = lo.ToPtr(intstr.FromString("75%"))
		}
		expectations.ExpectApplied(ctx, r.client, test.PodDisruptionBudget(opts))
		r.pdbs[i] = &studyPDB{Name: s.Name + "-pdb", Svc: i, Kind: s.PDB}
	}
}

// ---------------------------------------------------------------- pods, nodes, PDBs

func (r *studyRun) podObject(si int, name, nodeName string) *corev1.Pod {
	s := r.wl.Services[si]
	p := &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name: name, Namespace: studyNamespace,
			Labels: map[string]string{studyAppLabel: s.Name},
			OwnerReferences: []metav1.OwnerReference{{APIVersion: "apps/v1", Kind: "ReplicaSet", Name: s.Name + "-rs",
				UID: types.UID(fmt.Sprintf("00000000-0000-0000-0000-%012d", si)), Controller: lo.ToPtr(true), BlockOwnerDeletion: lo.ToPtr(true)}},
		},
		Spec: corev1.PodSpec{
			PriorityClassName: s.Kind,
			NodeSelector:      map[string]string{v1.NodePoolLabelKey: s.Pool},
			NodeName:          nodeName,
			Containers: []corev1.Container{{Name: "app", Image: "registry.k8s.io/pause:3.10", Resources: corev1.ResourceRequirements{Requests: corev1.ResourceList{
				corev1.ResourceCPU:    *resource.NewMilliQuantity(s.CPUMilli, resource.DecimalSI),
				corev1.ResourceMemory: *resource.NewQuantity(s.MemMi*1024*1024, resource.BinarySI),
			}}}},
		},
	}
	if s.CostAnno {
		p.Annotations = map[string]string{v1.DisruptionCostAnnotationKey: fmt.Sprintf("%d", 1<<27)}
	}
	if s.Pool == "batch" {
		p.Spec.Tolerations = []corev1.Toleration{{Key: "workload", Operator: corev1.TolerationOpEqual, Value: "batch", Effect: corev1.TaintEffectNoSchedule}}
	}
	if s.Spread {
		p.Spec.TopologySpreadConstraints = []corev1.TopologySpreadConstraint{{MaxSkew: 1, TopologyKey: corev1.LabelTopologyZone,
			WhenUnsatisfiable: corev1.DoNotSchedule, LabelSelector: &metav1.LabelSelector{MatchLabels: map[string]string{studyAppLabel: s.Name}}}}
	}
	return p
}

// createPod creates a replica of service si. With nodeName empty it is pending (unschedulable) and the next
// provision call places it.
func (r *studyRun) createPod(ctx context.Context, si int, nodeName string, readyAt time.Duration) *studyPod {
	idx := len(r.svcPods[si]) + r.removed[si]
	name := fmt.Sprintf("%s-%05d", r.wl.Services[si].Name, idx)
	p := r.podObject(si, name, nodeName)
	Expect(r.client.Create(ctx, p)).To(Succeed())
	if nodeName == "" {
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Reason: corev1.PodReasonUnschedulable, Status: corev1.ConditionFalse}}
		p.Status.Phase = corev1.PodPending
	} else {
		p.Status.Phase = corev1.PodRunning
		p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}}
	}
	Expect(r.client.Status().Update(ctx, p)).To(Succeed())
	if nodeName != "" {
		Expect(r.cluster.UpdatePod(ctx, p)).To(Succeed())
	}
	sp := &studyPod{Key: client.ObjectKeyFromObject(p), Svc: si, Node: nodeName, ReadyAt: readyAt, Index: idx, Pending: nodeName == ""}
	r.pods[sp.Key] = sp
	r.svcPods[si] = append(r.svcPods[si], sp)
	if readyAt > r.now {
		r.schedule(readyAt, func(at time.Duration) { r.onReady(at) })
	}
	return sp
}

func (r *studyRun) deletePod(ctx context.Context, sp *studyPod) {
	p := &corev1.Pod{ObjectMeta: metav1.ObjectMeta{Name: sp.Key.Name, Namespace: sp.Key.Namespace}}
	if err := r.client.Delete(ctx, p, client.GracePeriodSeconds(0)); err != nil && !errors.IsNotFound(err) {
		Expect(err).ToNot(HaveOccurred())
	}
	r.cluster.DeletePod(sp.Key)
	delete(r.pods, sp.Key)
	r.svcPods[sp.Svc] = lo.Without(r.svcPods[sp.Svc], sp)
	r.removed[sp.Svc]++
}

// provision runs Karpenter's provisioner for pending pods, launches what it asks for and binds pods where it
// placed them.
func (r *studyRun) provision(ctx context.Context, at time.Duration) {
	pending := lo.Filter(lo.Values(r.pods), func(p *studyPod, _ int) bool { return p.Pending })
	if len(pending) == 0 {
		return
	}
	results, err := r.prov.Schedule(ctx)
	Expect(err).ToNot(HaveOccurred())
	for _, nc := range results.NewNodeClaims {
		name, err := r.prov.Create(ctx, nc, provisioning.WithReason(metrics.ProvisionedReason))
		Expect(err).ToNot(HaveOccurred())
		node := r.launch(ctx, name, at)
		for _, p := range nc.Pods {
			r.bind(ctx, client.ObjectKeyFromObject(p), node)
		}
	}
	for _, en := range results.ExistingNodes {
		for _, p := range en.Pods {
			r.bind(ctx, client.ObjectKeyFromObject(p), en.Name())
		}
	}
}

func (r *studyRun) bind(ctx context.Context, key types.NamespacedName, nodeName string) {
	sp, ok := r.pods[key]
	if !ok || !sp.Pending {
		return
	}
	Expect(r.client.Create(ctx, &corev1.Binding{ObjectMeta: metav1.ObjectMeta{Name: key.Name, Namespace: key.Namespace}, Target: corev1.ObjectReference{Name: nodeName}})).To(Succeed())
	p := &corev1.Pod{}
	Expect(r.client.Get(ctx, key, p)).To(Succeed())
	p.Status.Phase = corev1.PodRunning
	p.Status.Conditions = []corev1.PodCondition{{Type: corev1.PodScheduled, Status: corev1.ConditionTrue}}
	Expect(r.client.Status().Update(ctx, p)).To(Succeed())
	Expect(r.cluster.UpdatePod(ctx, p)).To(Succeed())
	sp.Pending = false
	sp.Node = nodeName
}

// launch deploys a NodeClaim the provisioner created through the fake cloud provider, joins its Node and marks
// both initialized and consolidatable.
func (r *studyRun) launch(ctx context.Context, nodeClaimName string, at time.Duration) string {
	nc := &v1.NodeClaim{}
	Expect(r.client.Get(ctx, types.NamespacedName{Name: nodeClaimName}, nc)).To(Succeed())
	launchSpec := studySnapNode{Labels: nc.Labels, Annotations: nc.Annotations, Spec: *nc.Spec.DeepCopy()}
	nc, node := expectations.ExpectNodeClaimDeployedAndStateUpdated(ctx, r.client, r.cluster, r.cp, nc)
	Expect(node).ToNot(BeNil())
	expectations.ExpectMakeNodesAndNodeClaimsInitializedAndStateUpdated(ctx, r.client, r.clk, r.nodeCtrl, r.ncCtrl, []*corev1.Node{node}, []*v1.NodeClaim{nc})
	nc = expectations.ExpectExists(ctx, r.client, nc)
	nc.StatusConditions(status.WithClock(r.clk)).SetTrue(v1.ConditionTypeConsolidatable)
	expectations.ExpectApplied(ctx, r.client, nc)
	expectations.ExpectReconcileSucceeded(ctx, r.ncCtrl, client.ObjectKeyFromObject(nc))
	it := r.prices[node.Labels[corev1.LabelInstanceTypeStable]]
	price := 0.0
	if it != nil {
		price, _ = it.OfferingPrice(node.Labels[corev1.LabelTopologyZone], node.Labels[v1.CapacityTypeLabelKey])
	}
	r.nodeSeq++
	sn := &studyNode{Name: node.Name, ProviderID: node.Spec.ProviderID, NodeClaim: nc.Name, Pool: node.Labels[v1.NodePoolLabelKey],
		Seq: r.nodeSeq, Price: price, Start: max(at, 0), End: -1, DriftedAt: -1}
	r.nodes[sn.Name] = sn
	r.byPID[sn.ProviderID] = sn
	if r.snapSpecs != nil {
		launchSpec.InstanceType = node.Labels[corev1.LabelInstanceTypeStable]
		launchSpec.Zone = node.Labels[corev1.LabelTopologyZone]
		launchSpec.CapacityType = node.Labels[v1.CapacityTypeLabelKey]
		r.snapSpecs[sn.Name] = launchSpec
	}
	return node.Name
}

func (r *studyRun) removeNode(ctx context.Context, sn *studyNode, at time.Duration) {
	for _, obj := range []client.Object{&v1.NodeClaim{ObjectMeta: metav1.ObjectMeta{Name: sn.NodeClaim}}, &corev1.Node{ObjectMeta: metav1.ObjectMeta{Name: sn.Name}}} {
		if err := r.client.Get(ctx, client.ObjectKeyFromObject(obj), obj); err == nil {
			obj.SetFinalizers(nil)
			if err := r.client.Update(ctx, obj); err != nil && !errors.IsNotFound(err) {
				Expect(err).ToNot(HaveOccurred())
			}
			if err := r.client.Delete(ctx, obj, client.GracePeriodSeconds(0)); err != nil && !errors.IsNotFound(err) {
				Expect(err).ToNot(HaveOccurred())
			}
		}
	}
	r.cluster.DeleteNode(sn.Name)
	r.cluster.DeleteNodeClaim(sn.NodeClaim)
	sn.End = at
	if sn.DriftedAt >= 0 {
		r.res.DriftedResolved++
		r.driftTimes = append(r.driftTimes, (at - sn.DriftedAt).Minutes())
	}
	delete(r.byPID, sn.ProviderID)
	r.markChanged()
}

func (r *studyRun) liveNodes() []*studyNode {
	nodes := lo.Filter(lo.Values(r.nodes), func(n *studyNode, _ int) bool { return n.End < 0 })
	sort.Slice(nodes, func(i, j int) bool { return nodes[i].Seq < nodes[j].Seq })
	return nodes
}

func (r *studyRun) costRate() float64 {
	return lo.SumBy(r.liveNodes(), func(n *studyNode) float64 { return n.Price })
}

func (r *studyRun) readyCount(si int, at time.Duration) int {
	return lo.CountBy(r.svcPods[si], func(p *studyPod) bool { return !p.Pending && p.ReadyAt <= at })
}

// refreshPDBs plays the kube-controller-manager disruption controller: disruptionsAllowed = ready matching pods -
// desired healthy.
func (r *studyRun) refreshPDBs(ctx context.Context) {
	for si, d := range r.pdbs {
		expected := r.wl.Services[si].Desired
		desired := expected - 1
		if d.Kind == "min75" {
			desired = int(math.Ceil(0.75 * float64(expected)))
		}
		allowed := int32(max(0, r.readyCount(si, r.now)-desired))
		if allowed == d.Allowed && r.now > 0 {
			continue
		}
		pdb := &policyv1.PodDisruptionBudget{}
		Expect(r.client.Get(ctx, types.NamespacedName{Name: d.Name, Namespace: studyNamespace}, pdb)).To(Succeed())
		pdb.Status.DisruptionsAllowed = allowed
		pdb.Status.CurrentHealthy = int32(r.readyCount(si, r.now))
		pdb.Status.DesiredHealthy = int32(desired)
		pdb.Status.ExpectedPods = int32(expected)
		pdb.Status.ObservedGeneration = pdb.Generation
		Expect(r.client.Status().Update(ctx, pdb)).To(Succeed())
		d.Allowed = allowed
	}
	r.trackBlocked()
}

// trackBlocked integrates the number of nodes hosting a pod whose PDB allows no disruption.
func (r *studyRun) trackBlocked() {
	if r.now > r.lastBlockedAt && r.now > 0 {
		r.res.PDBBlockedNodeMinutes += float64(r.lastBlockedCount) * (min(r.now, r.horizon) - max(r.lastBlockedAt, 0)).Minutes()
	}
	blocked := map[string]bool{}
	for _, p := range r.pods {
		if d, ok := r.pdbs[p.Svc]; ok && d.Allowed == 0 && !p.Pending {
			blocked[p.Node] = true
		}
	}
	r.lastBlockedCount = len(blocked)
	r.lastBlockedAt = max(r.now, 0)
}

// syncClock sets the fake clock to the run's virtual now. jitter keeps it strictly increasing across state changes
// made at the same virtual instant, so Karpenter's consolidation-state timestamps always move forward.
func (r *studyRun) syncClock() {
	r.clk.SetTime(studyEpoch.Add(r.now + r.jitter))
}

func (r *studyRun) markChanged() {
	r.jitter += time.Millisecond
	r.syncClock()
	r.cluster.MarkUnconsolidated()
}

// ---------------------------------------------------------------- events

func (r *studyRun) schedule(at time.Duration, fn func(time.Duration)) {
	r.schedSeq++
	r.sched = append(r.sched, studyScheduled{At: at, Seq: r.schedSeq, Fn: fn})
	sort.SliceStable(r.sched, func(i, j int) bool {
		if r.sched[i].At != r.sched[j].At {
			return r.sched[i].At < r.sched[j].At
		}
		return r.sched[i].Seq < r.sched[j].Seq
	})
}

func (r *studyRun) nextEventAt() (time.Duration, bool) {
	if len(r.sched) == 0 {
		return 0, false
	}
	return r.sched[0].At, true
}

func (r *studyRun) runDue(upTo time.Duration) bool {
	ran := false
	for len(r.sched) > 0 && r.sched[0].At <= upTo {
		ev := r.sched[0]
		r.sched = r.sched[1:]
		r.now = max(r.now, ev.At)
		r.syncClock()
		ev.Fn(ev.At)
		ran = true
	}
	if ran {
		r.refreshPDBs(r.ctx)
	}
	return ran
}

func (r *studyRun) onReady(at time.Duration) {
	for si, start := range r.outageAt {
		if r.readyCount(si, at) > 0 {
			r.res.CriticalOutageSeconds += (at - start).Seconds()
			delete(r.outageAt, si)
		}
	}
}

func (r *studyRun) applyEvent(e studyEvent, at time.Duration) {
	ctx := injection.WithControllerName(r.ctx, "study-provisioner")
	switch e.Kind {
	case "finish":
		for _, sp := range append([]*studyPod{}, r.svcPods[e.Svc]...) {
			r.deletePod(ctx, sp)
		}
		r.wl.Services[e.Svc].Desired = 0
	case "scale":
		s := r.wl.Services[e.Svc]
		target := s.Desired + e.Delta
		if e.Frac > 0 {
			target = max(1, int(math.Round(float64(s.Desired)*e.Frac)))
		}
		r.scaleTo(ctx, e.Svc, target, at)
	case "drift":
		for _, sn := range r.liveNodes() {
			if sn.Pool == "batch" || sn.Deleting {
				continue
			}
			nc := &v1.NodeClaim{}
			if err := r.client.Get(ctx, types.NamespacedName{Name: sn.NodeClaim}, nc); err != nil {
				continue
			}
			nc.StatusConditions(status.WithClock(r.clk)).SetTrue(v1.ConditionTypeDrifted)
			expectations.ExpectApplied(ctx, r.client, nc)
			expectations.ExpectReconcileSucceeded(ctx, r.ncCtrl, client.ObjectKeyFromObject(nc))
			sn.DriftedAt = at
			r.res.DriftedTotal++
		}
	}
	r.markChanged()
}

func (r *studyRun) scaleTo(ctx context.Context, si, target int, at time.Duration) {
	s := r.wl.Services[si]
	cur := len(r.svcPods[si])
	s.Desired = target
	if target > cur {
		for range target - cur {
			r.createPod(ctx, si, "", at+studyReadyDelay)
		}
		r.provision(ctx, at)
		return
	}
	// ReplicaSet scale-down ranking, simplified: unready first, then pods on nodes with more replicas of this
	// service, then newest.
	perNode := lo.CountValuesBy(r.svcPods[si], func(p *studyPod) string { return p.Node })
	victims := append([]*studyPod{}, r.svcPods[si]...)
	sort.SliceStable(victims, func(i, j int) bool {
		a, b := victims[i], victims[j]
		ar, br := a.ReadyAt <= at, b.ReadyAt <= at
		if ar != br {
			return !ar
		}
		if perNode[a.Node] != perNode[b.Node] {
			return perNode[a.Node] > perNode[b.Node]
		}
		return a.Index > b.Index
	})
	for _, sp := range victims[:cur-target] {
		r.deletePod(ctx, sp)
	}
}

// ---------------------------------------------------------------- disruption loop

func (r *studyRun) loop() {
	nextLoop := time.Duration(0)
	for r.res.Loops < studyMaxLoops {
		r.runDue(r.now)
		if r.now >= r.horizon {
			break
		}
		if r.arm == "NONE" {
			next, ok := r.nextEventAt()
			if !ok || next >= r.horizon {
				r.now = r.horizon
				break
			}
			r.now = next
			continue
		}
		if r.now < nextLoop {
			next, ok := r.nextEventAt()
			if ok && next < nextLoop {
				r.now = next
				continue
			}
			r.now = nextLoop
			continue
		}
		r.syncClock()
		r.recorder.Reset()
		r.res.Loops++
		r.vclk.mark = r.counter.pendingLists.Load()
		wall := time.Now()
		ctx := injection.WithControllerName(r.ctx, "disruption")
		cmds, reason, timedOut := r.decide(ctx)
		sims := r.vclk.simsSinceMark()
		r.res.SimWallSeconds += time.Since(wall).Seconds()
		r.res.Simulations += sims
		r.vclk.mark = r.counter.pendingLists.Load()
		if timedOut {
			r.res.Timeouts++
		}
		elapsed := time.Duration(sims) * r.sc.SSim
		if len(cmds) > 0 && reason != string(v1.DisruptionReasonDrifted) {
			elapsed += studyValidationDelay
		}
		r.now += elapsed
		r.syncClock()
		if r.now >= r.horizon {
			break
		}
		if len(cmds) > 0 {
			for _, cmd := range cmds {
				r.execute(ctx, cmd, reason, sims)
			}
			nextLoop = r.now
			continue
		}
		if timedOut {
			nextLoop = r.now + studyPollPeriod
			continue
		}
		// The real controller polls every 10 s. With nothing changed it finds nothing until the next event or until
		// the cluster's consolidation state resets itself, five minutes after the last change.
		next, ok := r.nextEventAt()
		reset := r.cluster.ConsolidationState().Add(5*time.Minute).Sub(studyEpoch) - r.jitter
		if !ok || reset < next {
			next = reset
		}
		nextLoop = time.Duration(math.Ceil(float64(max(next, r.now+time.Millisecond))/float64(studyPollPeriod))) * studyPollPeriod
	}
}

func (r *studyRun) sortedCandidates(cands []*disruption.Candidate) []*disruption.Candidate {
	sort.SliceStable(cands, func(i, j int) bool { return r.seqOf(cands[i]) < r.seqOf(cands[j]) })
	return cands
}

func (r *studyRun) seqOf(c *disruption.Candidate) int {
	if sn, ok := r.byPID[c.ProviderID()]; ok {
		return sn.Seq
	}
	return math.MaxInt32
}

func (r *studyRun) decide(ctx context.Context) ([]disruption.Command, string, bool) {
	switch r.arm {
	case "FIXED":
		return r.decideFixed(ctx)
	case "U-SR-M1":
		if cmds, reason, _ := r.runMethod(ctx, r.multi); len(cmds) > 0 {
			return cmds, reason, false
		}
		return r.decideUnified(ctx, false)
	default:
		return r.decideUnified(ctx, true)
	}
}

// runMethod mirrors Controller.disrupt without the orchestration queue.
func (r *studyRun) runMethod(ctx context.Context, m disruption.Method) ([]disruption.Command, string, bool) {
	cands, totals, err := disruption.GetCandidatesWithTotals(ctx, r.cluster, r.client, r.recorder, r.vclk, r.cp, m.ShouldDisrupt, m.Class(), r.queue, r.clusterCost)
	Expect(err).ToNot(HaveOccurred())
	if len(cands) == 0 {
		return nil, "", false
	}
	cands = r.sortedCandidates(cands)
	if setter, ok := m.(disruption.NodePoolTotalsSetter); ok {
		setter.SetNodePoolTotals(totals)
	}
	budgets, err := disruption.BuildDisruptionBudgetMapping(ctx, r.cluster, r.vclk, r.client, r.cp, r.recorder, m.Reason())
	Expect(err).ToNot(HaveOccurred())
	start := r.vclk.Now()
	cmds, err := m.ComputeCommands(ctx, budgets, cands...)
	Expect(err).ToNot(HaveOccurred())
	timedOut := m == disruption.Method(r.single) && r.vclk.Now().Sub(start) > disruption.SingleNodeConsolidationTimeoutDuration
	cmds = lo.Filter(cmds, func(c disruption.Command, _ int) bool { return c.Decision() != disruption.NoOpDecision })
	return cmds, string(m.Reason()), timedOut
}

func (r *studyRun) decideFixed(ctx context.Context) ([]disruption.Command, string, bool) {
	timedOut := false
	for _, m := range r.fixedMethods {
		cmds, reason, to := r.runMethod(ctx, m)
		timedOut = timedOut || to
		if len(cmds) > 0 {
			if m == disruption.Method(r.emptiness) {
				reason = string(v1.DisruptionReasonEmpty)
			}
			return cmds, reason, timedOut
		}
	}
	return nil, "", timedOut
}

type studyCand struct {
	c        *disruption.Candidate
	seq      int
	reason   v1.DisruptionReason
	price    float64
	rdc      float64
	pods     int
	critical int
	tight    int
	util     float64
	entropy  float64
	conc     float64
	driftAge time.Duration
	class    int
	score    float64
	score2   float64
}

func (r *studyRun) describe(ctx context.Context, c *disruption.Candidate) *studyCand {
	sc := &studyCand{c: c, seq: r.seqOf(c), price: c.Price, rdc: c.RescheduleDisruptionCost}
	switch {
	case r.emptiness.ShouldDisrupt(ctx, c):
		sc.reason = v1.DisruptionReasonEmpty
	case r.drift.ShouldDisrupt(ctx, c):
		sc.reason = v1.DisruptionReasonDrifted
	default:
		sc.reason = v1.DisruptionReasonUnderutilized
	}
	if sn, ok := r.byPID[c.ProviderID()]; ok && sn.DriftedAt >= 0 {
		sc.driftAge = r.now - sn.DriftedAt
		if sc.reason != v1.DisruptionReasonEmpty {
			sc.reason = v1.DisruptionReasonDrifted
		}
	}
	pods := c.ReschedulablePodsForStudy()
	sc.pods = len(pods)
	owners := map[string]int{}
	for _, p := range pods {
		if p.Spec.Priority != nil && *p.Spec.Priority >= studyCriticalPriority {
			sc.critical++
		}
		owners[p.Labels[studyAppLabel]]++
		if si, ok := r.svcIndex(p.Labels[studyAppLabel]); ok {
			if d, ok := r.pdbs[si]; ok && d.Allowed <= 1 {
				sc.tight++
			}
		}
	}
	for svc, n := range owners {
		frac := float64(n) / float64(len(pods))
		sc.entropy -= frac * math.Log(frac)
		if si, ok := r.svcIndex(svc); ok {
			sc.conc += float64(n) / float64(max(1, r.wl.Services[si].Desired))
		}
	}
	alloc, req := c.Allocatable(), c.PodRequests()
	for name, a := range alloc {
		if a.IsZero() || name == corev1.ResourcePods || name == corev1.ResourceEphemeralStorage {
			continue
		}
		q := req[name]
		sc.util = math.Max(sc.util, q.AsApproximateFloat64()/a.AsApproximateFloat64())
	}
	sc.util = math.Min(sc.util, 1)
	return sc
}

func (r *studyRun) svcIndex(name string) (int, bool) {
	var i int
	if _, err := fmt.Sscanf(name, "svc-%d", &i); err != nil || i < 0 || i >= len(r.wl.Services) {
		return 0, false
	}
	return i, true
}

func studyMinMax(xs []float64) func(float64) float64 {
	lo_, hi := math.Inf(1), math.Inf(-1)
	for _, x := range xs {
		lo_, hi = math.Min(lo_, x), math.Max(hi, x)
	}
	return func(x float64) float64 {
		if hi-lo_ < 1e-12 {
			return 0
		}
		return (x - lo_) / (hi - lo_)
	}
}

// order scores and sorts unified-list candidates for the arm. Lower class first, then higher score, then higher
// score2, then older node.
func (r *studyRun) order(cands []*studyCand) []*studyCand {
	arm := r.arm
	driftClass := !strings.HasPrefix(arm, "D-BONUS")
	lambda := lo.SumBy(cands, func(c *studyCand) float64 { return c.price }) / math.Max(1, float64(lo.SumBy(cands, func(c *studyCand) int { return max(1, c.pods) })))
	nCrit := studyMinMax(lo.Map(cands, func(c *studyCand, _ int) float64 { return float64(c.critical) }))
	nEnt := studyMinMax(lo.Map(cands, func(c *studyCand, _ int) float64 { return c.entropy }))
	nConc := studyMinMax(lo.Map(cands, func(c *studyCand, _ int) float64 { return c.conc }))
	for _, c := range cands {
		sr := c.price / c.rdc
		switch arm {
		case "U-SAVINGS":
			c.score = c.price * (1 - c.util)
		case "U-STRAW":
			c.score = c.price*(1-c.util) - lambda*(c.rdc-1)
		case "U-BLAST":
			c.score = c.price / (c.rdc + 10*float64(c.critical) + 5*float64(c.tight))
		case "U-TIER":
			c.score = sr
			if c.critical > 0 {
				c.class = 1
			}
		case "U-FEWPODS":
			c.score, c.score2 = -float64(c.pods), c.price
		case "U-RANDOM":
			c.score = r.rng.Float64()
		case "C-GOV":
			c.score = c.price / (1 + 10*nCrit(float64(c.critical)))
		case "C-ENT":
			c.score = c.price / (1 + 10*nEnt(c.entropy))
		case "C-CONC":
			c.score = c.price / (1 + 10*nConc(c.conc))
		case "C-CEI":
			c.score = c.price / (1 + 10*(nCrit(float64(c.critical))+nEnt(c.entropy)+nConc(c.conc))/3)
		default: // U-SR, U-SR-M1, U-SR-CURSOR, D-BONUS, D-DEADLINE
			c.score = sr
		}
	}
	if arm == "D-BONUS" {
		med := 0.0
		if scores := lo.Map(cands, func(c *studyCand, _ int) float64 { return c.score }); len(scores) > 0 {
			sort.Float64s(scores)
			med = scores[len(scores)/2]
		}
		for _, c := range cands {
			if c.reason == v1.DisruptionReasonDrifted {
				c.score += med * c.driftAge.Minutes() / 10
			}
		}
	}
	for _, c := range cands {
		inDriftClass := c.reason == v1.DisruptionReasonDrifted && driftClass
		if arm == "D-DEADLINE" {
			inDriftClass = c.reason == v1.DisruptionReasonDrifted && c.driftAge >= 15*time.Minute
		}
		if inDriftClass {
			c.class = -1
		}
	}
	sort.SliceStable(cands, func(i, j int) bool {
		a, b := cands[i], cands[j]
		if a.class != b.class {
			return a.class < b.class
		}
		if a.score != b.score {
			return a.score > b.score
		}
		if a.score2 != b.score2 {
			return a.score2 > b.score2
		}
		return a.seq < b.seq
	})
	if arm == "U-SR-CURSOR" {
		fresh := lo.Filter(cands, func(c *studyCand, _ int) bool { return !r.evaluated[c.c.Name()] })
		if len(fresh) == 0 {
			r.evaluated = map[string]bool{}
			fresh = cands
		}
		seen := lo.Filter(cands, func(c *studyCand, _ int) bool { return r.evaluated[c.c.Name()] })
		cands = append(fresh, seen...)
	}
	return cands
}

func (r *studyRun) decideUnified(ctx context.Context, multiAfter bool) ([]disruption.Command, string, bool) {
	filter := func(ctx context.Context, c *disruption.Candidate) bool {
		return r.emptiness.ShouldDisrupt(ctx, c) || r.drift.ShouldDisrupt(ctx, c) || r.single.ShouldDisrupt(ctx, c)
	}
	cands, totals, err := disruption.GetCandidatesWithTotals(ctx, r.cluster, r.client, r.recorder, r.vclk, r.cp, filter, disruption.GracefulDisruptionClass, r.queue, r.clusterCost)
	Expect(err).ToNot(HaveOccurred())
	cands = r.sortedCandidates(cands)
	r.single.SetNodePoolTotals(totals)
	budgets := map[v1.DisruptionReason]map[string]int{}
	for _, reason := range []v1.DisruptionReason{v1.DisruptionReasonEmpty, v1.DisruptionReasonDrifted, v1.DisruptionReasonUnderutilized} {
		b, err := disruption.BuildDisruptionBudgetMapping(ctx, r.cluster, r.vclk, r.client, r.cp, r.recorder, reason)
		Expect(err).ToNot(HaveOccurred())
		budgets[reason] = b
	}
	// Same cache as the consolidation methods: skip the walk when nothing changed since a walk that found nothing.
	if r.unifiedState.Equal(r.cluster.ConsolidationState()) {
		cands = nil
	}
	described := r.order(lo.Map(cands, func(c *disruption.Candidate, _ int) *studyCand { return r.describe(ctx, c) }))
	timeout := r.vclk.Now().Add(disruption.SingleNodeConsolidationTimeoutDuration)
	timedOut, constrained := false, false
	for _, sc := range described {
		if r.vclk.Now().After(timeout) {
			timedOut = true
			break
		}
		if r.arm == "U-SR-CURSOR" {
			r.evaluated[sc.c.Name()] = true
		}
		if budgets[sc.reason][sc.c.NodePool.Name] == 0 {
			constrained = true
			continue
		}
		switch sc.reason {
		case v1.DisruptionReasonEmpty:
			return []disruption.Command{{Candidates: []*disruption.Candidate{sc.c}}}, string(sc.reason), false
		case v1.DisruptionReasonDrifted:
			b := map[string]int{sc.c.NodePool.Name: budgets[sc.reason][sc.c.NodePool.Name]}
			cmds, err := r.drift.ComputeCommands(ctx, b, sc.c)
			Expect(err).ToNot(HaveOccurred())
			cmds = lo.Filter(cmds, func(c disruption.Command, _ int) bool { return c.Decision() != disruption.NoOpDecision })
			if len(cmds) > 0 {
				return cmds, string(sc.reason), false
			}
		default:
			cmd, err := r.single.ConsolidationGateForStudy(ctx, sc.c)
			Expect(err).ToNot(HaveOccurred())
			if cmd.Decision() != disruption.NoOpDecision {
				return []disruption.Command{cmd}, string(sc.reason), false
			}
		}
	}
	if !timedOut && !constrained && len(cands) > 0 {
		r.unifiedState = r.cluster.ConsolidationState()
	}
	if multiAfter && !timedOut {
		if cmds, reason, _ := r.runMethod(ctx, r.multi); len(cmds) > 0 {
			return cmds, reason, false
		}
	}
	return nil, "", timedOut
}

// ---------------------------------------------------------------- execution

func (r *studyRun) execute(ctx context.Context, cmd disruption.Command, reason string, sims int64) {
	ctx = injection.WithControllerName(ctx, "study-provisioner")
	at := r.now
	replNodes := make([]string, len(cmd.Results.NewNodeClaims))
	if len(cmd.Replacements) > 0 {
		for i, nc := range cmd.Results.NewNodeClaims {
			name, err := r.prov.Create(ctx, nc, provisioning.WithReason(strings.ToLower(reason)))
			Expect(err).ToNot(HaveOccurred())
			replNodes[i] = r.launch(ctx, name, at)
		}
	}
	dest := map[types.NamespacedName]string{}
	for _, en := range cmd.Results.ExistingNodes {
		for _, p := range en.Pods {
			dest[client.ObjectKeyFromObject(p)] = en.Name()
		}
	}
	for i, nc := range cmd.Results.NewNodeClaims {
		if replNodes[i] == "" {
			continue
		}
		for _, p := range nc.Pods {
			dest[client.ObjectKeyFromObject(p)] = replNodes[i]
		}
	}
	r.cluster.MarkForDeletion(lo.Map(cmd.Candidates, func(c *disruption.Candidate, _ int) string { return c.ProviderID() })...)

	// PDB pacing: a drain that must evict more pods of one PDB than it currently allows takes extra rounds.
	perPDB := map[int]int{}
	evicted, critical := 0, 0
	affected := map[int]bool{}
	var moves []*studyPod
	for _, c := range cmd.Candidates {
		for _, p := range c.ReschedulablePodsForStudy() {
			sp, ok := r.pods[client.ObjectKeyFromObject(p)]
			if !ok {
				continue
			}
			moves = append(moves, sp)
			if _, ok := r.pdbs[sp.Svc]; ok {
				perPDB[sp.Svc]++
			}
		}
	}
	rounds := 1
	for si, n := range perPDB {
		rounds = max(rounds, int(math.Ceil(float64(n)/math.Max(1, float64(r.pdbs[si].Allowed)))))
	}
	stalled := rounds > 1
	unplaced := false
	for _, sp := range moves {
		key := sp.Key
		si := sp.Svc
		target, ok := dest[key]
		evictedBefore := sp.Evicted
		r.deletePod(ctx, sp)
		var np *studyPod
		if ok {
			np = r.createPod(ctx, si, target, at+studyReadyDelay)
		} else {
			np = r.createPod(ctx, si, "", at+studyReadyDelay)
			r.res.PodsUnplaced++
			unplaced = true
		}
		np.Evicted = evictedBefore + 1
		if np.Evicted == 2 {
			r.res.PodsEvicted2Plus++
		}
		evicted++
		if r.wl.Services[si].Kind == "critical" {
			critical++
			affected[si] = true
		}
	}
	if unplaced {
		r.provision(ctx, at)
	}
	for si := range affected {
		if _, in := r.outageAt[si]; !in && r.readyCount(si, at) == 0 {
			r.outageAt[si] = at
			r.res.CriticalOutages++
		}
	}
	drain := studyDeleteDrain
	if len(cmd.Replacements) > 0 {
		drain = studyReplaceDrain
	}
	drain += time.Duration(rounds-1) * studyPDBRound
	for _, c := range cmd.Candidates {
		sn := r.byPID[c.ProviderID()]
		if sn == nil {
			continue
		}
		sn.Deleting = true
		r.schedule(at+drain, func(done time.Duration) { r.removeNode(r.ctx, sn, done) })
	}
	r.res.Commands++
	r.res.CommandsByReason[strings.ToLower(reason)]++
	r.res.NodesDisrupted += len(cmd.Candidates)
	r.res.PodsEvicted += evicted
	r.res.CriticalPodsEvicted += critical
	if stalled {
		r.res.PDBStalledCommands++
	}
	r.res.CommandLog = append(r.res.CommandLog, studyCommandLog{TMin: at.Minutes(), Reason: strings.ToLower(reason), Decision: string(cmd.Decision()),
		Nodes: len(cmd.Candidates), Pods: evicted, Critical: critical, Savings: cmd.EstimatedSavings(), Sims: sims, PDBStalled: stalled})
	r.refreshPDBs(ctx)
	r.markChanged()
}

// ---------------------------------------------------------------- results

func (r *studyRun) finish() {
	r.now = r.horizon
	r.trackBlocked()
	h := r.horizon
	cost := 0.0
	for _, n := range r.nodes {
		end := n.End
		if end < 0 || end > h {
			end = h
		}
		start := max(n.Start, 0)
		if end > start {
			cost += n.Price * (end - start).Hours()
		}
	}
	r.res.CostDollars = cost
	for m := 0.0; m <= h.Minutes(); m++ {
		at := time.Duration(m * float64(time.Minute))
		rate := 0.0
		for _, n := range r.nodes {
			if max(n.Start, 0) <= at && (n.End < 0 || n.End > at) {
				rate += n.Price
			}
		}
		r.res.CostCurve = append(r.res.CostCurve, [2]float64{m, rate})
	}
	live := r.liveNodes()
	r.res.EndNodes = len(live)
	r.res.FinalCostPerHour = r.costRate()
	for _, n := range live {
		if n.DriftedAt >= 0 {
			r.res.DriftedLeft++
			r.res.DriftLeftMaxAgeMin = math.Max(r.res.DriftLeftMaxAgeMin, (h - n.DriftedAt).Minutes())
		}
	}
	if len(r.driftTimes) > 0 {
		r.res.DriftResolveMeanMin = lo.Sum(r.driftTimes) / float64(len(r.driftTimes))
		r.res.DriftResolveMaxMin = lo.Max(r.driftTimes)
	}
	for si, start := range r.outageAt {
		r.res.CriticalOutageSeconds += (h - start).Seconds()
		delete(r.outageAt, si)
	}
	r.res.PendingAtEnd = lo.CountBy(lo.Values(r.pods), func(p *studyPod) bool { return p.Pending })
}
