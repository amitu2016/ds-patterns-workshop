package scheduler

import (
	"context"
	"fmt"
	"sort"
	"time"

	"kubelite/pkg/api"
	"kubelite/pkg/registry"
)

// maxNodeScore is the ceiling for a priority function's output.
// Mirrors framework.MaxNodeScore in k8s.io/kubernetes/pkg/scheduler.
const maxNodeScore = 100

type Scheduler struct {
	podRegistry    *registry.PodRegistry
	nodeRegistry   *registry.NodeRegistry
	schedulingRate time.Duration
}

func NewScheduler(podRegistry *registry.PodRegistry, nodeRegistry *registry.NodeRegistry, schedulingRate time.Duration) *Scheduler {
	return &Scheduler{
		podRegistry:    podRegistry,
		nodeRegistry:   nodeRegistry,
		schedulingRate: schedulingRate,
	}
}

func (s *Scheduler) Start(ctx context.Context) {
	ticker := time.NewTicker(s.schedulingRate)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			if err := s.schedulePendingPods(ctx); err != nil {
				fmt.Printf("Error scheduling pods: %v\n", err)
			}
		}
	}
}

// NodeScore is one node's rating from the scoring phase.
// Mirrors framework.NodeScore.
type NodeScore struct {
	Node  *api.Node
	Score int
	Why   string
}

func (s *Scheduler) SchedulePendingPods(ctx context.Context) error {
	return s.schedulePendingPods(ctx)
}

// schedulePendingPods runs the scheduling cycle for every pending pod.
//
// Mirrors the three phases of k8s.io/kubernetes/pkg/scheduler's scheduleOne:
//
//	FILTER  (predicates) — eliminate nodes the pod cannot run on
//	SCORE   (priorities) — rank the survivors
//	BIND                 — write the chosen node onto the pod
//
// Real Kubernetes runs each phase as a chain of plugins and scores in parallel.
// We keep one predicate and one priority so the shape stays visible.
func (s *Scheduler) schedulePendingPods(ctx context.Context) error {
	pods, err := s.podRegistry.ListPendingPods(ctx)
	if err != nil {
		return fmt.Errorf("failed to list pending pods: %v", err)
	}

	nodes, err := s.nodeRegistry.ListNodes(ctx)
	if err != nil {
		return fmt.Errorf("failed to list nodes: %v", err)
	}

	assigned, err := s.PodsPerNode(ctx)
	if err != nil {
		return err
	}

	for _, pod := range pods {
		feasible := FindNodesThatFit(pod, nodes)
		if len(feasible) == 0 {
			// Real Kubernetes leaves the pod Pending and records an Unschedulable
			// event; it does not fail the whole cycle. Neither do we.
			continue
		}

		chosen := SelectHost(PrioritizeNodes(feasible, assigned))
		if err := s.Bind(ctx, pod, chosen); err != nil {
			return err
		}
		assigned[chosen.Name]++
	}

	return nil
}

// FindNodesThatFit is the FILTER phase — k8s calls these predicates.
// A node that fails any predicate is not a candidate, however good its score
// would have been.
func FindNodesThatFit(pod *api.Pod, nodes []*api.Node) []*api.Node {
	var feasible []*api.Node
	for _, node := range nodes {
		if nodeIsReady(node) {
			feasible = append(feasible, node)
		}
	}
	return feasible
}

// nodeIsReady is our single predicate. A Kubelet that stops reporting leaves its
// node in NotReady / MemoryPressure / DiskPressure, and the scheduler must not
// place new work there.
//
// Real Kubernetes also runs PodFitsResources, PodFitsHostPorts, node affinity,
// taints and tolerations. Our api.Pod carries no resource requests, so those have
// nothing to check against — see MAPPING.md.
func nodeIsReady(node *api.Node) bool {
	return node.Status == api.NodeReady
}

// PrioritizeNodes is the SCORE phase — k8s calls these priorities.
//
// Our one priority is LeastPods, the analogue of k8s LeastAllocated: spread work
// onto the emptiest node. 100 means "holds no pods", 0 means "holds as many as
// the busiest node".
func PrioritizeNodes(feasible []*api.Node, podsPerNode map[string]int) []NodeScore {
	busiest := 0
	for _, node := range feasible {
		if podsPerNode[node.Name] > busiest {
			busiest = podsPerNode[node.Name]
		}
	}

	// Scored against busiest+1 rather than busiest, so equally-loaded nodes tie at a
	// meaningful value instead of all collapsing to zero: empty scores 100, and three
	// nodes holding one pod each all score 50.
	headroom := busiest + 1

	scores := make([]NodeScore, 0, len(feasible))
	for _, node := range feasible {
		running := podsPerNode[node.Name]
		score := maxNodeScore * (headroom - running) / headroom
		scores = append(scores, NodeScore{
			Node:  node,
			Score: score,
			Why:   fmt.Sprintf("%d pod(s) already assigned", running),
		})
	}
	return scores
}

// SelectHost picks the highest score, breaking ties by name.
//
// Divergence: real Kubernetes picks uniformly at random among tied nodes
// (reservoir sampling) to avoid hot-spotting. We break ties deterministically so
// a demo produces the same placement every run.
func SelectHost(scores []NodeScore) *api.Node {
	if len(scores) == 0 {
		return nil
	}
	sort.SliceStable(scores, func(i, j int) bool {
		if scores[i].Score != scores[j].Score {
			return scores[i].Score > scores[j].Score
		}
		return scores[i].Node.Name < scores[j].Node.Name
	})
	return scores[0].Node
}

// Bind is the BIND phase: the decision becomes durable in the Consistent Core.
// Until this write lands, nothing has been scheduled.
func (s *Scheduler) Bind(ctx context.Context, pod *api.Pod, node *api.Node) error {
	pod.NodeName = node.Name
	pod.Status = api.PodScheduled
	if err := s.podRegistry.UpdatePod(ctx, pod); err != nil {
		return fmt.Errorf("failed to bind pod %s to node %s: %v", pod.Name, node.Name, err)
	}
	return nil
}

// PodsPerNode counts what each node already holds — the input to scoring.
func (s *Scheduler) PodsPerNode(ctx context.Context) (map[string]int, error) {
	allPods, err := s.podRegistry.ListPods(ctx)
	if err != nil {
		return nil, fmt.Errorf("failed to list pods: %v", err)
	}
	counts := map[string]int{}
	for _, pod := range allPods {
		if pod.NodeName != "" {
			counts[pod.NodeName]++
		}
	}
	return counts, nil
}
