package demo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"kubelite/pkg/api"
	"kubelite/pkg/registry"
	"kubelite/pkg/scheduler"
)

// SLIDE: Scheduling Algorithm Step-by-Step   (demo 2.4)
//
// Two-phase placement, exactly as k8s.io/kubernetes/pkg/scheduler does it:
//
//	FILTER (predicates) — eliminate nodes the pod cannot run on. A node that
//	                      fails any predicate is out, however good its score.
//	SCORE  (priorities) — rank the survivors. Ours is LeastPods, the analogue
//	                      of k8s LeastAllocated: prefer the emptiest node.
//	BIND                — write the chosen node onto the pod in etcd. Nothing
//	                      is scheduled until that write lands.
//
// TRY IT: flip worker-node-4 to api.NodeReady in schedulerNodes below and re-run — it survives
// FILTER and, being empty, immediately wins SCORE. Add a fifth node while you are there. The
// assertions are derived from this table rather than naming nodes, so the demo follows it.
var schedulerNodes = []struct {
	name   string
	status api.NodeStatus
}{
	{"worker-node-1", api.NodeReady},
	{"worker-node-2", api.NodeReady},
	{"worker-node-3", api.NodeReady},
	{"worker-node-4", api.NodeNotReady}, // Kubelet stopped reporting
}
func TestDemo_2_4_Scheduler(t *testing.T) {
	printBanner("DEMO 2.4: SCHEDULING ALGORITHM (FILTER -> SCORE -> BIND)")

	ctx := context.Background()
	etcdStorage := startEmbeddedEtcd(t)

	podRegistry := registry.NewPodRegistry(etcdStorage)
	nodeRegistry := registry.NewNodeRegistry(etcdStorage)
	sched := scheduler.NewScheduler(podRegistry, nodeRegistry, 1*time.Second)

	// 1. Four nodes — one of them unhealthy, so FILTER has something to do.
	fmt.Println("\n--- 1. Registering worker nodes ---")
	for _, n := range schedulerNodes {
		require.NoError(t, nodeRegistry.CreateNode(ctx,
			&api.Node{ObjectMeta: api.ObjectMeta{Name: n.name}, Status: n.status}))
		fmt.Printf("  %-15s Status=%s\n", n.name, n.status)
	}

	// 2. Three pods with nowhere to run yet.
	fmt.Println("\n--- 2. Submitting unscheduled (Pending) pods ---")
	for _, name := range []string{"web-frontend-1", "web-frontend-2", "api-service-1"} {
		require.NoError(t, podRegistry.CreatePod(ctx, &api.Pod{
			ObjectMeta: api.ObjectMeta{Name: name},
			Spec:       api.PodSpec{Containers: []api.Container{{Name: "main", Image: "nginx:latest"}}},
			Status:     api.PodPending,
		}))
	}
	pending, err := podRegistry.ListPendingPods(ctx)
	require.NoError(t, err)
	printPodTable("  Pending:", pending)

	allNodes, err := nodeRegistry.ListNodes(ctx)
	require.NoError(t, err)

	// 3. FILTER — run the phase on its own so the elimination is visible.
	fmt.Println("\n--- 3. FILTER phase (predicates) ---")
	feasible := scheduler.FindNodesThatFit(pending[0], allNodes)
	for _, node := range allNodes {
		verdict := "❌ eliminated (not Ready)"
		if containsNode(feasible, node.Name) {
			verdict = "✅ feasible"
		}
		fmt.Printf("  %-15s %s\n", node.Name, verdict)
	}
	ready := 0
	for _, n := range schedulerNodes {
		if n.status == api.NodeReady {
			ready++
		}
	}
	require.Len(t, feasible, ready, "FILTER keeps the Ready nodes and eliminates the rest")
	for _, n := range schedulerNodes {
		if n.status == api.NodeNotReady {
			assert.False(t, containsNode(feasible, n.name),
				"a NotReady node must not survive FILTER: %s", n.name)
		}
	}

	// 4. SCORE — every feasible node is empty, so all tie at the ceiling.
	fmt.Println("\n--- 4. SCORE phase (priorities: LeastPods) ---")
	counts, err := sched.PodsPerNode(ctx)
	require.NoError(t, err)
	printScores(scheduler.PrioritizeNodes(feasible, counts))

	// 5. BIND — and re-score, to show placement changing the ranking.
	fmt.Println("\n--- 5. BIND phase — scheduling all pending pods ---")
	require.NoError(t, sched.SchedulePendingPods(ctx))

	scheduled, err := podRegistry.ListPods(ctx)
	require.NoError(t, err)
	printPodTable("  Bound in etcd:", scheduled)
	for _, pod := range scheduled {
		assert.Equal(t, api.PodScheduled, pod.Status)
		for _, n := range schedulerNodes {
			if n.status == api.NodeNotReady {
				assert.NotEqual(t, n.name, pod.NodeName, "nothing may land on a NotReady node")
			}
		}
	}

	fmt.Println("\n--- 6. One node is now loaded — watch the ranking move ---")
	require.NoError(t, podRegistry.CreatePod(ctx, &api.Pod{
		ObjectMeta: api.ObjectMeta{Name: "data-worker-1"},
		Spec:       api.PodSpec{Containers: []api.Container{{Name: "worker", Image: "python:3.11"}}},
		Status:     api.PodPending,
	}))
	require.NoError(t, sched.SchedulePendingPods(ctx))

	placed, err := podRegistry.GetPod(ctx, "data-worker-1")
	require.NoError(t, err)
	fmt.Printf("  data-worker-1 bound to %s — that node now holds 2 pods\n", placed.NodeName)

	counts, err = sched.PodsPerNode(ctx)
	require.NoError(t, err)
	rescored := scheduler.PrioritizeNodes(feasible, counts)
	printScores(rescored)

	best := scheduler.SelectHost(rescored)
	assert.NotEqual(t, placed.NodeName, best.Name,
		"the node that just took a pod should no longer be the top choice")
	fmt.Printf("  next pod would go to %s\n", best.Name)

	printNarration("what succeeded", `FILTER eliminated worker-node-4 before scoring ever ran — a failed predicate
is disqualifying, not a low score, however empty the node.

SCORE then ranked the survivors by how much work they already hold, and BIND
made the decision durable in etcd. Nothing is scheduled until that write lands.

Because scoring reads live state, the ranking moves as pods land: the node that
just accepted a pod drops below its idler neighbours, so the next pod goes
elsewhere. Load spreads without anyone computing a global plan.`)
}

func containsNode(nodes []*api.Node, name string) bool {
	for _, n := range nodes {
		if n.Name == name {
			return true
		}
	}
	return false
}
