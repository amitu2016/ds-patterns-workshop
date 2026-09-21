package demo

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"kubelite/pkg/api"
	"kubelite/pkg/controller"
	"kubelite/pkg/registry"
)

// SLIDE: ReplicaSet Controller Detailed Flow   (demo 2.3)
//
// Declarative Reconciliation Loop:
//   The controller enforces the core equation of Kubernetes:
//       diff = desired_replicas - actual_active_pods
//
//   If diff > 0: create pods
//   If diff < 0: terminate pods
//   If diff == 0: no-op (converged)
//
// Level-Triggered (not Edge-Triggered):
//   The controller does not merely react to point-in-time failure events.
//   It continually compares full cluster state against desired specification,
//   enabling self-healing even if individual notifications were dropped.
//
// TRY IT: Change desiredReplicas from 3 to 5. Step 2 provisions five pods, and the
// scale-down in step 6 then has four to terminate rather than two.
func TestDemo_2_3_ReplicaSetController(t *testing.T) {
	printBanner("DEMO 2.3: REPLICASET CONTROLLER RECONCILIATION & SELF-HEALING")

	ctx := context.Background()
	etcdStorage := startEmbeddedEtcd(t)

	replicaSetRegistry := registry.NewReplicaSetRegistry(etcdStorage)
	podRegistry := registry.NewPodRegistry(etcdStorage)
	rsc := controller.NewReplicaSetController(replicaSetRegistry, podRegistry)

	rsName := "frontend-rs"
	desiredReplicas := int32(3)

	// Step 1: Define and store ReplicaSet with 3 desired replicas
	fmt.Printf("\n--- 1. Defining ReplicaSet '%s' (Desired: %d Replicas) ---\n",
		rsName, desiredReplicas)

	rs := &api.ReplicaSet{
		ObjectMeta: api.ObjectMeta{Name: rsName},
		Spec: api.ReplicaSetSpec{
			Replicas: desiredReplicas,
			Template: api.PodTemplateSpec{
				Spec: api.PodSpec{
					Containers: []api.Container{
						{Name: "web-server", Image: "nginx:alpine"},
					},
				},
			},
		},
	}
	err := replicaSetRegistry.Create(ctx, rs)
	require.NoError(t, err)

	// Step 2: First Reconciliation (Initial Provisioning: 0 -> 3 pods)
	fmt.Println("\n--- 2. Running Reconcile() — Initial Provisioning (0 -> 3 pods) ---")
	err = rsc.Reconcile(ctx, rs)
	require.NoError(t, err)

	allPods, err := podRegistry.ListPods(ctx)
	require.NoError(t, err)
	require.Equal(t, int(desiredReplicas), len(allPods), "Expected 3 pods created by ReplicaSet controller")

	fmt.Printf("  ✅ Controller reconciled: Desired=%d, Actual=%d\n", desiredReplicas, len(allPods))
	printPodTable("  Cluster pods in etcd:", allPods)

	// Step 3: Simulate Pod Failure / Deletion
	podToDelete := allPods[0]
	fmt.Printf("\n--- 3. Simulating Pod Failure: Deleting '%s' ---\n", podToDelete.Name)
	err = podRegistry.DeletePod(ctx, podToDelete.Name)
	require.NoError(t, err)

	remainingPods, err := podRegistry.ListPods(ctx)
	require.NoError(t, err)
	require.Equal(t, 2, len(remainingPods))
	fmt.Printf("  ⚠️ Discrepancy Detected! Desired=%d, Actual=%d (diff = +1 missing pod)\n",
		desiredReplicas, len(remainingPods))

	// Step 4: Second Reconciliation (Self-Healing / Recovery Loop)
	fmt.Println("\n--- 4. Running Reconcile() — Self-Healing / Recovery Loop ---")
	err = rsc.Reconcile(ctx, rs)
	require.NoError(t, err)

	healedPods, err := podRegistry.ListPods(ctx)
	require.NoError(t, err)
	assert.Equal(t, int(desiredReplicas), len(healedPods), "Expected controller to restore pod count to 3")

	fmt.Printf("  ✅ Self-healing completed: Desired=%d, Actual=%d\n", desiredReplicas, len(healedPods))
	printPodTable("  Restored cluster pods in etcd:", healedPods)

	// Step 5: Verify ReplicaSet Status in Registry
	updatedRS, err := replicaSetRegistry.Get(ctx, rsName)
	require.NoError(t, err)
	assert.Equal(t, desiredReplicas, updatedRS.Status.Replicas)
	fmt.Printf("\n--- 5. ReplicaSet '%s' status confirmed: %d ready replicas ---\n",
		rsName, updatedRS.Status.Replicas)

	// Step 6: Scale down. The same loop, the same equation, the opposite sign.
	fmt.Println("\n--- 6. Scaling Down: Spec.Replicas 3 -> 1 ---")
	rs.Spec.Replicas = 1
	require.NoError(t, replicaSetRegistry.Update(ctx, rs))
	fmt.Printf("  Desired=%d, Actual=%d (diff = -2 excess pods)\n", 1, len(healedPods))

	require.NoError(t, rsc.Reconcile(ctx, rs))

	scaledPods, err := podRegistry.ListPods(ctx)
	require.NoError(t, err)
	assert.Equal(t, 1, len(scaledPods), "controller must terminate the excess pods, not just report success")
	printPodTable("  Cluster pods after scale-down:", scaledPods)

	scaledRS, err := replicaSetRegistry.Get(ctx, rsName)
	require.NoError(t, err)
	assert.Equal(t, int32(1), scaledRS.Status.Replicas)
	fmt.Printf("  ✅ Status reports %d replica — and etcd really holds %d pod\n",
		scaledRS.Status.Replicas, len(scaledPods))

	printNarration("what succeeded", `The ReplicaSet controller demonstrated level-triggered reconciliation:
1. Desired (3) vs Actual (0)  -> creates 3 pods.
2. A pod is deleted, Actual=2 -> the loop notices on its next pass, not from an event.
3. Desired (3) vs Actual (2)  -> creates 1 replacement. Self-healing, no intervention.
4. Desired (1) vs Actual (3)  -> terminates 2. The same equation with the sign flipped.

Both directions matter. A loop that only ever creates is reacting, not reconciling —
and note the status was written only after the deletions landed, so it never claims
a convergence that did not happen.`)
}
