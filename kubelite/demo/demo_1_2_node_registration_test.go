package demo

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"kubelite/pkg/api"
	"kubelite/pkg/kubelet"
)

// SLIDE: Assignment 2: Node registration with etcd   (demo 1.2)
// SLIDE: Consistent Core Interface & Cluster Primitives
//
// Contrast with Kafka (Demo 1.1):
//   In Kafka, brokers connect directly to ZooKeeper and create ephemeral znodes.
//   In Kubernetes, worker nodes NEVER talk directly to etcd. All state transitions
//   flow declaratively through the API Server, which acts as the sole gatekeeper,
//   validator, and writer to the Consistent Core.
//
// TRY IT: Register a second node "worker-node-2" — both appear as independent
// persistent records under /registry/nodes/ in etcd.
func TestDemo_1_2_NodeRegistration(t *testing.T) {
	printBanner("DEMO 1.2: NODE REGISTRATION WITH ETCD (KUBELET -> API SERVER -> ETCD)")

	ctx := context.Background()
	etcdStorage := startEmbeddedEtcd(t)
	apiServer := startAPIServer(t, etcdStorage)

	nodeName := "worker-node-1"
	etcdKey := "/registry/nodes/" + nodeName

	// 1. Inspect Consistent Core before Kubelet starts
	fmt.Println("\n--- 1. etcd state before Kubelet starts ---")
	var beforeNode api.Node
	err := etcdStorage.Get(ctx, etcdKey, &beforeNode)
	require.Error(t, err, "Node should not exist in etcd yet")
	fmt.Printf("  Key '%s' exists: false\n", etcdKey)

	// 2. Worker node boots up and runs Kubelet
	fmt.Printf("\n--- 2. Kubelet starts for '%s' ---\n", nodeName)
	k := kubelet.NewKubeletWithClient(nodeName, apiServer.Listener.Addr().String(), nil)

	// 3. Kubelet registers with API Server via POST /api/v1/nodes
	fmt.Println("\n--- 3. Kubelet sends HTTP POST /api/v1/nodes ---")
	err = k.RegisterNode()
	require.NoError(t, err)
	fmt.Printf("  ✅ API Server accepted registration (201 Created)\n")

	// 4. Verify record in etcd
	fmt.Printf("\n--- 4. Inspecting Consistent Core (etcd) directly ---\n")
	var storedNode api.Node
	err = etcdStorage.Get(ctx, etcdKey, &storedNode)
	require.NoError(t, err)
	assert.Equal(t, nodeName, storedNode.Name)
	assert.Equal(t, api.NodeReady, storedNode.Status)

	payload, _ := json.MarshalIndent(storedNode, "  ", "  ")
	fmt.Printf("  etcd key: %s\n", etcdKey)
	fmt.Printf("  payload:\n  %s\n", string(payload))

	printNarration("what succeeded", `The Kubelet registered itself with the Consistent Core (etcd) through
the Kubernetes API server. The node status is 'Ready', and its record is
persistently stored under /registry/nodes/worker-node-1 for the scheduler
and controllers to discover. Notice that Kubelet never touched etcd directly.`)
}
