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
//
//	In Kafka, brokers connect directly to ZooKeeper and create ephemeral znodes.
//	In Kubernetes, worker nodes NEVER talk directly to etcd. All state transitions
//	flow declaratively through the API Server, which acts as the sole gatekeeper,
//	validator, and writer to the Consistent Core.
//
// TRY IT: add "worker-node-2" to nodeNames below. Each kubelet registers itself through the
// API server and appears as its own record under /registry/nodes/ — nothing else in this demo
// changes, because every step below is driven from the list.
var nodeNames = []string{"worker-node-1", "worker-node-2"}

func TestDemo_1_2_NodeRegistration(t *testing.T) {
	printBanner("DEMO 1.2: NODE REGISTRATION WITH ETCD (KUBELET -> API SERVER -> ETCD)")

	ctx := context.Background()
	etcdStorage := startEmbeddedEtcd(t)
	apiServer := startAPIServer(t, etcdStorage)

	etcdKeyFor := func(name string) string { return "/registry/nodes/" + name }

	// 1. Inspect Consistent Core before any Kubelet starts
	fmt.Println("\n--- 1. etcd state before Kubelets start ---")
	for _, name := range nodeNames {
		var beforeNode api.Node
		err := etcdStorage.Get(ctx, etcdKeyFor(name), &beforeNode)
		require.Error(t, err, "Node %s should not exist in etcd yet", name)
		fmt.Printf("  Key '%s' exists: false\n", etcdKeyFor(name))
	}

	// 2 & 3. Each worker boots a Kubelet, which registers via POST /api/v1/nodes.
	//        Nothing coordinates them — registration is self-service, exactly as in Demo 1.1.
	for _, name := range nodeNames {
		fmt.Printf("\n--- 2. Kubelet starts for '%s' ---\n", name)
		k := kubelet.NewKubeletWithClient(name, apiServer.Listener.Addr().String(), nil)

		fmt.Printf("--- 3. Kubelet '%s' sends HTTP POST /api/v1/nodes ---\n", name)
		err := k.RegisterNode()
		require.NoError(t, err)
		fmt.Printf("  ✅ API Server accepted registration (201 Created)\n")
	}

	// 4. Verify every record landed in etcd
	fmt.Printf("\n--- 4. Inspecting Consistent Core (etcd) directly ---\n")
	for _, name := range nodeNames {
		var storedNode api.Node
		err := etcdStorage.Get(ctx, etcdKeyFor(name), &storedNode)
		require.NoError(t, err)
		assert.Equal(t, name, storedNode.Name)
		assert.Equal(t, api.NodeReady, storedNode.Status)

		payload, _ := json.MarshalIndent(storedNode, "  ", "  ")
		fmt.Printf("  etcd key: %s\n", etcdKeyFor(name))
		fmt.Printf("  payload:\n  %s\n", string(payload))
	}

	printNarration("what succeeded", `The Kubelet registered itself with the Consistent Core (etcd) through
the Kubernetes API server. The node status is 'Ready', and its record is
persistently stored under /registry/nodes/<name> for the scheduler and
controllers to discover. Notice that no Kubelet ever touched etcd directly,
and that nothing coordinated them — each registered itself.`)
}
