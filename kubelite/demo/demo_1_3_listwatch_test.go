package demo

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"kubelite/pkg/listwatch"
	"kubelite/pkg/storage"
)

// SLIDE: Consistent Core Interface & Cluster Primitives   (demo 1.3)
//
// The ListWatch Pattern:
//   1. LIST: Fetches a baseline snapshot of existing resources to populate
//      local caches, emitting 'Added' events for all pre-existing objects.
//   2. WATCH: Opens a streaming connection from the latest resource version,
//      delivering real-time 'Added', 'Modified', and 'Deleted' events without polling.
//
// Controllers and Schedulers in Kubernetes rely on ListWatch to maintain
// up-to-date in-memory views of cluster state with zero database polling load.
//
// TRY IT: add more names to baselinePods. Every one of them existed before the client
// connected, and LIST emits an Added for each — which is the guarantee that a controller
// starting late never misses state it was not around to see.
var baselinePods = []string{"initial-pod"}

func TestDemo_1_3_ListWatch(t *testing.T) {
	printBanner("DEMO 1.3: CONSISTENT CORE LISTWATCH (LIST -> ADDED -> MODIFIED -> DELETED)")

	ctx := context.Background()
	cli := storage.StartEmbeddedEtcdForTest(t)

	prefix := "/registry/pods/"
	dynamicPodKey := prefix + "dynamic-pod"

	// 1. Pre-populate etcd before ListWatch starts (baseline cluster state)
	fmt.Println("\n--- 1. Pre-populating etcd (baseline state before client connects) ---")
	baselineKeys := make(map[string]bool, len(baselinePods))
	for _, name := range baselinePods {
		key := prefix + name
		_, err := cli.Put(ctx, key, fmt.Sprintf(`{"name":%q,"status":"Running"}`, name))
		require.NoError(t, err)
		baselineKeys[key] = true
		fmt.Printf("  Wrote baseline key '%s' to etcd\n", key)
	}

	// 2. Start ListWatch — demonstrating the initial LIST baseline catch-up
	fmt.Printf("\n--- 2. Starting ListWatch on prefix '%s' ---\n", prefix)
	lw := newListWatcher(t, cli, prefix)
	ch, stopWatch, err := lw.ListAndWatch(ctx)
	require.NoError(t, err)
	t.Cleanup(stopWatch)

	// List phase catches every pre-existing pod, in whatever order etcd returns them.
	// What is guaranteed is the set, not the sequence.
	for range baselinePods {
		e := expectAddedIn(t, ch, baselineKeys)
		fmt.Printf("  📋 [LIST Catch-Up]  Event=%s Key=%s\n", e.Type, e.Key)
	}
	require.Empty(t, baselineKeys, "LIST must emit an Added for every pre-existing key")

	// 3. Watch phase: Real-time ADDED event
	fmt.Println("\n--- 3. [WATCH] New Pod Created in etcd ---")
	_, err = cli.Put(ctx, dynamicPodKey, `{"name":"dynamic-pod","status":"Pending"}`)
	require.NoError(t, err)
	event := expectEvent(t, ch, listwatch.Added, dynamicPodKey)
	fmt.Printf("  📥 [WATCH Stream]    Event=%s Key=%s Value=%s\n",
		event.Type, event.Key, string(event.Value))

	// 4. Watch phase: Real-time MODIFIED event
	fmt.Println("\n--- 4. [WATCH] Pod Status Updated in etcd ---")
	_, err = cli.Put(ctx, dynamicPodKey, `{"name":"dynamic-pod","status":"Running"}`)
	require.NoError(t, err)
	event = expectEvent(t, ch, listwatch.Modified, dynamicPodKey)
	fmt.Printf("  📥 [WATCH Stream]    Event=%s Key=%s Value=%s\n",
		event.Type, event.Key, string(event.Value))

	// 5. Watch phase: Real-time DELETED event
	fmt.Println("\n--- 5. [WATCH] Pod Deleted from etcd ---")
	_, err = cli.Delete(ctx, dynamicPodKey)
	require.NoError(t, err)
	event = expectEvent(t, ch, listwatch.Deleted, dynamicPodKey)
	fmt.Printf("  📥 [WATCH Stream]    Event=%s Key=%s\n", event.Type, event.Key)

	printNarration("what succeeded", `ListWatch provides reliable, streaming cluster synchronization:
1. LIST guarantees the client never misses historical state that existed before startup.
2. WATCH streams real-time mutations directly from the Consistent Core (etcd) revision log.
Controllers and schedulers build Informer in-memory caches on top of this primitive,
eliminating polling overhead across the cluster.`)
}

// expectAddedIn consumes one Added event whose key is still expected, and removes it from the
// set. LIST does not promise an order across objects, so asserting a sequence would make this
// demo flaky for a reason that has nothing to do with what it teaches.
func expectAddedIn(t *testing.T, ch <-chan listwatch.Event, expected map[string]bool) listwatch.Event {
	t.Helper()
	select {
	case e := <-ch:
		require.Equal(t, listwatch.Added, e.Type, "expected an Added during LIST catch-up")
		require.True(t, expected[e.Key], "unexpected key during LIST catch-up: %s", e.Key)
		delete(expected, e.Key)
		return e
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for a LIST Added event; still expecting %v", expected)
		return listwatch.Event{}
	}
}
