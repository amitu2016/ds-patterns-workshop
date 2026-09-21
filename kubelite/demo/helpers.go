package demo

import (
	"fmt"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/emicklei/go-restful/v3"
	"github.com/stretchr/testify/require"

	"kubelite/pkg/api"
	apiserver "kubelite/pkg/api/server"
	"kubelite/pkg/listwatch"
	"kubelite/pkg/retry"
	"kubelite/pkg/scheduler"
	"kubelite/pkg/storage"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// startEmbeddedEtcd starts an in-process embedded etcd instance for testing,
// registers automatic cleanup on test completion, and returns the storage client.
func startEmbeddedEtcd(t *testing.T) *storage.EtcdStorage {
	t.Helper()
	cli := storage.StartEmbeddedEtcdForTest(t)
	return storage.NewEtcdStorage(cli)
}

// startAPIServer starts an in-process HTTP REST API server on an ephemeral port,
// registers automatic cleanup on test completion, and returns the httptest.Server.
func startAPIServer(t *testing.T, s storage.Storage) *httptest.Server {
	t.Helper()
	srv := apiserver.NewAPIServer(s)
	container := restful.NewContainer()
	srv.RegisterRoutes(container)

	ts := httptest.NewServer(container)
	t.Cleanup(ts.Close)
	return ts
}

// watchEventTimeout bounds how long a demo waits for a watch event.
//
// Generous on purpose. The happy path returns the moment the event arrives, so a
// large value costs nothing; it only decides how long we wait before declaring
// failure. A tight bound turns a loaded laptop into a failed demo in front of an
// audience, which is the one failure mode not worth optimising against.
const watchEventTimeout = 15 * time.Second

type silentLogger struct{}

func (l *silentLogger) Info(msg string, keysAndValues ...interface{})  {}
func (l *silentLogger) Error(msg string, keysAndValues ...interface{}) {}

// newListWatcher creates a configured ListWatcher pointing at an embedded etcd client.
func newListWatcher(t *testing.T, cli *clientv3.Client, prefix string) *listwatch.ListWatch {
	t.Helper()
	endpoint := cli.Endpoints()[0]
	opts := listwatch.Options{
		DialTimeout: 2 * time.Second,
		RetryOpts: retry.Options{
			InitialDelay: 50 * time.Millisecond,
			MaxDelay:     500 * time.Millisecond,
			Multiplier:   1.5,
		},
		EventChannelBuffer: 50,
	}

	lw, err := listwatch.NewListWatch([]string{endpoint}, prefix, opts, &silentLogger{})
	require.NoError(t, err)
	return lw
}

// expectEvent awaits an event on the ListWatch channel, asserting its type and key.
func expectEvent(t *testing.T, ch <-chan listwatch.Event, expectedType listwatch.EventType, expectedKey string) listwatch.Event {
	t.Helper()
	select {
	case event := <-ch:
		require.Equal(t, expectedType, event.Type, "Event type mismatch on key %s", expectedKey)
		require.Equal(t, expectedKey, event.Key, "Event key mismatch")
		return event
	case <-time.After(watchEventTimeout):
		t.Fatalf("Timeout waiting for %s event on key '%s'", expectedType, expectedKey)
		return listwatch.Event{}
	}
}

// printPodTable prints a formatted table of pods for cluster state walkthroughs.
func printPodTable(title string, pods []*api.Pod) {
	fmt.Println(title)
	for i, pod := range pods {
		node := pod.NodeName
		if node == "" {
			node = "<unassigned>"
		}
		fmt.Printf("    [%d] %-20s | Node: %-15s | Status: %s\n",
			i+1, pod.Name, node, pod.Status)
	}
}

// printBanner formats and prints a clear presentation heading for the demo.
func printBanner(title string) {
	fmt.Println("\n" + strings.Repeat("=", 72))
	fmt.Println(" " + title)
	fmt.Println(strings.Repeat("=", 72))
}

// printNarration prints the educational takeaway of the demo under a caption.
//
// The caption is a parameter because not every demo demonstrates success: a
// `without` demo in a without/with pair shows a failure mode on purpose, and
// labelling that "what succeeded" would be a lie on screen.
func printNarration(caption, text string) {
	fmt.Printf("\n── %s ──\n", caption)
	fmt.Println(strings.TrimSpace(text))
	fmt.Println(strings.Repeat("=", 72))
}

// printScores renders the scoring phase as a table — one row per candidate node.
func printScores(scores []scheduler.NodeScore) {
	for _, s := range scores {
		bar := strings.Repeat("█", s.Score/10)
		fmt.Printf("  %-15s score=%3d %-10s (%s)\n", s.Node.Name, s.Score, bar, s.Why)
	}
}
