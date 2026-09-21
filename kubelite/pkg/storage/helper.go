package storage

import (
	"fmt"
	"testing"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
)

// StartEmbeddedEtcdForTest starts an in-process embedded etcd instance for testing,
// connects a client, registers automatic cleanup with t.Cleanup, and returns the client.
func StartEmbeddedEtcdForTest(t *testing.T) *clientv3.Client {
	t.Helper()
	etcdServer, port, err := StartEmbeddedEtcd()
	if err != nil {
		t.Fatalf("Failed to start embedded etcd: %v", err)
	}
	t.Cleanup(func() {
		StopEmbeddedEtcd(etcdServer)
	})

	cli, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{fmt.Sprintf("http://localhost:%d", port)},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("Failed to create etcd client: %v", err)
	}
	t.Cleanup(func() {
		_ = cli.Close()
	})

	return cli
}

// TestWithEmbeddedEtcd takes in testing.T, starts the embedded etcd server,
// handles the cleanup of the server after the test is done and invokes the test function
// with the embedded etcd server instance.
func TestWithEmbeddedEtcd(t *testing.T, test func(t *testing.T, etcdServer *clientv3.Client)) {
	cli := StartEmbeddedEtcdForTest(t)
	test(t, cli)
}
