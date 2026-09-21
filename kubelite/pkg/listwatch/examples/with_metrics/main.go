package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	clientv3 "go.etcd.io/etcd/client/v3"
	"kubelite/pkg/listwatch"
	"kubelite/pkg/retry"
	"kubelite/pkg/storage"
)

func simulateDataChanges(ctx context.Context, client *clientv3.Client, prefix string) {
	log.Printf("Updating key values")
	for i := 0; ; i++ {

		// Add a new key
		key := fmt.Sprintf("%sapp%d", prefix, i)
		log.Printf("Adding key: %s", key)
		_, err := client.Put(ctx, key, fmt.Sprintf("value%d", i))
		if err != nil {
			log.Printf("Failed to put key: %v", err)
			return
		}

		time.Sleep(2 * time.Second)

		// Modify the key
		log.Printf("Modifying key: %s", key)
		_, err = client.Put(ctx, key, fmt.Sprintf("value%d-updated", i))
		if err != nil {
			log.Printf("Failed to modify key: %v", err)
			return
		}

		time.Sleep(2 * time.Second)

		// Delete the key
		log.Printf("Deleting key: %s", key)
		_, err = client.Delete(ctx, key)
		if err != nil {
			log.Printf("Failed to delete key: %v", err)
			return
		}

		time.Sleep(2 * time.Second)
	}
}

func main() {
	// Start Prometheus metrics server
	go func() {
		http.Handle("/metrics", promhttp.Handler())
		log.Fatal(http.ListenAndServe(":2112", nil))
	}()

	// Start embedded etcd
	etcdServer, port, err := storage.StartEmbeddedEtcd()
	if err != nil {
		log.Fatalf("Failed to start embedded etcd: %v", err)
	}
	defer storage.StopEmbeddedEtcd(etcdServer)

	// Create etcd client
	endpoint := fmt.Sprintf("http://127.0.0.1:%d", port)
	etcdClient, err := clientv3.New(clientv3.Config{
		Endpoints:   []string{endpoint},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatalf("Failed to create etcd client: %v", err)
	}
	defer etcdClient.Close()

	// Create a ListWatch with custom options
	opts := listwatch.Options{
		DialTimeout: 5 * time.Second,
		RetryOpts: retry.Options{
			InitialDelay: 1 * time.Second,
			MaxDelay:     30 * time.Second,
			Multiplier:   2.0,
		},
	}

	prefix := "/example/"
	lw, err := listwatch.NewListWatch(
		[]string{endpoint},
		prefix,
		opts,
		nil,
	)
	if err != nil {
		log.Fatalf("Failed to create ListWatch: %v", err)
	}

	ctx := context.Background()
	eventCh, stopWatch, err := lw.ListAndWatch(ctx)
	if err != nil {
		log.Fatalf("Failed to start watching: %v", err)
	}
	defer stopWatch()

	// Start goroutine to simulate data changes
	go simulateDataChanges(ctx, etcdClient, prefix)

	fmt.Println("Watching for events. Metrics available at :2112/metrics")
	fmt.Println("Check the following metrics:")
	fmt.Println("- listwatch_events_total{type=\"added|modified|deleted\"}")
	fmt.Println("- listwatch_connection_state")
	fmt.Println("- listwatch_watch_session_duration_seconds")
	fmt.Println("- listwatch_errors_total{type=\"connection_failed|watch_error\"}")

	for event := range eventCh {
		if event.Type == listwatch.Error {
			log.Printf("Error event: %s", string(event.Value))
			continue
		}
		log.Printf("Event: type=%s key=%s value=%s", event.Type, event.Key, string(event.Value))
	}
}
