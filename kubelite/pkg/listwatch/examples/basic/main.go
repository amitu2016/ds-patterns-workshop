package main

import (
	"context"
	"fmt"
	"log"
	"time"

	clientv3 "go.etcd.io/etcd/client/v3"
	"kubelite/pkg/listwatch"
	"kubelite/pkg/storage"
)

func main() {
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

	// Create a new ListWatch instance
	prefix := "/example/"
	opts := listwatch.DefaultOptions()
	lw, err := listwatch.NewListWatch([]string{endpoint}, prefix, opts, nil)
	if err != nil {
		log.Fatalf("Failed to create ListWatch: %v", err)
	}

	// Create a context with timeout
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	// Start watching for changes
	eventCh, stopWatch, err := lw.ListAndWatch(ctx)
	if err != nil {
		log.Fatalf("Failed to start watching: %v", err)
	}
	defer stopWatch()

	// Start a goroutine to make changes to etcd
	go func() {
		// Wait for watch to be established
		time.Sleep(1 * time.Second)

		// Add a new key
		key1 := prefix + "app1"
		fmt.Printf("Adding key: %s\n", key1)
		_, err := etcdClient.Put(ctx, key1, "value1")
		if err != nil {
			log.Printf("Failed to put key1: %v", err)
			return
		}

		time.Sleep(2 * time.Second)

		// Modify the key
		fmt.Printf("Modifying key: %s\n", key1)
		_, err = etcdClient.Put(ctx, key1, "value1-updated")
		if err != nil {
			log.Printf("Failed to modify key1: %v", err)
			return
		}

		time.Sleep(2 * time.Second)

		// Add another key
		key2 := prefix + "app2"
		fmt.Printf("Adding key: %s\n", key2)
		_, err = etcdClient.Put(ctx, key2, "value2")
		if err != nil {
			log.Printf("Failed to put key2: %v", err)
			return
		}

		time.Sleep(2 * time.Second)

		// Delete first key
		fmt.Printf("Deleting key: %s\n", key1)
		_, err = etcdClient.Delete(ctx, key1)
		if err != nil {
			log.Printf("Failed to delete key1: %v", err)
			return
		}
	}()

	// Process events
	fmt.Println("Watching for events...")
	for event := range eventCh {
		switch event.Type {
		case listwatch.Added:
			fmt.Printf("Added: %s = %s\n", event.Key, string(event.Value))
		case listwatch.Modified:
			fmt.Printf("Modified: %s = %s\n", event.Key, string(event.Value))
		case listwatch.Deleted:
			fmt.Printf("Deleted: %s\n", event.Key)
		case listwatch.Error:
			fmt.Printf("Error: %s\n", string(event.Value))
		}
	}
}
