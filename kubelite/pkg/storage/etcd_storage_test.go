package storage

import (
	"context"
	"github.com/stretchr/testify/require"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	clientv3 "go.etcd.io/etcd/client/v3"
)

type TestObject struct {
	Name string `json:"name"`
}

func TestEtcdStorage_Create(t *testing.T) {
	TestWithEmbeddedEtcd(t, func(t *testing.T, cli *clientv3.Client) {
		storage := NewEtcdStorage(cli)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		obj := &TestObject{Name: "test-value"}
		err := storage.Create(ctx, "test-key", obj)
		assert.NoError(t, err)

		var retrievedObj TestObject
		err = storage.Get(ctx, "test-key", &retrievedObj)
		assert.NoError(t, err)

		assert.Equal(t, "test-value", retrievedObj.Name)
	})
}

func TestEtcdStorage_Update(t *testing.T) {
	TestWithEmbeddedEtcd(t, func(t *testing.T, cli *clientv3.Client) {
		storage := NewEtcdStorage(cli)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		obj := &TestObject{Name: "test-value"}
		err := storage.Create(ctx, "test-key", obj)
		assert.NoError(t, err)

		updatedObj := &TestObject{Name: "updated-value"}
		err = storage.Update(ctx, "test-key", updatedObj)
		assert.NoError(t, err)

		var retrievedObj TestObject
		err = storage.Get(ctx, "test-key", &retrievedObj)
		assert.NoError(t, err)

		assert.Equal(t, "updated-value", retrievedObj.Name)
	})
}

func TestEtcdStorage_Delete(t *testing.T) {
	TestWithEmbeddedEtcd(t, func(t *testing.T, cli *clientv3.Client) {
		storage := NewEtcdStorage(cli)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		obj := &TestObject{Name: "test-value"}
		err := storage.Create(ctx, "test-key", obj)
		assert.NoError(t, err)

		err = storage.Delete(ctx, "test-key")
		assert.NoError(t, err)

		var retrievedObj TestObject
		err = storage.Get(ctx, "test-key", &retrievedObj)
		assert.Error(t, err)
	})
}

func TestEtcdStorage_List(t *testing.T) {
	TestWithEmbeddedEtcd(t, func(t *testing.T, cli *clientv3.Client) {
		storage := NewEtcdStorage(cli)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		obj1 := &TestObject{Name: "value1"}
		err := storage.Create(ctx, "/prefix/key1", obj1)
		assert.NoError(t, err)

		obj2 := &TestObject{Name: "value2"}
		err = storage.Create(ctx, "/prefix/key2", obj2)
		assert.NoError(t, err)

		var list []*TestObject
		err = storage.List(ctx, "/prefix/", &list)
		assert.NoError(t, err)

		assert.Len(t, list, 2)
		assert.ElementsMatch(t, []*TestObject{obj1, obj2}, list)
	})
}

func TestEtcdStorage_Watch(t *testing.T) {
	t.Run("should watch all CRUD operations", func(t *testing.T) {
		TestWithEmbeddedEtcd(t, func(t *testing.T, cli *clientv3.Client) {
			storage := NewEtcdStorage(cli)
			prefix := "/watch-test/"
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			// Create initial test object
			obj1 := &TestObject{Name: "test1"}
			obj2 := &TestObject{Name: "test1-updated"}
			obj3 := &TestObject{Name: "test2"}

			// Start watching before making changes
			watchChan, err := storage.Watch(ctx, prefix)
			require.NoError(t, err)

			// Test sequence of operations
			testCases := []struct {
				name   string
				action func() error
				expect watchExpectation
			}{
				{
					name: "create first object",
					action: func() error {
						return storage.Create(ctx, prefix+"key1", obj1)
					},
					expect: watchExpectation{
						eventType:   EventAdd,
						key:         prefix + "key1",
						hasValue:    true,
						hasOldValue: false,
					},
				},
				{
					name: "update first object",
					action: func() error {
						return storage.Update(ctx, prefix+"key1", obj2)
					},
					expect: watchExpectation{
						eventType:   EventUpdate,
						key:         prefix + "key1",
						hasValue:    true,
						hasOldValue: true,
					},
				},
				{
					name: "create second object",
					action: func() error {
						return storage.Create(ctx, prefix+"key2", obj3)
					},
					expect: watchExpectation{
						eventType:   EventAdd,
						key:         prefix + "key2",
						hasValue:    true,
						hasOldValue: false,
					},
				},
				{
					name: "delete first object",
					action: func() error {
						return storage.Delete(ctx, prefix+"key1")
					},
					expect: watchExpectation{
						eventType:   EventDelete,
						key:         prefix + "key1",
						hasValue:    false,
						hasOldValue: true,
					},
				},
			}

			// Execute test cases
			for _, tc := range testCases {
				t.Run(tc.name, func(t *testing.T) {
					require.NoError(t, tc.action())
					verifyWatchEvent(t, watchChan, tc.expect)
				})
			}
		})
	})

	t.Run("should stop watching when context is cancelled", func(t *testing.T) {
		TestWithEmbeddedEtcd(t, func(t *testing.T, cli *clientv3.Client) {
			storage := NewEtcdStorage(cli)
			ctx, cancel := context.WithCancel(context.Background())

			watchChan, err := storage.Watch(ctx, "/test/")
			require.NoError(t, err)

			// Cancel context and verify channel is closed
			cancel()
			verifyChannelClosed(t, watchChan)
		})
	})

	t.Run("should watch multiple objects with prefix", func(t *testing.T) {
		TestWithEmbeddedEtcd(t, func(t *testing.T, cli *clientv3.Client) {
			storage := NewEtcdStorage(cli)
			prefix := "/multi-watch/"
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			watchChan, err := storage.Watch(ctx, prefix)
			require.NoError(t, err)

			// Create multiple objects in different paths
			objects := []struct {
				key string
				obj *TestObject
			}{
				{prefix + "a/key1", &TestObject{Name: "test-a1"}},
				{prefix + "b/key1", &TestObject{Name: "test-b1"}},
				{prefix + "a/key2", &TestObject{Name: "test-a2"}},
			}

			// Create objects and verify events
			for _, o := range objects {
				err := storage.Create(ctx, o.key, o.obj)
				require.NoError(t, err)

				verifyWatchEvent(t, watchChan, watchExpectation{
					eventType:   EventAdd,
					key:         o.key,
					hasValue:    true,
					hasOldValue: false,
				})
			}
		})
	})
}

type watchExpectation struct {
	eventType   EventType
	key         string
	hasValue    bool
	hasOldValue bool
}

func verifyWatchEvent(t *testing.T, watchChan <-chan WatchEvent, expect watchExpectation) {
	select {
	case event := <-watchChan:
		assert.Equal(t, expect.eventType, event.Type, "wrong event type")
		assert.Equal(t, expect.key, event.Key, "wrong key")

		if expect.hasValue {
			assert.NotEmpty(t, event.Value, "should have value")
		} else {
			assert.Empty(t, event.Value, "should not have value")
		}

		if expect.hasOldValue {
			assert.NotEmpty(t, event.OldValue, "should have old value")
		} else {
			assert.Empty(t, event.OldValue, "should not have old value")
		}
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for event")
	}
}

func verifyChannelClosed(t *testing.T, watchChan <-chan WatchEvent) {
	select {
	case _, ok := <-watchChan:
		assert.False(t, ok, "Expected watch channel to be closed")
	case <-time.After(time.Second):
		t.Fatal("Timed out waiting for channel to close")
	}
}
