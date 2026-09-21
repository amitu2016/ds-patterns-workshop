# kubelite: Miniature Kubernetes Control Plane with Embedded etcd

`kubelite` is a faithful Go implementation of the Kubernetes control plane designed to demonstrate distributed coordination, declarative state reconciliation, and the Consistent Core pattern.

It runs against a real in-process embedded **etcd v3** cluster (`go.etcd.io/etcd/server/v3/embed`), requiring zero external dependencies, Docker daemons, or separate etcd instances.

---

## Demos

All demos run from the root of `litesystems`:

```bash
make demo-1.2    # Node Registration with etcd (Kubelet -> API Server -> etcd)
make demo-1.3    # Consistent Core Interface & Cluster Primitives (ListWatch pattern)
make demo-2.3    # ReplicaSet Controller (Reconciliation & Self-Healing loop)
make demo-2.4    # Scheduler (Pod Scheduling Algorithm & Node Assignment)
```

| Demo | Slide / Topic | What it Demonstrates |
|---|---|---|
| `demo-1.2` | Node registration with etcd | Kubelet registers host with API server via `POST /api/v1/nodes`; persisted under `/registry/nodes/<n>`. |
| `demo-1.3` | ListWatch Pattern | Initial baseline `List` followed by streaming `Watch` from resource revision to process `Added`, `Modified`, and `Deleted` events. |
| `demo-2.3` | ReplicaSet Controller Flow | Reconciles actual vs desired pod count; detects killed/crashed pods and automatically recovers. |
| `demo-2.4` | Pod Scheduling Algorithm | Discovers unassigned `Pending` pods, selects available healthy nodes, and binds pods (`pod.NodeName`). |

---

## Architecture & Upstream Mapping

Every package in `kubelite` corresponds directly to upstream Kubernetes code:

| kubelite Package | Upstream Kubernetes Counterpart | Role |
|---|---|---|
| `pkg/api` | `k8s.io/api/core/v1` | Core declarative structs (`Pod`, `Node`, `ReplicaSet`, `ObjectMeta`). |
| `pkg/storage` | `k8s.io/apiserver/pkg/storage/etcd3` | Real etcd v3 client wrapper (`Put`, `Get`, `List`, `Watch`, `Delete`). |
| `pkg/registry` | `k8s.io/kubernetes/pkg/registry` | Resource-specific REST storage mappings to etcd keys. |
| `pkg/api/server` | `k8s.io/kubernetes/pkg/controlplane.Instance` | REST HTTP API server exposing `/api/v1/{nodes,pods,replicasets}`. |
| `pkg/listwatch` | `k8s.io/client-go/tools/cache.ListWatch` | Reliable event stream consumer pattern. |
| `pkg/controller` | `k8s.io/kubernetes/pkg/controller/replicaset` | Reconciles desired replica count with actual active cluster pods. |
| `pkg/scheduler` | `k8s.io/kubernetes/pkg/scheduler` | Matches pending unscheduled pods with healthy nodes. |
| `pkg/kubelet` | `k8s.io/kubernetes/pkg/kubelet` | Node agent registering node readiness with the API server. |

See [MAPPING.md](./MAPPING.md) for full upstream mapping and deliberate pedagogical divergences.

---

## Directory Structure

```
kubelite/
├── cmd/               # CLI entrypoints (apiserver, controller, kubelet, scheduler)
├── demo/              # Slide demos (1.2, 1.3, 2.3, 2.4)
├── pkg/
│   ├── api/           # Domain models, JSON REST serialization, and HTTP handlers
│   │   ├── handlers/  # REST endpoints for nodes, pods, replicasets
│   │   └── server/    # go-restful API server container
│   ├── controller/    # ReplicaSet controller reconciliation
│   ├── kubelet/       # Node registration and heartbeats
│   ├── listwatch/     # ListWatch client and event streaming
│   ├── registry/      # Node, Pod, and ReplicaSet registries backed by etcd
│   ├── scheduler/     # Pod scheduling algorithm
│   └── storage/       # Embedded etcd harness and EtcdStorage client
├── test/              # Integration and end-to-end tests
├── go.mod             # Go module definition
├── MAPPING.md         # Upstream source mapping
└── README.md
```

---

## Running Tests

From `kubelite/`:
```bash
go test ./...
```

Or from the repository root:
```bash
make test
```