# kubelite → Kubernetes

The bridge from this model to real Kubernetes upstream source code. Every package and component in `kubelite` maps directly to its upstream Kubernetes counterpart.

## Architecture Mapping

| kubelite Package / Type | Upstream Kubernetes | Upstream Source Location | Concept & Responsibility |
|---|---|---|---|
| `pkg/api` (`Pod`, `Node`, `ReplicaSet`, `ObjectMeta`) | `k8s.io/api/core/v1`, `k8s.io/apimachinery/pkg/apis/meta/v1` | [k8s.io/api](https://github.com/kubernetes/api) | Core declarative resource schemas, specs, statuses, and metadata. |
| `pkg/storage` (`EtcdStorage`) | `k8s.io/apiserver/pkg/storage/etcd3` | [k8s.io/apiserver/pkg/storage/etcd3](https://github.com/kubernetes/apiserver/tree/master/pkg/storage/etcd3) | Low-level key-value persistence adapter wrapping etcd v3 client (`Put`, `Get`, `List`, `Watch`, `Delete`). |
| `pkg/registry` (`PodRegistry`, `NodeRegistry`, `ReplicaSetRegistry`) | `k8s.io/kubernetes/pkg/registry/{pods,nodes,replicasets}` | [k8s.io/kubernetes/pkg/registry](https://github.com/kubernetes/kubernetes/tree/master/pkg/registry) | REST storage abstractions translating domain resource operations into standardized etcd keys (`/registry/<resource>/<name>`). |
| `pkg/api/server` (`APIServer`) | `k8s.io/kubernetes/pkg/controlplane.Instance`, `k8s.io/apiserver/pkg/server` | [k8s.io/kubernetes/pkg/controlplane](https://github.com/kubernetes/kubernetes/tree/master/pkg/controlplane) | HTTP REST API server handling request validation, routing, and exposing `/api/v1/{nodes,pods,replicasets}` endpoints. |
| `pkg/api/handlers` | `k8s.io/apiserver/pkg/endpoints/handlers` | [k8s.io/apiserver/pkg/endpoints/handlers](https://github.com/kubernetes/apiserver/tree/master/pkg/endpoints/handlers) | HTTP handler methods for standard CRUD and collection operations. |
| `pkg/listwatch` (`ListWatcher`, `Informer`) | `k8s.io/client-go/tools/cache.ListWatch` | [k8s.io/client-go/tools/cache](https://github.com/kubernetes/client-go/tree/master/tools/cache) | Reliable event-driven synchronization pattern: initial `List` for baseline state followed by streaming `Watch` from resource revision to track `Added`, `Modified`, and `Deleted` events. |
| `pkg/controller` (`ReplicaSetController`) | `k8s.io/kubernetes/pkg/controller/replicaset` | [k8s.io/kubernetes/pkg/controller/replicaset](https://github.com/kubernetes/kubernetes/tree/master/pkg/controller/replicaset) | Declarative reconciliation loop: continuously inspects active pods matching the selector vs. `spec.replicas` and creates or terminates pods to converge actual state to desired state. |
| `pkg/scheduler` (`Scheduler`) | `k8s.io/kubernetes/pkg/scheduler` | [k8s.io/kubernetes/pkg/scheduler](https://github.com/kubernetes/kubernetes/tree/master/pkg/scheduler) | Watches for unscheduled pods (`Status == Pending`, `NodeName == ""`), selects healthy candidate nodes (`Status == Ready`), binds pods to nodes, and commits assignments to etcd. |
| `pkg/kubelet` (`Kubelet`) | `k8s.io/kubernetes/pkg/kubelet` | [k8s.io/kubernetes/pkg/kubelet](https://github.com/kubernetes/kubernetes/tree/master/pkg/kubelet) | Node agent that registers the host machine (`POST /api/v1/nodes`), posts periodic health heartbeats, and watches for pods assigned to its node. |

---

## Deliberate Divergences

| Divergence | upstream Kubernetes | kubelite | Rationale |
|---|---|---|---|
| **Process Model** | Multi-process distributed daemons (`kube-apiserver`, `kube-controller-manager`, `kube-scheduler`, `etcd`, `kubelet`) running on separate nodes. | Single in-process runtime or isolated integration tests. | Zero setup friction. Eliminates complex external etcd setup, TLS certificates, and distributed orchestration while demonstrating identical algorithmic interactions. |
| **Embedded etcd** | Dedicated standalone or clustered etcd cluster running as external processes. | In-process embedded etcd (`go.etcd.io/etcd/server/v3/embed`) listening on ephemeral localhost ports with auto-cleaned temp directories. | Guaranteed isolation, zero host port conflicts, fast startup, and 100% real etcd v3 Raft protocol without mocking. |
| **Scheduling Pipeline** | Multi-phase scheduling cycle with Predicates (Filtering: resource requests, taints/tolerations, affinity) and Priorities (Scoring: bin-packing, spreading). | Round-robin / random selection over nodes with `NodeReady` status. | Focuses on the core coordination mechanism (finding pending pods, selecting a candidate node, atomically writing `pod.NodeName` and `PodScheduled`) without the complexity of full resource accounting. |
| **Container Runtime Interface (CRI)** | Kubelet communicates over gRPC with a container runtime daemon (`containerd`, `CRI-O`) to manage Linux cgroups, namespaces, and rootfs. | Kubelet tracks pod state, node heartbeats, and API events without spawning real OS containers. | Focuses on the distributed systems control plane (etcd, API server, controllers, scheduling) rather than Linux kernel container primitives. |
| **Controller Queues** | Informer delta FIFOs, shared indexers, and rate-limiting work queues. | Direct registry queries and ticker-based reconciliation triggers. | Clear pedagogical clarity for understanding the reconciliation equation: `diff = desired - actual`. |

---

## Going Deeper

- Upstream source: [kubernetes/kubernetes](https://github.com/kubernetes/kubernetes)
- Client library: [kubernetes/client-go](https://github.com/kubernetes/client-go)
- etcd distributed key-value store: [etcd-io/etcd](https://github.com/etcd-io/etcd)

## Scheduler — the two phases, and what we left out

| kubelite | Kubernetes | |
|---|---|---|
| `scheduler.FindNodesThatFit` | `pkg/scheduler` Filter plugins (predicates) | one predicate: `nodeIsReady` |
| `scheduler.PrioritizeNodes` | Score plugins (priorities) | one priority: LeastPods, the analogue of `LeastAllocated` |
| `scheduler.SelectHost` | `selectHost` | |
| `scheduler.Bind` | Bind plugin | |
| `maxNodeScore` | `framework.MaxNodeScore` | 100 |

Deliberate divergences:

- **One predicate, one priority.** Real Kubernetes runs chains of both — `PodFitsResources`,
  `PodFitsHostPorts`, node affinity, taints and tolerations, and a dozen scorers. Our `api.Pod`
  carries no resource requests, so most have nothing to evaluate. The *shape* — filter is
  disqualifying, score is a ranking — is what carries over.
- **Ties break by name, not at random.** Upstream picks uniformly among tied nodes (reservoir
  sampling) to avoid hot-spotting. We break deterministically so a demo places identically
  every run.
- **Scoring is sequential.** Upstream scores nodes in parallel across a worker pool.

## ReplicaSet controller — both directions of the diff

Scale-down deletes the excess pods. Upstream ranks victims first (`getPodsToDelete`: unassigned
before assigned, Pending before Running, newest before oldest) so scaling down costs least; we
delete from the end of the owned list. Status is written only after the deletions land, so it
never reports a convergence that did not happen.
