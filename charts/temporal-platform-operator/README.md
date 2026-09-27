# Temporal Platform Operator chart

This chart installs the Nori Cloud Temporal operator and the upstream Temporal
Worker Controller dependencies.

## Controllers

The operator image runs as two deployments:

- `temporal-platform-operator-namespace` registers Temporal namespaces.
- `temporal-platform-operator-worker` creates direct frontend Connections and
  WorkerDeployments.

Both deployments receive `temporal.frontendAddress` from Helm values. This is a
cluster-internal service address, not a credential.

## Values

```yaml
namespace: temporal

temporal:
  frontendAddress: temporal-frontend.temporal.svc.cluster.local:7233

controllers:
  namespace:
    enabled: true
    replicaCount: 1
  worker:
    enabled: true
    replicaCount: 1
```

The chart does not deploy a Temporal proxy. Temporal namespaces are logical
names in the Temporal API; Kubernetes namespaces remain the scope for the
operator resources.
