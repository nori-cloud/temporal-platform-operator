# Temporal Platform Operator

A Kubernetes operator for a self-hosted Temporal cluster.

## Controllers

The Helm chart runs two independent controller deployments:

- `namespace` registers and updates Temporal namespaces.
- `worker` creates Temporal Worker Controller resources and direct frontend connections.

Both controllers use the configured `temporal.frontendAddress`. The namespace
controller creates the Temporal SDK client; the worker controller only writes
the endpoint into the generated Connection resource.

## Install

```sh
helm install temporal-platform-operator \
  oci://ghcr.io/nori-cloud/charts/temporal-platform-operator \
  --version <version> \
  --namespace temporal \
  --create-namespace \
  --set temporal.frontendAddress=temporal-frontend.temporal.svc.cluster.local:7233
```

The chart installs CRDs and the upstream Temporal Worker Controller dependency.

## Example namespace

```yaml
apiVersion: temporal.nori-cloud.io/v1alpha1
kind: TemporalNamespace
metadata:
  name: test
  namespace: temporal
spec:
  retentionDays: 7
```

A Temporal namespace is a logical Temporal resource. It is not a Kubernetes
namespace and does not require a proxy.

## Validation

```sh
make lint
go build ./...
go vet ./...
helm lint --strict charts/temporal-platform-operator
```
