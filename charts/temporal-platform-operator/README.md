# Temporal Platform Operator Helm chart

This umbrella chart installs the Temporal platform operator, its platform CRDs, a
Temporal Proxy, and the OCI-published Temporal Worker Controller dependencies.
The worker controller and its CRDs remain chart dependencies; their templates are
not copied into this chart.

## Install

The chart installs into the namespace configured by `namespace` (default:
`temporal`). Create that namespace first, or install with a release namespace
that matches it:

```sh
helm upgrade --install temporal-platform-operator ./charts/temporal-platform-operator \
  --namespace temporal --create-namespace
```

The default proxy routes every namespace to the configured Temporal frontend:

```yaml
namespace: temporal
temporal:
  frontendAddress: temporal-frontend.temporal.svc.cluster.local:7233
proxy:
  image:
    repository: temporalio/temporal-proxy
    tag: v0.7.0
```

The parent chart pins both OCI dependencies to version `0.29.1` and defaults
the worker controller image to `v1.11.0`. It also disables installation of
cert-manager while leaving its integration enabled, and does not create the
optional worker-controller end-user roles.

## Validate

Fetch dependencies and run Helm's local checks:

```sh
helm dependency build ./charts/temporal-platform-operator
helm lint ./charts/temporal-platform-operator
helm template temporal-platform-operator ./charts/temporal-platform-operator >/tmp/temporal-platform-operator.yaml
```
