/*
Copyright 2026 Nori Cloud.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"os"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	corev1 "k8s.io/api/core/v1"
	networkingv1 "k8s.io/api/networking/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/intstr"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	temporalv1alpha1 "github.com/nori-cloud/temporal-platform-operator/api/v1alpha1"
)

const (
	temporalProxyImage   = "temporalio/temporal-proxy:v0.7.0"
	proxyPort            = int32(7233)
	proxyConfigKey       = "config.yaml"
	proxyConfigMount     = "/etc/proxy"
	frontendAddressEnv   = "TEMPORAL_FRONTEND_ADDRESS"
	defaultProxyName     = "default"
	defaultProxyWorkload = "temporal-proxy"
	proxyNameLabel       = "app.kubernetes.io/name"
	proxyInstanceLabel   = "app.kubernetes.io/instance"
)

// TemporalProxyReconciler reconciles a TemporalProxy object.
type TemporalProxyReconciler struct {
	client.Client
	Scheme   *runtime.Scheme
	Recorder record.EventRecorder
}

// +kubebuilder:rbac:groups=temporal.nori-cloud.io,resources=temporalproxies,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=temporal.nori-cloud.io,resources=temporalproxies/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=temporal.nori-cloud.io,resources=temporalproxies/finalizers,verbs=update
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=services,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups="",resources=events,verbs=create;patch;update
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=networking.k8s.io,resources=networkpolicies,verbs=get;list;watch;create;update;patch;delete

func (r *TemporalProxyReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	resource := &temporalv1alpha1.TemporalProxy{}
	if err := r.Get(ctx, req.NamespacedName, resource); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !resource.DeletionTimestamp.IsZero() {
		if controllerutil.ContainsFinalizer(resource, temporalFinalizer) {
			controllerutil.RemoveFinalizer(resource, temporalFinalizer)
			return ctrl.Result{}, r.Update(ctx, resource)
		}
		return ctrl.Result{}, nil
	}

	if !controllerutil.ContainsFinalizer(resource, temporalFinalizer) {
		controllerutil.AddFinalizer(resource, temporalFinalizer)
		return ctrl.Result{}, r.Update(ctx, resource)
	}

	if resource.Name == defaultProxyName {
		return r.reconcileDefault(ctx, resource)
	}

	frontendAddress := os.Getenv(frontendAddressEnv)
	if err := validateFrontendAddress(frontendAddress); err != nil {
		return ctrl.Result{}, r.setError(ctx, resource, err)
	}

	config := proxyConfig(frontendAddress)
	hash := configRevision(config)
	labels := proxyLabels(resource)
	configMap := &corev1.ConfigMap{}
	deployment := &appsv1.Deployment{}
	service := &corev1.Service{}
	networkPolicy := &networkingv1.NetworkPolicy{}
	changed := false

	mutations := []struct {
		object client.Object
		mutate func() error
	}{
		{configMap, func() error {
			configMap.Name, configMap.Namespace = proxyConfigName(resource), resource.Namespace
			configMap.Labels = labels
			configMap.Data = map[string]string{proxyConfigKey: config}
			return controllerutil.SetControllerReference(resource, configMap, r.Scheme)
		}},
		{deployment, func() error {
			deployment.Name, deployment.Namespace = proxyDeploymentName(resource), resource.Namespace
			deployment.Labels = labels
			deployment.Spec = proxyDeploymentSpec(resource, hash)
			return controllerutil.SetControllerReference(resource, deployment, r.Scheme)
		}},
		{service, func() error {
			service.Name, service.Namespace = proxyServiceName(resource), resource.Namespace
			service.Labels = labels
			service.Spec = proxyServiceSpec(resource)
			return controllerutil.SetControllerReference(resource, service, r.Scheme)
		}},
		{networkPolicy, func() error {
			networkPolicy.Name, networkPolicy.Namespace = proxyNetworkPolicyName(resource), resource.Namespace
			networkPolicy.Labels = labels
			networkPolicy.Spec = proxyNetworkPolicySpec(resource)
			return controllerutil.SetControllerReference(resource, networkPolicy, r.Scheme)
		}},
	}
	for _, mutation := range mutations {
		created, err := controllerutil.CreateOrUpdate(ctx, r.Client, mutation.object, mutation.mutate)
		if err != nil {
			return ctrl.Result{}, r.setError(ctx, resource, err)
		}
		changed = changed || created != controllerutil.OperationResultNone
	}

	if changed {
		r.event(resource, corev1.EventTypeNormal, "DriftDetected", "reconciled drift in Temporal proxy infrastructure")
	}
	r.event(resource, corev1.EventTypeNormal, "Sync", "Temporal proxy infrastructure is synchronized")

	resource.Status.ObservedGeneration = resource.Generation
	resource.Status.Service = &temporalv1alpha1.TemporalProxyServiceStatus{
		Name:      service.Name,
		Namespace: service.Namespace,
		Endpoint:  fmt.Sprintf("%s.%s.svc.cluster.local:%d", service.Name, service.Namespace, proxyPort),
	}
	resource.Status.Routes = []string{"*"}
	resource.Status.ConfigRevision = hash
	apiMeta.SetStatusCondition(&resource.Status.Conditions, metav1.Condition{
		Type:               readyConditionType,
		Status:             metav1.ConditionTrue,
		Reason:             "Synced",
		Message:            "Temporal proxy is ready",
		ObservedGeneration: resource.Generation,
	})
	if err := r.Status().Update(ctx, resource); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

const defaultProxyRecheckInterval = 5 * time.Second

func (r *TemporalProxyReconciler) reconcileDefault(ctx context.Context, resource *temporalv1alpha1.TemporalProxy) (ctrl.Result, error) {
	configMap := &corev1.ConfigMap{}
	deployment := &appsv1.Deployment{}
	service := &corev1.Service{}

	configMapKey := client.ObjectKey{Namespace: resource.Namespace, Name: "temporal-proxy-config"}
	if err := r.Get(ctx, configMapKey, configMap); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: defaultProxyRecheckInterval}, r.setDefaultNotReady(ctx, resource, "ConfigMapNotFound", "Helm-managed default proxy ConfigMap does not exist", nil, "")
		}
		return ctrl.Result{}, err
	}

	deploymentKey := client.ObjectKey{Namespace: resource.Namespace, Name: defaultProxyWorkload}
	if err := r.Get(ctx, deploymentKey, deployment); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: defaultProxyRecheckInterval}, r.setDefaultNotReady(ctx, resource, "DeploymentNotFound", "Helm-managed default proxy Deployment does not exist", nil, configRevisionFromConfigMap(configMap))
		}
		return ctrl.Result{}, err
	}

	if err := r.Get(ctx, client.ObjectKey{Namespace: resource.Namespace, Name: defaultProxyWorkload}, service); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: defaultProxyRecheckInterval}, r.setDefaultNotReady(ctx, resource, "ServiceNotFound", "Helm-managed default proxy Service does not exist", nil, configRevisionFromConfigMap(configMap))
		}
		return ctrl.Result{}, err
	}

	serviceStatus := &temporalv1alpha1.TemporalProxyServiceStatus{
		Name:      service.Name,
		Namespace: service.Namespace,
		Endpoint:  fmt.Sprintf("%s.%s.svc.cluster.local:%d", service.Name, service.Namespace, proxyPort),
	}
	configRevision := configRevisionFromConfigMap(configMap)
	if configMap.Data[proxyConfigKey] == "" {
		return ctrl.Result{RequeueAfter: defaultProxyRecheckInterval}, r.setDefaultNotReady(ctx, resource, "ConfigMapInvalid", "Helm-managed default proxy ConfigMap has no config.yaml", serviceStatus, configRevision)
	}
	if err := validateProxyService(service); err != nil {
		return ctrl.Result{RequeueAfter: defaultProxyRecheckInterval}, r.setDefaultNotReady(ctx, resource, "ServiceNotReady", err.Error(), serviceStatus, configRevision)
	}
	if err := validateProxyDeployment(deployment); err != nil {
		return ctrl.Result{RequeueAfter: defaultProxyRecheckInterval}, r.setDefaultNotReady(ctx, resource, "DeploymentNotReady", err.Error(), serviceStatus, configRevision)
	}

	r.event(resource, corev1.EventTypeNormal, "Observed", "observed ready Helm-managed default proxy infrastructure")
	if err := r.setDefaultStatus(ctx, resource, metav1.ConditionTrue, "Observed", "Helm-managed default proxy is ready", serviceStatus, configRevision); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *TemporalProxyReconciler) setDefaultNotReady(ctx context.Context, resource *temporalv1alpha1.TemporalProxy, reason, message string, service *temporalv1alpha1.TemporalProxyServiceStatus, configRevision string) error {
	r.event(resource, corev1.EventTypeWarning, reason, message)
	return r.setDefaultStatus(ctx, resource, metav1.ConditionFalse, reason, message, service, configRevision)
}

func (r *TemporalProxyReconciler) setDefaultStatus(ctx context.Context, resource *temporalv1alpha1.TemporalProxy, status metav1.ConditionStatus, reason, message string, service *temporalv1alpha1.TemporalProxyServiceStatus, configRevision string) error {
	resource.Status.ObservedGeneration = resource.Generation
	resource.Status.Service = service
	resource.Status.Routes = []string{"*"}
	resource.Status.ConfigRevision = configRevision
	apiMeta.SetStatusCondition(&resource.Status.Conditions, metav1.Condition{
		Type:               readyConditionType,
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: resource.Generation,
	})
	return r.Status().Update(ctx, resource)
}

func validateProxyService(service *corev1.Service) error {
	if len(service.Spec.Selector) == 0 {
		return errors.New("default proxy Service has no pod selector")
	}
	for _, port := range service.Spec.Ports {
		if port.Port == proxyPort {
			return nil
		}
	}
	return fmt.Errorf("default proxy Service does not expose port %d", proxyPort)
}

func validateProxyDeployment(deployment *appsv1.Deployment) error {
	desiredReplicas := int32(1)
	if deployment.Spec.Replicas != nil {
		desiredReplicas = *deployment.Spec.Replicas
	}
	if deployment.Status.AvailableReplicas < desiredReplicas {
		return fmt.Errorf("default proxy Deployment has %d/%d available replicas", deployment.Status.AvailableReplicas, desiredReplicas)
	}
	return nil
}

func configRevisionFromConfigMap(configMap *corev1.ConfigMap) string {
	return configRevision(configMap.Data[proxyConfigKey])
}

func (r *TemporalProxyReconciler) setError(ctx context.Context, resource *temporalv1alpha1.TemporalProxy, reconcileErr error) error {
	r.event(resource, corev1.EventTypeWarning, "Error", reconcileErr.Error())
	resource.Status.ObservedGeneration = resource.Generation
	apiMeta.SetStatusCondition(&resource.Status.Conditions, metav1.Condition{
		Type:               readyConditionType,
		Status:             metav1.ConditionFalse,
		Reason:             "Error",
		Message:            reconcileErr.Error(),
		ObservedGeneration: resource.Generation,
	})
	if statusErr := r.Status().Update(ctx, resource); statusErr != nil {
		return statusErr
	}
	return reconcileErr
}

func (r *TemporalProxyReconciler) event(object client.Object, eventType, reason, message string) {
	if r.Recorder != nil {
		r.Recorder.Event(object, eventType, reason, message)
	}
}

func (r *TemporalProxyReconciler) SetupWithManager(mgr ctrl.Manager) error {
	//nolint:staticcheck // controller-runtime's new event API is not yet compatible with this recorder field.
	r.Recorder = mgr.GetEventRecorderFor("temporalproxy")
	return ctrl.NewControllerManagedBy(mgr).
		For(&temporalv1alpha1.TemporalProxy{}).
		Owns(&corev1.ConfigMap{}).
		Owns(&appsv1.Deployment{}).
		Owns(&corev1.Service{}).
		Owns(&networkingv1.NetworkPolicy{}).
		Watches(&corev1.ConfigMap{}, handler.EnqueueRequestsFromMapFunc(mapDefaultProxyConfigMap)).
		Watches(&appsv1.Deployment{}, handler.EnqueueRequestsFromMapFunc(mapDefaultProxyDeployment)).
		Watches(&corev1.Service{}, handler.EnqueueRequestsFromMapFunc(mapDefaultProxyService)).
		Watches(&networkingv1.NetworkPolicy{}, handler.EnqueueRequestsFromMapFunc(mapDefaultProxyNetworkPolicy)).
		Named("temporalproxy").
		Complete(r)
}

func mapDefaultProxyConfigMap(ctx context.Context, object client.Object) []reconcile.Request {
	return mapDefaultProxyResource(object, defaultProxyWorkload+"-config")
}

func mapDefaultProxyDeployment(ctx context.Context, object client.Object) []reconcile.Request {
	return mapDefaultProxyResource(object, defaultProxyWorkload)
}

func mapDefaultProxyService(ctx context.Context, object client.Object) []reconcile.Request {
	return mapDefaultProxyResource(object, defaultProxyWorkload)
}

func mapDefaultProxyNetworkPolicy(ctx context.Context, object client.Object) []reconcile.Request {
	return mapDefaultProxyResource(object, defaultProxyWorkload)
}

func mapDefaultProxyResource(object client.Object, name string) []reconcile.Request {
	if object.GetName() != name {
		return nil
	}
	return []reconcile.Request{{NamespacedName: client.ObjectKey{Namespace: object.GetNamespace(), Name: defaultProxyName}}}
}

func validateFrontendAddress(address string) error {
	if address == "" {
		return errors.New("TEMPORAL_FRONTEND_ADDRESS is not set")
	}
	_, port, err := net.SplitHostPort(address)
	if err != nil || port == "" {
		return fmt.Errorf("TEMPORAL_FRONTEND_ADDRESS must be a host:port address: %q", address)
	}
	return nil
}

func proxyConfig(frontendAddress string) string {
	return fmt.Sprintf("hostPort: %q\ninsecure: true\nrouting:\n  default: self-hosted\n  system: self-hosted\nupstreams:\n  - name: self-hosted\n    hostPort: %q\n    insecure: true\n", ":7233", frontendAddress)
}

func configRevision(config string) string {
	digest := sha256.Sum256([]byte(config))
	return hex.EncodeToString(digest[:])
}

func proxyLabels(resource *temporalv1alpha1.TemporalProxy) map[string]string {
	return map[string]string{
		proxyNameLabel:                 defaultProxyWorkload,
		proxyInstanceLabel:             resource.Name,
		"app.kubernetes.io/managed-by": "temporal-platform-operator",
	}
}

func proxyBaseName(resource *temporalv1alpha1.TemporalProxy) string {
	if resource.Name == defaultProxyName {
		return defaultProxyWorkload
	}
	return resource.Name + "-proxy"
}

func proxyConfigName(resource *temporalv1alpha1.TemporalProxy) string {
	return proxyBaseName(resource) + "-config"
}

func proxyDeploymentName(resource *temporalv1alpha1.TemporalProxy) string {
	return proxyBaseName(resource)
}

func proxyServiceName(resource *temporalv1alpha1.TemporalProxy) string {
	return proxyBaseName(resource)
}

func proxyNetworkPolicyName(resource *temporalv1alpha1.TemporalProxy) string {
	return proxyBaseName(resource)
}

func proxyDeploymentSpec(resource *temporalv1alpha1.TemporalProxy, hash string) appsv1.DeploymentSpec {
	labels := proxyLabels(resource)
	return appsv1.DeploymentSpec{
		Replicas: int32ptr(1),
		Selector: &metav1.LabelSelector{MatchLabels: map[string]string{proxyInstanceLabel: resource.Name, proxyNameLabel: defaultProxyWorkload}},
		Template: corev1.PodTemplateSpec{
			ObjectMeta: metav1.ObjectMeta{Labels: labels, Annotations: map[string]string{"temporal.nori-cloud.io/config-hash": hash}},
			Spec: corev1.PodSpec{Containers: []corev1.Container{{
				Name:         "proxy",
				Image:        temporalProxyImage,
				Args:         []string{"serve", "--config", proxyConfigMount + "/" + proxyConfigKey},
				Ports:        []corev1.ContainerPort{{Name: "grpc", ContainerPort: proxyPort, Protocol: corev1.ProtocolTCP}},
				VolumeMounts: []corev1.VolumeMount{{Name: "proxy-config", MountPath: proxyConfigMount, ReadOnly: true}},
			}}, Volumes: []corev1.Volume{{Name: "proxy-config", VolumeSource: corev1.VolumeSource{ConfigMap: &corev1.ConfigMapVolumeSource{LocalObjectReference: corev1.LocalObjectReference{Name: proxyConfigName(resource)}}}}}},
		},
	}
}

func proxyServiceSpec(resource *temporalv1alpha1.TemporalProxy) corev1.ServiceSpec {
	return corev1.ServiceSpec{Selector: map[string]string{proxyInstanceLabel: resource.Name, proxyNameLabel: defaultProxyWorkload}, Ports: []corev1.ServicePort{{Name: "grpc", Port: proxyPort, TargetPort: intstr.FromInt32(proxyPort), Protocol: corev1.ProtocolTCP}}}
}

func proxyNetworkPolicySpec(resource *temporalv1alpha1.TemporalProxy) networkingv1.NetworkPolicySpec {
	selector := metav1.LabelSelector{MatchLabels: map[string]string{proxyInstanceLabel: resource.Name, proxyNameLabel: defaultProxyWorkload}}
	port := intstr.FromInt32(proxyPort)
	return networkingv1.NetworkPolicySpec{PodSelector: selector, PolicyTypes: []networkingv1.PolicyType{networkingv1.PolicyTypeIngress, networkingv1.PolicyTypeEgress}, Ingress: []networkingv1.NetworkPolicyIngressRule{{Ports: []networkingv1.NetworkPolicyPort{{Port: &port, Protocol: protocolPtr(corev1.ProtocolTCP)}}}}, Egress: []networkingv1.NetworkPolicyEgressRule{{}}}
}

func protocolPtr(protocol corev1.Protocol) *corev1.Protocol { return &protocol }
func int32ptr(value int32) *int32                           { return &value }
