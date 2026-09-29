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
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	temporalv1alpha1 "github.com/nori-cloud/temporal-platform-operator/api/v1alpha1"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
)

const (
	workerRecheckInterval          = 5 * time.Second
	defaultWorkerReplicas          = int32(1)
	defaultProgressDeadlineSeconds = int32(600)
)

// TemporalWorkerReconciler reconciles a TemporalWorker object.
type TemporalWorkerReconciler struct {
	client.Client
	Scheme          *runtime.Scheme
	FrontendAddress string
}

// +kubebuilder:rbac:groups=temporal.nori-cloud.io,resources=temporalworkers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=temporal.nori-cloud.io,resources=temporalworkers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=temporal.nori-cloud.io,resources=temporalworkers/finalizers,verbs=update
// +kubebuilder:rbac:groups=temporal.nori-cloud.io,resources=temporalnamespaces,verbs=get;list;watch
// +kubebuilder:rbac:groups=temporal.nori-cloud.io,resources=temporalproxies,verbs=get;list;watch
// +kubebuilder:rbac:groups=temporal.io,resources=connections,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=temporal.io,resources=workerdeployments,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=temporal.io,resources=workerdeployments/status,verbs=get

//nolint:gocyclo // reconciliation coordinates several independent Kubernetes resources and lifecycle states.
func (r *TemporalWorkerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	resource := &temporalv1alpha1.TemporalWorker{}
	if err := r.Get(ctx, req.NamespacedName, resource); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	workerDeployment := &temporaliov1alpha1.WorkerDeployment{}
	workerDeploymentKey := client.ObjectKey{Namespace: resource.Namespace, Name: resource.Name}
	if !resource.DeletionTimestamp.IsZero() {
		if err := r.Get(ctx, workerDeploymentKey, workerDeployment); err != nil {
			if !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
			if controllerutil.ContainsFinalizer(resource, temporalFinalizer) {
				controllerutil.RemoveFinalizer(resource, temporalFinalizer)
				return ctrl.Result{}, r.Update(ctx, resource)
			}
			return ctrl.Result{}, nil
		}
		if workerDeployment.DeletionTimestamp.IsZero() {
			if err := r.Delete(ctx, workerDeployment); err != nil && !apierrors.IsNotFound(err) {
				return ctrl.Result{}, err
			}
		}
		return ctrl.Result{RequeueAfter: workerRecheckInterval}, nil
	}

	if !controllerutil.ContainsFinalizer(resource, temporalFinalizer) {
		controllerutil.AddFinalizer(resource, temporalFinalizer)
		return ctrl.Result{}, r.Update(ctx, resource)
	}

	temporalNamespace := &temporalv1alpha1.TemporalNamespace{}
	namespaceKey := client.ObjectKey{Namespace: resource.Namespace, Name: resource.Spec.TemporalNamespaceRef.Name}
	if err := r.Get(ctx, namespaceKey, temporalNamespace); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{RequeueAfter: workerRecheckInterval}, r.setPending(ctx, resource, "WaitingForTemporalNamespace", "referenced TemporalNamespace does not exist")
		}
		return ctrl.Result{}, err
	}
	beforeOwnerReferences := append([]metav1.OwnerReference(nil), resource.OwnerReferences...)
	if err := controllerutil.SetControllerReference(temporalNamespace, resource, r.Scheme); err != nil {
		return ctrl.Result{}, err
	}
	if !reflect.DeepEqual(beforeOwnerReferences, resource.OwnerReferences) {
		if err := r.Update(ctx, resource); err != nil {
			return ctrl.Result{}, err
		}
	}

	if !isReady(temporalNamespace.Status.Conditions) {
		return ctrl.Result{RequeueAfter: workerRecheckInterval}, r.setPending(ctx, resource, "WaitingForTemporalNamespace", "referenced TemporalNamespace is not Ready")
	}

	if r.FrontendAddress == "" {
		return ctrl.Result{RequeueAfter: workerRecheckInterval}, r.setPending(ctx, resource, "WaitingForFrontend", "Temporal frontend address is not configured")
	}

	connection := &temporaliov1alpha1.Connection{}
	connection.Namespace = resource.Namespace
	connection.Name = resource.Name + "-connection"
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, connection, func() error {
		if err := controllerutil.SetControllerReference(resource, connection, r.Scheme); err != nil {
			return err
		}
		connection.Spec.HostPort = r.FrontendAddress
		return nil
	}); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.Get(ctx, workerDeploymentKey, workerDeployment); err != nil && !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	if workerDeployment.Name == "" {
		workerDeployment.Namespace = resource.Namespace
		workerDeployment.Name = resource.Name
	}
	desiredWorkerDeployment := workerDeploymentSpec(resource, connection.Name, r.FrontendAddress)
	if _, err := controllerutil.CreateOrUpdate(ctx, r.Client, workerDeployment, func() error {
		if err := controllerutil.SetControllerReference(resource, workerDeployment, r.Scheme); err != nil {
			return err
		}
		workerDeployment.Spec = desiredWorkerDeployment
		return nil
	}); err != nil {
		return ctrl.Result{}, err
	}

	conditions := append([]metav1.Condition(nil), workerDeployment.Status.Conditions...)
	if len(conditions) == 0 {
		conditions = []metav1.Condition{{
			Type:               readyConditionType,
			Status:             metav1.ConditionUnknown,
			Reason:             "WaitingForWorkerDeployment",
			Message:            "WorkerDeployment has not reported a status yet",
			ObservedGeneration: resource.Generation,
			LastTransitionTime: metav1.Now(),
		}}
	}
	if err := r.setStatus(ctx, resource, connection.Name, workerDeployment.Name, conditions); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *TemporalWorkerReconciler) setPending(ctx context.Context, resource *temporalv1alpha1.TemporalWorker, reason, message string) error {
	conditions := []metav1.Condition{{
		Type:               readyConditionType,
		Status:             metav1.ConditionUnknown,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: resource.Generation,
		LastTransitionTime: metav1.Now(),
	}}
	return r.setStatus(ctx, resource, "", "", conditions)
}

func (r *TemporalWorkerReconciler) setStatus(ctx context.Context, resource *temporalv1alpha1.TemporalWorker, connectionName, workerDeploymentName string, conditions []metav1.Condition) error {
	status := resource.Status.DeepCopy()
	status.ObservedGeneration = resource.Generation
	if connectionName == "" {
		status.ConnectionRef = nil
	} else {
		status.ConnectionRef = &corev1.LocalObjectReference{Name: connectionName}
	}
	if workerDeploymentName == "" {
		status.WorkerDeploymentRef = nil
	} else {
		status.WorkerDeploymentRef = &corev1.LocalObjectReference{Name: workerDeploymentName}
	}
	status.Conditions = append([]metav1.Condition(nil), conditions...)
	if reflect.DeepEqual(resource.Status, *status) {
		return nil
	}
	resource.Status = *status
	return r.Status().Update(ctx, resource)
}

func workerDeploymentSpec(resource *temporalv1alpha1.TemporalWorker, connectionName, frontendAddress string) temporaliov1alpha1.WorkerDeploymentSpec {
	replicas := defaultWorkerReplicas
	if resource.Spec.Replicas != nil {
		replicas = *resource.Spec.Replicas
	}
	progressDeadlineSeconds := defaultProgressDeadlineSeconds
	template := *resource.Spec.Template.DeepCopy()
	if len(template.Spec.Containers) > 0 {
		template.Spec.Containers[0].Env = upsertEnv(template.Spec.Containers[0].Env,
			corev1.EnvVar{Name: "TEMPORAL_GRPC_ENDPOINT", Value: frontendAddress},
			corev1.EnvVar{Name: "TEMPORAL_NAMESPACE", Value: resource.Spec.TemporalNamespaceRef.Name},
		)
	}
	return temporaliov1alpha1.WorkerDeploymentSpec{
		Replicas:                &replicas,
		Template:                template,
		ProgressDeadlineSeconds: &progressDeadlineSeconds,
		RolloutStrategy: temporaliov1alpha1.RolloutStrategy{
			Strategy: temporaliov1alpha1.UpdateProgressive,
			Steps: []temporaliov1alpha1.RolloutStep{
				{RampPercentage: 25, PauseDuration: metav1.Duration{Duration: 30 * time.Second}},
				{RampPercentage: 50, PauseDuration: metav1.Duration{Duration: 30 * time.Second}},
				{RampPercentage: 99, PauseDuration: metav1.Duration{Duration: 30 * time.Second}},
			},
		},
		SunsetStrategy: temporaliov1alpha1.SunsetStrategy{
			ScaledownDelay: &metav1.Duration{Duration: time.Hour},
			DeleteDelay:    &metav1.Duration{Duration: 24 * time.Hour},
		},
		WorkerOptions: temporaliov1alpha1.WorkerOptions{
			ConnectionRef:     temporaliov1alpha1.ConnectionReference{Name: connectionName},
			TemporalNamespace: resource.Spec.TemporalNamespaceRef.Name,
		},
	}
}

func upsertEnv(env []corev1.EnvVar, managed ...corev1.EnvVar) []corev1.EnvVar {
	result := append([]corev1.EnvVar(nil), env...)
	for _, value := range managed {
		updated := false
		for index := range result {
			if result[index].Name == value.Name {
				result[index] = value
				updated = true
				break
			}
		}
		if !updated {
			result = append(result, value)
		}
	}
	return result
}

func isReady(conditions []metav1.Condition) bool {
	condition := apiMeta.FindStatusCondition(conditions, readyConditionType)
	return condition != nil && condition.Status == metav1.ConditionTrue
}

// SetupWithManager sets up the controller with the Manager.
func (r *TemporalWorkerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&temporalv1alpha1.TemporalWorker{}).
		Owns(&temporaliov1alpha1.WorkerDeployment{}).
		Watches(&temporalv1alpha1.TemporalNamespace{}, handler.EnqueueRequestsFromMapFunc(r.mapTemporalNamespaceToWorkers)).
		Named("temporalworker").
		Complete(r)
}

func (r *TemporalWorkerReconciler) mapTemporalNamespaceToWorkers(ctx context.Context, object client.Object) []reconcile.Request {
	workers := &temporalv1alpha1.TemporalWorkerList{}
	if err := r.List(ctx, workers, client.InNamespace(object.GetNamespace())); err != nil {
		return nil
	}
	requests := make([]reconcile.Request, 0)
	for i := range workers.Items {
		worker := &workers.Items[i]
		if worker.Spec.TemporalNamespaceRef.Name == object.GetName() {
			requests = append(requests, reconcile.Request{NamespacedName: client.ObjectKeyFromObject(worker)})
		}
	}
	return requests
}
