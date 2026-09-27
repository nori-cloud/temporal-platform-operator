/*
Copyright 2026 Nori Cloud.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package controller

import (
	"context"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"

	temporalv1alpha1 "github.com/nori-cloud/temporal-platform-operator/api/v1alpha1"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
)

// TemporalWorkerReconciler reconciles a TemporalWorker object.
type TemporalWorkerReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=temporal.nori-cloud.io,resources=temporalworkers,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=temporal.nori-cloud.io,resources=temporalworkers/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=temporal.nori-cloud.io,resources=temporalworkers/finalizers,verbs=update
// +kubebuilder:rbac:groups=temporal.io,resources=workerdeployments,verbs=get;list;watch;create;update;patch;delete

func (r *TemporalWorkerReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	resource := &temporalv1alpha1.TemporalWorker{}
	if err := r.Get(ctx, req.NamespacedName, resource); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !resource.DeletionTimestamp.IsZero() {
		// Owned WorkerDeployments are garbage-collected with the custom resource.
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

	changed := setPendingCondition(&resource.Status.Conditions, resource.Generation,
		"Temporal worker reconciliation is not implemented yet")
	if changed {
		return ctrl.Result{}, r.Status().Update(ctx, resource)
	}

	// TODO: Reconcile a WorkerDeployment and its progressively rolled out versions.
	return ctrl.Result{}, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *TemporalWorkerReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&temporalv1alpha1.TemporalWorker{}).
		Owns(&temporaliov1alpha1.WorkerDeployment{}).
		Named("temporalworker").
		Complete(r)
}
