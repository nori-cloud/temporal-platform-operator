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
	"errors"
	"fmt"
	"reflect"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	apiMeta "k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/tools/record"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/handler"
	"sigs.k8s.io/controller-runtime/pkg/reconcile"

	"google.golang.org/grpc/codes"
	goapierrors "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"

	namespacev1 "go.temporal.io/api/namespace/v1"
	operatorservice "go.temporal.io/api/operatorservice/v1"
	replicationv1 "go.temporal.io/api/replication/v1"
	serviceerror "go.temporal.io/api/serviceerror"
	workflowservice "go.temporal.io/api/workflowservice/v1"
	temporalclient "go.temporal.io/sdk/client"

	temporalv1alpha1 "github.com/nori-cloud/temporal-platform-operator/api/v1alpha1"
)

const (
	temporalNamespaceRefIndex = "temporal.nori-cloud.io/temporal-namespace-ref"
	workersRecheckInterval    = 5 * time.Second
	defaultRetentionDays      = int32(7)
	minRetentionDays          = int32(7)
	maxRetentionDays          = int32(30)
)

// TemporalNamespaceReconciler reconciles a TemporalNamespace object.
type TemporalNamespaceReconciler struct {
	client.Client
	Scheme *runtime.Scheme

	// TemporalClient is shared by all namespace reconciles and is created from
	// TEMPORAL_FRONTEND_ADDRESS by the manager startup code.
	TemporalClient temporalclient.Client
	Recorder       record.EventRecorder
}

// +kubebuilder:rbac:groups=temporal.nori-cloud.io,resources=temporalnamespaces,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=temporal.nori-cloud.io,resources=temporalnamespaces/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=temporal.nori-cloud.io,resources=temporalnamespaces/finalizers,verbs=update
// +kubebuilder:rbac:groups=temporal.nori-cloud.io,resources=temporalworkers,verbs=get;list;watch

func (r *TemporalNamespaceReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	resource := &temporalv1alpha1.TemporalNamespace{}
	if err := r.Get(ctx, req.NamespacedName, resource); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !resource.DeletionTimestamp.IsZero() {
		return r.reconcileDeletion(ctx, resource)
	}

	if !controllerutil.ContainsFinalizer(resource, temporalFinalizer) {
		controllerutil.AddFinalizer(resource, temporalFinalizer)
		if err := r.Update(ctx, resource); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{}, nil
	}

	retentionDays := resource.Spec.RetentionDays
	if retentionDays == 0 {
		retentionDays = defaultRetentionDays
	}
	if retentionDays < minRetentionDays || retentionDays > maxRetentionDays {
		return r.reconcileError(ctx, resource, fmt.Errorf("retentionDays must be between %d and %d (got %d)", minRetentionDays, maxRetentionDays, retentionDays))
	}
	if r.TemporalClient == nil {
		return r.reconcileError(ctx, resource, errors.New("Temporal client is not configured"))
	}

	describe, err := r.TemporalClient.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: resource.Name})
	if err != nil {
		var notFound *serviceerror.NamespaceNotFound
		if !errors.As(err, &notFound) && goapierrors.Code(err) != codes.NotFound {
			return r.reconcileError(ctx, resource, fmt.Errorf("describe Temporal namespace %q: %w", resource.Name, err))
		}

		_, registerErr := r.TemporalClient.WorkflowService().RegisterNamespace(ctx, &workflowservice.RegisterNamespaceRequest{
			Namespace:                        resource.Name,
			WorkflowExecutionRetentionPeriod: durationpb.New(retentionDuration(retentionDays)),
		})
		if registerErr != nil {
			var alreadyExists *serviceerror.NamespaceAlreadyExists
			if !errors.As(registerErr, &alreadyExists) && goapierrors.Code(registerErr) != codes.AlreadyExists {
				return r.reconcileError(ctx, resource, fmt.Errorf("register Temporal namespace %q: %w", resource.Name, registerErr))
			}
			// Another reconcile may have registered it. Describe it and continue
			// through the normal drift/update path.
			describe, err = r.TemporalClient.WorkflowService().DescribeNamespace(ctx, &workflowservice.DescribeNamespaceRequest{Namespace: resource.Name})
			if err != nil {
				return r.reconcileError(ctx, resource, fmt.Errorf("describe Temporal namespace %q after registration race: %w", resource.Name, err))
			}
		} else {
			r.emitEvent(resource, corev1.EventTypeNormal, "Sync", "Registered Temporal namespace")
			return r.setReady(ctx, resource, "Temporal namespace registered")
		}
	}

	if describe == nil || describe.Config == nil || describe.Config.WorkflowExecutionRetentionTtl == nil ||
		describe.Config.WorkflowExecutionRetentionTtl.AsDuration() != retentionDuration(retentionDays) {
		r.emitEvent(resource, corev1.EventTypeNormal, "DriftDetected", fmt.Sprintf("Temporal namespace retention differs from desired %d days", retentionDays))
		if err := r.updateNamespace(ctx, resource.Name, describe, retentionDays); err != nil {
			return r.reconcileError(ctx, resource, fmt.Errorf("update Temporal namespace %q: %w", resource.Name, err))
		}
		r.emitEvent(resource, corev1.EventTypeNormal, "Sync", "Updated Temporal namespace retention")
	}

	return r.setReady(ctx, resource, "Temporal namespace is synchronized")
}

func (r *TemporalNamespaceReconciler) updateNamespace(ctx context.Context, name string, described *workflowservice.DescribeNamespaceResponse, retentionDays int32) error {
	config := &namespacev1.NamespaceConfig{}
	if described != nil && described.Config != nil {
		config = proto.Clone(described.Config).(*namespacev1.NamespaceConfig)
	}
	config.WorkflowExecutionRetentionTtl = durationpb.New(retentionDuration(retentionDays))

	request := &workflowservice.UpdateNamespaceRequest{
		Namespace: name,
		Config:    config,
	}
	if described != nil {
		if described.NamespaceInfo != nil {
			// Temporal's update API replaces these fields. Preserve metadata that
			// may have been assigned by an administrator or another controller.
			request.UpdateInfo = &namespacev1.UpdateNamespaceInfo{
				Description: described.NamespaceInfo.Description,
				OwnerEmail:  described.NamespaceInfo.OwnerEmail,
				Data:        cloneStringMap(described.NamespaceInfo.Data),
			}
		}
		if described.ReplicationConfig != nil {
			request.ReplicationConfig = proto.Clone(described.ReplicationConfig).(*replicationv1.NamespaceReplicationConfig)
		}
	}
	_, err := r.TemporalClient.WorkflowService().UpdateNamespace(ctx, request)
	return err
}

func (r *TemporalNamespaceReconciler) reconcileDeletion(ctx context.Context, resource *temporalv1alpha1.TemporalNamespace) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(resource, temporalFinalizer) {
		return ctrl.Result{}, nil
	}

	workers, err := r.workersReferencingNamespace(ctx, resource.Name)
	if err != nil {
		return r.reconcileError(ctx, resource, fmt.Errorf("list TemporalWorkers referencing namespace %q: %w", resource.Name, err))
	}
	if len(workers.Items) != 0 {
		message := fmt.Sprintf("waiting for %d TemporalWorker(s) to be deleted", len(workers.Items))
		r.emitEvent(resource, corev1.EventTypeNormal, "Sync", message)
		if err := r.setCondition(ctx, resource, metav1.ConditionUnknown, "WaitingForWorkers", message); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{RequeueAfter: workersRecheckInterval}, nil
	}

	// Re-list immediately before the destructive call. This closes the race
	// between the first worker check and deleting the Temporal namespace.
	workers, err = r.workersReferencingNamespace(ctx, resource.Name)
	if err != nil {
		return r.reconcileError(ctx, resource, fmt.Errorf("final worker check for namespace %q: %w", resource.Name, err))
	}
	if len(workers.Items) != 0 {
		return ctrl.Result{RequeueAfter: workersRecheckInterval}, nil
	}
	if r.TemporalClient == nil {
		return r.reconcileError(ctx, resource, errors.New("Temporal client is not configured"))
	}

	_, err = r.TemporalClient.OperatorService().DeleteNamespace(ctx, &operatorservice.DeleteNamespaceRequest{Namespace: resource.Name})
	if err != nil && !isTemporalNotFound(err) {
		return r.reconcileError(ctx, resource, fmt.Errorf("delete Temporal namespace %q: %w", resource.Name, err))
	}
	r.emitEvent(resource, corev1.EventTypeNormal, "Sync", "Deleted Temporal namespace")
	controllerutil.RemoveFinalizer(resource, temporalFinalizer)
	if err := r.Update(ctx, resource); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *TemporalNamespaceReconciler) workersReferencingNamespace(ctx context.Context, namespace string) (*temporalv1alpha1.TemporalWorkerList, error) {
	workers := &temporalv1alpha1.TemporalWorkerList{}
	err := r.List(ctx, workers, client.MatchingFields{temporalNamespaceRefIndex: namespace})
	return workers, err
}

func (r *TemporalNamespaceReconciler) reconcileError(ctx context.Context, resource *temporalv1alpha1.TemporalNamespace, err error) (ctrl.Result, error) {
	r.emitEvent(resource, corev1.EventTypeWarning, "Error", err.Error())
	if statusErr := r.setCondition(ctx, resource, metav1.ConditionFalse, "ReconciliationError", err.Error()); statusErr != nil {
		return ctrl.Result{}, statusErr
	}
	return ctrl.Result{}, err
}

func (r *TemporalNamespaceReconciler) setReady(ctx context.Context, resource *temporalv1alpha1.TemporalNamespace, message string) (ctrl.Result, error) {
	if err := r.setCondition(ctx, resource, metav1.ConditionTrue, "Synced", message); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

func (r *TemporalNamespaceReconciler) setCondition(ctx context.Context, resource *temporalv1alpha1.TemporalNamespace, status metav1.ConditionStatus, reason, message string) error {
	before := append([]metav1.Condition(nil), resource.Status.Conditions...)
	apiMeta.SetStatusCondition(&resource.Status.Conditions, metav1.Condition{
		Type:               "Ready",
		Status:             status,
		Reason:             reason,
		Message:            message,
		ObservedGeneration: resource.Generation,
	})
	if reflect.DeepEqual(before, resource.Status.Conditions) {
		return nil
	}
	return r.Status().Update(ctx, resource)
}

func (r *TemporalNamespaceReconciler) emitEvent(resource *temporalv1alpha1.TemporalNamespace, eventType, reason, message string) {
	if r.Recorder != nil {
		r.Recorder.Event(resource, eventType, reason, message)
	}
}

func (r *TemporalNamespaceReconciler) SetupWithManager(mgr ctrl.Manager) error {
	if err := mgr.GetFieldIndexer().IndexField(context.Background(), &temporalv1alpha1.TemporalWorker{}, temporalNamespaceRefIndex, func(obj client.Object) []string {
		worker := obj.(*temporalv1alpha1.TemporalWorker)
		if worker.Spec.TemporalNamespaceRef.Name == "" {
			return nil
		}
		return []string{worker.Spec.TemporalNamespaceRef.Name}
	}); err != nil {
		return err
	}

	return ctrl.NewControllerManagedBy(mgr).
		For(&temporalv1alpha1.TemporalNamespace{}).
		Watches(&temporalv1alpha1.TemporalWorker{}, handler.EnqueueRequestsFromMapFunc(func(ctx context.Context, obj client.Object) []reconcile.Request {
			worker := obj.(*temporalv1alpha1.TemporalWorker)
			if worker.Spec.TemporalNamespaceRef.Name == "" {
				return nil
			}
			return []reconcile.Request{{NamespacedName: types.NamespacedName{Namespace: worker.Namespace, Name: worker.Spec.TemporalNamespaceRef.Name}}}
		})).
		WithOptions(controller.Options{MaxConcurrentReconciles: 1}).
		Named("temporalnamespace").
		Complete(r)
}

func retentionDuration(days int32) time.Duration {
	return time.Duration(days) * 24 * time.Hour
}

func cloneStringMap(values map[string]string) map[string]string {
	if values == nil {
		return nil
	}
	clone := make(map[string]string, len(values))
	for key, value := range values {
		clone[key] = value
	}
	return clone
}

func isTemporalNotFound(err error) bool {
	var notFound *serviceerror.NamespaceNotFound
	return errors.As(err, &notFound) || goapierrors.Code(err) == codes.NotFound
}
