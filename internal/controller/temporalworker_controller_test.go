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
	"testing"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"

	temporalv1alpha1 "github.com/nori-cloud/temporal-platform-operator/api/v1alpha1"
	temporaliov1alpha1 "github.com/temporalio/temporal-worker-controller/api/v1alpha1"
)

func TestTemporalWorkerCreatesSharedConnectionAndDelegatesTemplate(t *testing.T) {
	const (
		testNamespace    = "app"
		testTemporalNS   = "orders"
		testWorker       = "orders-worker"
		applicationLabel = "app"
	)

	scheme := runtime.NewScheme()
	if err := temporalv1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add operator scheme: %v", err)
	}
	if err := temporaliov1alpha1.AddToScheme(scheme); err != nil {
		t.Fatalf("add upstream scheme: %v", err)
	}

	namespace := &temporalv1alpha1.TemporalNamespace{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testTemporalNS,
			Namespace: testNamespace,
			UID:       types.UID("namespace-uid"),
		},
		Status: temporalv1alpha1.TemporalNamespaceStatus{
			Conditions: []metav1.Condition{{
				Type:   readyConditionType,
				Status: metav1.ConditionTrue,
			}},
		},
	}
	template := corev1.PodTemplateSpec{
		ObjectMeta: metav1.ObjectMeta{
			Labels: map[string]string{applicationLabel: testWorker},
		},
		Spec: corev1.PodSpec{
			Containers: []corev1.Container{{
				Name:  "worker",
				Image: "example.com/" + testWorker + ":v1.0.0",
				Env:   []corev1.EnvVar{{Name: "CUSTOM_SETTING", Value: "kept"}},
			}},
		},
	}
	worker := &temporalv1alpha1.TemporalWorker{
		ObjectMeta: metav1.ObjectMeta{
			Name:      testWorker,
			Namespace: testNamespace,
			UID:       types.UID("worker-uid"),
		},
		Spec: temporalv1alpha1.TemporalWorkerSpec{
			TemporalNamespaceRef: corev1.LocalObjectReference{Name: testTemporalNS},
			Template:             template,
		},
	}

	client := fake.NewClientBuilder().
		WithScheme(scheme).
		WithStatusSubresource(&temporalv1alpha1.TemporalWorker{}).
		WithObjects(namespace, worker).
		Build()
	reconciler := &TemporalWorkerReconciler{
		Client:          client,
		Scheme:          scheme,
		FrontendAddress: "temporal-frontend.temporal.svc.cluster.local:7233",
	}
	request := ctrl.Request{NamespacedName: types.NamespacedName{Namespace: testNamespace, Name: testWorker}}

	for attempt := range 4 {
		if _, err := reconciler.Reconcile(context.Background(), request); err != nil {
			t.Fatalf("reconcile attempt %d: %v", attempt+1, err)
		}
	}

	connection := &temporaliov1alpha1.Connection{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testTemporalNS}, connection); err != nil {
		t.Fatalf("get shared connection: %v", err)
	}
	if connection.Spec.HostPort != reconciler.FrontendAddress {
		t.Fatalf("connection host = %q, want %q", connection.Spec.HostPort, reconciler.FrontendAddress)
	}
	if owner := metav1.GetControllerOf(connection); owner == nil || owner.UID != namespace.UID {
		t.Fatalf("connection owner = %#v, want TemporalNamespace %q", owner, namespace.UID)
	}

	workerDeployment := &temporaliov1alpha1.WorkerDeployment{}
	if err := client.Get(context.Background(), types.NamespacedName{Namespace: testNamespace, Name: testWorker}, workerDeployment); err != nil {
		t.Fatalf("get worker deployment: %v", err)
	}
	if owner := metav1.GetControllerOf(workerDeployment); owner == nil || owner.UID != worker.UID {
		t.Fatalf("worker deployment owner = %#v, want TemporalWorker %q", owner, worker.UID)
	}
	if workerDeployment.Spec.WorkerOptions.ConnectionRef.Name != connection.Name {
		t.Fatalf("connection reference = %q, want %q", workerDeployment.Spec.WorkerOptions.ConnectionRef.Name, connection.Name)
	}
	if workerDeployment.Spec.WorkerOptions.TemporalNamespace != namespace.Name {
		t.Fatalf("Temporal namespace = %q, want %q", workerDeployment.Spec.WorkerOptions.TemporalNamespace, namespace.Name)
	}
	if !reflect.DeepEqual(workerDeployment.Spec.Template, template) {
		t.Fatalf("worker template was changed by the wrapper: %#v", workerDeployment.Spec.Template)
	}
	if got := workerDeployment.Spec.Template.Spec.Containers[0].Env; len(got) != 1 || got[0].Name != "CUSTOM_SETTING" {
		t.Fatalf("managed environment variables were injected: %#v", got)
	}
	if workerDeployment.Spec.RolloutStrategy.Strategy != temporaliov1alpha1.UpdateProgressive {
		t.Fatalf("default rollout strategy = %q, want Progressive", workerDeployment.Spec.RolloutStrategy.Strategy)
	}
}
