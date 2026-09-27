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

package v1alpha1

import (
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// TemporalWorkerSpec defines the desired state of TemporalWorker
type TemporalWorkerSpec struct {
	// TemporalNamespaceRef references the TemporalNamespace used by this worker.
	// +kubebuilder:validation:XValidation:rule="self.name != ''",message="temporalNamespaceRef.name must be set"
	// +required
	TemporalNamespaceRef corev1.LocalObjectReference `json:"temporalNamespaceRef"`

	// Template describes the worker pod template.
	// +required
	Template corev1.PodTemplateSpec `json:"template"`

	// Replicas is the desired number of worker replicas.
	// +kubebuilder:validation:Minimum=1
	// +kubebuilder:default=1
	// +optional
	Replicas *int32 `json:"replicas,omitempty"`
}

// TemporalWorkerStatus defines the observed state of TemporalWorker.
type TemporalWorkerStatus struct {
	ObservedGeneration  int64                        `json:"observedGeneration,omitempty"`
	ConnectionRef       *corev1.LocalObjectReference `json:"connectionRef,omitempty"`
	WorkerDeploymentRef *corev1.LocalObjectReference `json:"workerDeploymentRef,omitempty"`

	// conditions represent the current state of the TemporalWorker resource.
	// Each condition has a unique type and reflects the status of a specific aspect of the resource.
	//
	// Standard condition types include:
	// - "Available": the resource is fully functional
	// - "Progressing": the resource is being created or updated
	// - "Degraded": the resource failed to reach or maintain its desired state
	//
	// The status of each condition is one of True, False, or Unknown.
	// +listType=map
	// +listMapKey=type
	// +optional
	Conditions []metav1.Condition `json:"conditions,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// TemporalWorker is the Schema for the temporalworkers API
type TemporalWorker struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of TemporalWorker
	// +required
	Spec TemporalWorkerSpec `json:"spec"`

	// status defines the observed state of TemporalWorker
	// +optional
	Status TemporalWorkerStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TemporalWorkerList contains a list of TemporalWorker
type TemporalWorkerList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []TemporalWorker `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &TemporalWorker{}, &TemporalWorkerList{})
		return nil
	})
}
