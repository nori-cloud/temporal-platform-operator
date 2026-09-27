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

// TemporalNamespaceSpec defines the desired state of TemporalNamespace
type TemporalNamespaceSpec struct {
	// RetentionDays is the number of days Temporal retains workflow history.
	// +kubebuilder:validation:Minimum=7
	// +kubebuilder:validation:Maximum=30
	// +kubebuilder:default=7
	// +optional
	RetentionDays int32 `json:"retentionDays,omitempty"`

	// ProxyRef references the TemporalProxy used by this namespace.
	// +kubebuilder:default={"name":"default"}
	// +kubebuilder:validation:XValidation:rule="self.name != ''",message="proxyRef.name must be set"
	// +required
	ProxyRef corev1.LocalObjectReference `json:"proxyRef"`
}

// TemporalNamespaceStatus defines the observed state of TemporalNamespace.
type TemporalNamespaceStatus struct {
	// INSERT ADDITIONAL STATUS FIELD - define observed state of cluster
	// Important: Run "make" to regenerate code after modifying this file

	// For Kubernetes API conventions, see:
	// https://github.com/kubernetes/community/blob/master/contributors/devel/sig-architecture/api-conventions.md#typical-status-properties

	// conditions represent the current state of the TemporalNamespace resource.
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

// TemporalNamespace is the Schema for the temporalnamespaces API
type TemporalNamespace struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of TemporalNamespace
	// +required
	Spec TemporalNamespaceSpec `json:"spec"`

	// status defines the observed state of TemporalNamespace
	// +optional
	Status TemporalNamespaceStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TemporalNamespaceList contains a list of TemporalNamespace
type TemporalNamespaceList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []TemporalNamespace `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &TemporalNamespace{}, &TemporalNamespaceList{})
		return nil
	})
}
