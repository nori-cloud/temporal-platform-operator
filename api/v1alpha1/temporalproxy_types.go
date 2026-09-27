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
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// TemporalProxySpec defines the desired state of TemporalProxy
type TemporalProxySpec struct {
	// RoutePolicy controls which Temporal namespaces may use this proxy.
	// +kubebuilder:validation:Enum=AllNamespaces
	// +kubebuilder:default="AllNamespaces"
	// +optional
	RoutePolicy string `json:"routePolicy,omitempty"`
}

// TemporalProxyServiceStatus describes the Service endpoint published for a proxy.
type TemporalProxyServiceStatus struct {
	Name      string `json:"name,omitempty"`
	Namespace string `json:"namespace,omitempty"`
	Endpoint  string `json:"endpoint,omitempty"`
}

// TemporalProxyStatus defines the observed state of TemporalProxy.
type TemporalProxyStatus struct {
	ObservedGeneration int64                       `json:"observedGeneration,omitempty"`
	Service            *TemporalProxyServiceStatus `json:"service,omitempty"`
	Routes             []string                    `json:"routes,omitempty"`
	ConfigRevision     string                      `json:"configRevision,omitempty"`

	// conditions represent the current state of the TemporalProxy resource.
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

// TemporalProxy is the Schema for the temporalproxies API
type TemporalProxy struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of TemporalProxy
	// +required
	Spec TemporalProxySpec `json:"spec"`

	// status defines the observed state of TemporalProxy
	// +optional
	Status TemporalProxyStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// TemporalProxyList contains a list of TemporalProxy
type TemporalProxyList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []TemporalProxy `json:"items"`
}

func init() {
	SchemeBuilder.Register(func(s *runtime.Scheme) error {
		s.AddKnownTypes(SchemeGroupVersion, &TemporalProxy{}, &TemporalProxyList{})
		return nil
	})
}
