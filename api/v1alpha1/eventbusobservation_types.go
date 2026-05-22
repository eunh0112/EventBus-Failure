/*
Copyright 2026.

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
)

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// EventBusObservationSpec defines the desired state of EventBusObservation
// EventBusObservationSpec defines the desired state of EventBusObservation
type EventBusObservationSpec struct {
	EventBusRef EventBusRef `json:"eventBusRef"`

	PrometheusURL string `json:"prometheusURL"`

	StreamName string `json:"streamName"`

	Primary EventBusCandidate `json:"primary"`

	Candidates []EventBusCandidate `json:"candidates"`

	FailoverRequired bool `json:"failoverRequired,omitempty"`
}

type EventBusRef struct {
	Name string `json:"name"`

	Namespace string `json:"namespace"`
}

type EventBusCandidate struct {
	Cluster string `json:"cluster"`

	Endpoint string `json:"endpoint"`
}

// EventBusObservationStatus defines the observed state of EventBusObservation
type EventBusObservationStatus struct {
	PrimaryStatus EventBusObservedStatus `json:"primaryStatus,omitempty"`

	CandidateStatuses []EventBusObservedStatus `json:"candidateStatuses,omitempty"`

	SelectedStandby *SelectedStandby `json:"selectedStandby,omitempty"`

	FailoverRequired bool `json:"failoverRequired,omitempty"`
}

type EventBusObservedStatus struct {
	Cluster string `json:"cluster,omitempty"`

	Endpoint string `json:"endpoint,omitempty"`

	Phase string `json:"phase,omitempty"`

	LastSeq int64 `json:"lastSeq,omitempty"`

	ReplicationGap int64 `json:"replicationGap,omitempty"`
}

type SelectedStandby struct {
	Cluster string `json:"cluster,omitempty"`

	Endpoint string `json:"endpoint,omitempty"`

	Reason string `json:"reason,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// EventBusObservation is the Schema for the eventbusobservations API
type EventBusObservation struct {
	metav1.TypeMeta `json:",inline"`

	// metadata is a standard object metadata
	// +optional
	metav1.ObjectMeta `json:"metadata,omitzero"`

	// spec defines the desired state of EventBusObservation
	// +required
	Spec EventBusObservationSpec `json:"spec"`

	// status defines the observed state of EventBusObservation
	// +optional
	Status EventBusObservationStatus `json:"status,omitzero"`
}

// +kubebuilder:object:root=true

// EventBusObservationList contains a list of EventBusObservation
type EventBusObservationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitzero"`
	Items           []EventBusObservation `json:"items"`
}

func init() {
	SchemeBuilder.Register(&EventBusObservation{}, &EventBusObservationList{})
}
