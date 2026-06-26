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

// ObservationSpec defines the desired state of Observation
type BrokerObservation struct {
	Name         string `json:"name"`
	Role         string `json:"role,omitempty"`
	Healthy      bool   `json:"healthy"`
	Source       string `json:"source,omitempty"`
	Lag          int64  `json:"lag,omitempty"`
	LastSequence int64  `json:"lastSequence,omitempty"`
	Reason       string `json:"reason,omitempty"`
}

type ObservationSpec struct {
	ProfileName string `json:"profileName"`
}

// ObservationStatus defines the observed state of Observation.
type ObservationStatus struct {
	Phase          string              `json:"phase,omitempty"`
	ActiveBroker   string              `json:"activeBroker,omitempty"`
	SelectedBroker string              `json:"selectedBroker,omitempty"`
	Brokers        []BrokerObservation `json:"brokers,omitempty"`
	Reason         string              `json:"reason,omitempty"`
	LastUpdated    metav1.Time         `json:"lastUpdated,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// Observation is the Schema for the observations API
type Observation struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   ObservationSpec   `json:"spec,omitempty"`
	Status ObservationStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// ObservationList contains a list of Observation
type ObservationList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []Observation `json:"items"`
}

func init() {
	SchemeBuilder.Register(&Observation{}, &ObservationList{})
}
