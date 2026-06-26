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

type NatsBroker struct {
	Name     string `json:"name"`
	Domain   string `json:"domain"`
	Endpoint string `json:"endpoint"`
}

type GatewayRef struct {
	Namespace      string `json:"namespace"`
	ConfigMapName  string `json:"configMapName"`
	DeploymentName string `json:"deploymentName"`
}

type FailoverThresholds struct {
	ActiveFailureSeconds int32 `json:"activeFailureSeconds,omitempty"`
	MaxLag               int64 `json:"maxLag,omitempty"`
}

// EDIT THIS FILE!  THIS IS SCAFFOLDING FOR YOU TO OWN!
// NOTE: json tags are required.  Any new fields you add must have json tags for the fields to be serialized.

// NatsProfileSpec defines the desired state of NatsProfile
type NatsProfileSpec struct {
	StreamName   string `json:"streamName"`
	Subject      string `json:"subject"`
	ActiveBroker string `json:"activeBroker"`

	Brokers []NatsBroker `json:"brokers"`

	Gateway GatewayRef `json:"gateway"`

	Thresholds FailoverThresholds `json:"thresholds,omitempty"`
}

// NatsProfileStatus defines the observed state of NatsProfile.
type NatsProfileStatus struct {
	Phase   string `json:"phase,omitempty"`
	Message string `json:"message,omitempty"`
}

// +kubebuilder:object:root=true
// +kubebuilder:subresource:status

// NatsProfile is the Schema for the natsprofiles API
type NatsProfile struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec   NatsProfileSpec   `json:"spec,omitempty"`
	Status NatsProfileStatus `json:"status,omitempty"`
}

// +kubebuilder:object:root=true

// NatsProfileList contains a list of NatsProfile
type NatsProfileList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`
	Items           []NatsProfile `json:"items"`
}

func init() {
	SchemeBuilder.Register(&NatsProfile{}, &NatsProfileList{})
}
