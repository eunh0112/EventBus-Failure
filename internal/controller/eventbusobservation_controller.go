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

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"time"

	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	failoverv1alpha1 "github.com/eunho/eventbus-failover-controller/api/v1alpha1"
)

// EventBusObservationReconciler reconciles a EventBusObservation object
type EventBusObservationReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=failover.scheduler.dev,resources=eventbusobservations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=failover.scheduler.dev,resources=eventbusobservations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=failover.scheduler.dev,resources=eventbusobservations/finalizers,verbs=update

// Reconcile is part of the main kubernetes reconciliation loop which aims to
// move the current state of the cluster closer to the desired state.
// TODO(user): Modify the Reconcile function to compare the state specified by
// the EventBusObservation object against the actual cluster state, and then
// perform operations to make the cluster state reflect the state specified by
// the user.
//
// For more details, check Reconcile and its Result here:
// - https://pkg.go.dev/sigs.k8s.io/controller-runtime@v0.23.3/pkg/reconcile
func (r *EventBusObservationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx)

	var obs failoverv1alpha1.EventBusObservation
	if err := r.Get(ctx, req.NamespacedName, &obs); err != nil {
		return ctrl.Result{}, client.IgnoreNotFound(err)
	}

	seqMap, err := queryLastSeq(obs.Spec.PrometheusURL, obs.Spec.StreamName)
	if err != nil {
		logger.Error(err, "[Observation] failed to query Prometheus")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	primarySeq := seqMap[obs.Spec.Primary.Cluster]

	primaryStatus := failoverv1alpha1.EventBusObservedStatus{
		Cluster:  obs.Spec.Primary.Cluster,
		Endpoint: obs.Spec.Primary.Endpoint,
		Phase:    "Healthy",
		LastSeq:  primarySeq,
	}

	var selected *failoverv1alpha1.SelectedStandby
	var candidateStatuses []failoverv1alpha1.EventBusObservedStatus

	minGap := int64(1<<63 - 1)

	for _, c := range obs.Spec.Candidates {
		lastSeq := seqMap[c.Cluster]
		gap := primarySeq - lastSeq
		if gap < 0 {
			gap = 0
		}

		status := failoverv1alpha1.EventBusObservedStatus{
			Cluster:        c.Cluster,
			Endpoint:       c.Endpoint,
			Phase:          "Healthy",
			LastSeq:        lastSeq,
			ReplicationGap: gap,
		}
		candidateStatuses = append(candidateStatuses, status)

		if gap < minGap {
			minGap = gap
			selected = &failoverv1alpha1.SelectedStandby{
				Cluster:  c.Cluster,
				Endpoint: c.Endpoint,
				Reason:   "MinReplicationGap",
			}
		}
	}

	obs.Status.PrimaryStatus = primaryStatus
	obs.Status.CandidateStatuses = candidateStatuses
	obs.Status.SelectedStandby = selected
	obs.Status.FailoverRequired = obs.Spec.FailoverRequired

	if err := r.Status().Update(ctx, &obs); err != nil {
		logger.Error(err, "[Observation] failed to update Observation.status")
		return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
	}

	if selected != nil {
		logger.Info("[Observation] selectedStandby=" + selected.Cluster + " reason=" + selected.Reason)
	}
	logger.Info("[Observation] update Observation.status succeeded")

	if obs.Status.FailoverRequired && selected != nil {
		logger.Info("[Designation] failoverRequired=true")
		logger.Info("[Designation] read selectedStandby=" + selected.Cluster + " endpoint=" + selected.Endpoint)

		changed, err := r.patchEventBusURL(ctx, obs.Spec.EventBusRef.Namespace, obs.Spec.EventBusRef.Name, selected.Endpoint)
		if err != nil {
			logger.Error(err, "[Designation] failed to patch EventBus")
			return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
		}

		if changed {
			logger.Info("[Designation] patch EventBus/" + obs.Spec.EventBusRef.Name + " spec.jetstreamExotic.url=" + selected.Endpoint)
		} else {
			logger.Info("[Designation] EventBus/" + obs.Spec.EventBusRef.Name + " already uses selected endpoint")
		}
	}

	return ctrl.Result{RequeueAfter: 10 * time.Second}, nil
}

type prometheusResponse struct {
	Status string `json:"status"`
	Data   struct {
		Result []struct {
			Metric map[string]string `json:"metric"`
			Value  []interface{}     `json:"value"`
		} `json:"result"`
	} `json:"data"`
}

func queryLastSeq(promURL string, streamName string) (map[string]int64, error) {
	query := fmt.Sprintf(`nats_stream_last_seq{job="nats-eventbus-ha",stream_name="%s"}`, streamName)
	url := fmt.Sprintf("%s/api/v1/query?query=%s", promURL, query)

	resp, err := http.Get(url)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	var pr prometheusResponse
	if err := json.Unmarshal(body, &pr); err != nil {
		return nil, err
	}

	result := make(map[string]int64)

	for _, item := range pr.Data.Result {
		cluster := item.Metric["cluster"]
		if cluster == "" {
			continue
		}
		if len(item.Value) < 2 {
			continue
		}

		valueStr, ok := item.Value[1].(string)
		if !ok {
			continue
		}

		valueFloat, err := strconv.ParseFloat(valueStr, 64)
		if err != nil {
			continue
		}

		result[cluster] = int64(valueFloat)
	}

	return result, nil
}
func (r *EventBusObservationReconciler) patchEventBusURL(ctx context.Context, namespace, name, desiredURL string) (bool, error) {
	eventBus := &unstructured.Unstructured{}
	eventBus.SetAPIVersion("argoproj.io/v1alpha1")
	eventBus.SetKind("EventBus")

	if err := r.Get(ctx, client.ObjectKey{Namespace: namespace, Name: name}, eventBus); err != nil {
		return false, err
	}

	currentURL, _, _ := unstructured.NestedString(eventBus.Object, "spec", "jetstreamExotic", "url")
	if currentURL == desiredURL {
		return false, nil
	}

	if err := unstructured.SetNestedField(eventBus.Object, desiredURL, "spec", "jetstreamExotic", "url"); err != nil {
		return false, err
	}

	if err := r.Update(ctx, eventBus); err != nil {
		return false, err
	}

	return true, nil
}

// SetupWithManager sets up the controller with the Manager.
func (r *EventBusObservationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&failoverv1alpha1.EventBusObservation{}).
		Named("eventbusobservation").
		Complete(r)
}
