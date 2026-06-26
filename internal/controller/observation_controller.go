/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"reflect"
	"strconv"
	"strings"
	"time"

	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	natsv1alpha1 "github.com/eunh0112/EventBus-Failure/api/v1alpha1"
)

const observationRequeueInterval = 5 * time.Second

type promResponse struct {
	Status string `json:"status"`
	Error  string `json:"error,omitempty"`
	Data   struct {
		Result []promSample `json:"result"`
	} `json:"data"`
}

type promSample struct {
	Metric map[string]string `json:"metric"`
	Value  []interface{}     `json:"value"`
}

// ObservationReconciler reconciles a Observation object.
type ObservationReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=nats.failover.io,resources=observations,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=nats.failover.io,resources=observations/status,verbs=get;update;patch
// +kubebuilder:rbac:groups=nats.failover.io,resources=observations/finalizers,verbs=update
// +kubebuilder:rbac:groups=nats.failover.io,resources=natsprofiles,verbs=get;list;watch

func (r *ObservationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var obs natsv1alpha1.Observation
	if err := r.Get(ctx, req.NamespacedName, &obs); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	profileName := obs.Spec.ProfileName
	if profileName == "" {
		status := natsv1alpha1.ObservationStatus{
			Phase:       "Invalid",
			Reason:      "spec.profileName is empty",
			LastUpdated: metav1.Now(),
		}
		return r.updateObservationStatus(ctx, &obs, status)
	}

	var profile natsv1alpha1.NatsProfile
	if err := r.Get(ctx, client.ObjectKey{Name: profileName, Namespace: req.Namespace}, &profile); err != nil {
		return ctrl.Result{}, err
	}

	promURL := strings.TrimRight(os.Getenv("PROMETHEUS_URL"), "/")
	if promURL == "" {
		status := natsv1alpha1.ObservationStatus{
			Phase:        "Invalid",
			ActiveBroker: profile.Spec.ActiveBroker,
			Reason:       "PROMETHEUS_URL is not set",
			LastUpdated:  metav1.Now(),
		}
		return r.updateObservationStatus(ctx, &obs, status)
	}

	streamName := profile.Spec.StreamName
	if streamName == "" {
		streamName = "default"
	}

	activeBroker := profile.Spec.ActiveBroker
	activeDomain, ok := domainOf(profile.Spec.Brokers, activeBroker)
	if !ok {
		status := natsv1alpha1.ObservationStatus{
			Phase:        "Invalid",
			ActiveBroker: activeBroker,
			Reason:       fmt.Sprintf("active broker %q is not defined in NatsProfile brokers", activeBroker),
			LastUpdated:  metav1.Now(),
		}
		return r.updateObservationStatus(ctx, &obs, status)
	}

	activeSourceAPI := fmt.Sprintf("$JS.%s.API", activeDomain)

	brokerStatuses := make([]natsv1alpha1.BrokerObservation, 0, len(profile.Spec.Brokers))

	for _, broker := range profile.Spec.Brokers {
		role := "Standby"
		if broker.Name == activeBroker {
			role = "Active"
		}

		up, upFound, _, err := queryPromScalar(ctx, promURL, fmt.Sprintf(`up{job="nats-brokers",broker=%q}`, broker.Name))
		if err != nil {
			return ctrl.Result{}, err
		}

		jsDisabled, jsFound, _, err := queryPromScalar(ctx, promURL, fmt.Sprintf(`nats_server_jetstream_disabled{job="nats-brokers",broker=%q}`, broker.Name))
		if err != nil {
			return ctrl.Result{}, err
		}

		lastSeq, _, _, err := queryPromScalar(ctx, promURL, fmt.Sprintf(`nats_stream_last_seq{job="nats-brokers",broker=%q,stream_name=%q}`, broker.Name, streamName))
		if err != nil {
			return ctrl.Result{}, err
		}

		healthy := upFound && up == 1 && jsFound && jsDisabled == 0
		reason := ""
		if !upFound || up != 1 {
			reason = appendReason(reason, "prometheus target is down")
		}
		if !jsFound {
			reason = appendReason(reason, "jetstream metric is missing")
		} else if jsDisabled != 0 {
			reason = appendReason(reason, "jetstream is disabled")
		}

		var lag int64
		source := ""

		if role == "Standby" {
			lagValue, lagFound, labels, err := queryPromScalar(ctx, promURL,
				fmt.Sprintf(`nats_stream_source_lag{job="nats-brokers",broker=%q,stream_name=%q,source_api=%q}`,
					broker.Name, streamName, activeSourceAPI))
			if err != nil {
				return ctrl.Result{}, err
			}

			if lagFound {
				lag = int64(lagValue)
				source = labels["source_api"]
			} else {
				healthy = false
				reason = appendReason(reason, fmt.Sprintf("source lag metric for %s is missing", activeSourceAPI))
			}
		}

		brokerStatuses = append(brokerStatuses, natsv1alpha1.BrokerObservation{
			Name:         broker.Name,
			Role:         role,
			Healthy:      healthy,
			Source:       source,
			Lag:          lag,
			LastSequence: int64(lastSeq),
			Reason:       reason,
		})
	}

	activeHealthy := false
	for _, b := range brokerStatuses {
		if b.Name == activeBroker && b.Healthy {
			activeHealthy = true
			break
		}
	}

	selectedBroker := selectStandbyCandidate(brokerStatuses, profile.Spec.Thresholds.MaxLag)

	phase := "Healthy"
	reason := "active broker is healthy"
	if !activeHealthy {
		phase = "ActiveFailed"
		reason = fmt.Sprintf("active broker %s is unhealthy", activeBroker)
		if selectedBroker == "" {
			phase = "NoStandbyCandidate"
			reason = appendReason(reason, "no healthy standby candidate")
		}
	}

	status := natsv1alpha1.ObservationStatus{
		Phase:          phase,
		ActiveBroker:   activeBroker,
		SelectedBroker: selectedBroker,
		Brokers:        brokerStatuses,
		Reason:         reason,
		LastUpdated:    metav1.Now(),
	}

	log.Info("computed observation",
		"profile", profile.Name,
		"activeBroker", activeBroker,
		"selectedBroker", selectedBroker,
		"phase", phase,
	)

	return r.updateObservationStatus(ctx, &obs, status)
}

func (r *ObservationReconciler) updateObservationStatus(
	ctx context.Context,
	obs *natsv1alpha1.Observation,
	desired natsv1alpha1.ObservationStatus,
) (ctrl.Result, error) {
	if observationStatusEqualExceptLastUpdated(obs.Status, desired) {
		if !obs.Status.LastUpdated.IsZero() &&
			time.Since(obs.Status.LastUpdated.Time) < observationRequeueInterval {
			return ctrl.Result{RequeueAfter: observationRequeueInterval}, nil
		}
	}

	obs.Status = desired
	if err := r.Status().Update(ctx, obs); err != nil {
		return ctrl.Result{}, err
	}

	return ctrl.Result{RequeueAfter: observationRequeueInterval}, nil
}

func queryPromScalar(ctx context.Context, promURL string, query string) (float64, bool, map[string]string, error) {
	endpoint := fmt.Sprintf("%s/api/v1/query?query=%s", promURL, url.QueryEscape(query))

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
	if err != nil {
		return 0, false, nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return 0, false, nil, err
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return 0, false, nil, fmt.Errorf("prometheus query failed: status=%d query=%s", resp.StatusCode, query)
	}

	var parsed promResponse
	if err := json.NewDecoder(resp.Body).Decode(&parsed); err != nil {
		return 0, false, nil, err
	}

	if parsed.Status != "success" {
		return 0, false, nil, fmt.Errorf("prometheus query failed: %s query=%s", parsed.Error, query)
	}

	if len(parsed.Data.Result) == 0 {
		return 0, false, nil, nil
	}

	value, err := parsePromValue(parsed.Data.Result[0].Value)
	if err != nil {
		return 0, false, nil, err
	}

	return value, true, parsed.Data.Result[0].Metric, nil
}

func parsePromValue(value []interface{}) (float64, error) {
	if len(value) < 2 {
		return 0, fmt.Errorf("prometheus sample value is invalid")
	}

	switch v := value[1].(type) {
	case string:
		return strconv.ParseFloat(v, 64)
	case float64:
		return v, nil
	default:
		return 0, fmt.Errorf("unsupported prometheus sample value type %T", value[1])
	}
}

func domainOf(brokers []natsv1alpha1.NatsBroker, brokerName string) (string, bool) {
	for _, broker := range brokers {
		if broker.Name == brokerName {
			return broker.Domain, true
		}
	}
	return "", false
}

func selectStandbyCandidate(brokers []natsv1alpha1.BrokerObservation, maxLag int64) string {
	const maxInt64 = int64(9223372036854775807)

	best := ""
	bestLag := maxInt64

	for _, broker := range brokers {
		if broker.Role != "Standby" || !broker.Healthy {
			continue
		}
		if maxLag > 0 && broker.Lag > maxLag {
			continue
		}
		if broker.Lag < bestLag {
			best = broker.Name
			bestLag = broker.Lag
		}
	}

	return best
}

func appendReason(base string, msg string) string {
	if base == "" {
		return msg
	}
	return base + "; " + msg
}

func observationStatusEqualExceptLastUpdated(a, b natsv1alpha1.ObservationStatus) bool {
	return a.Phase == b.Phase &&
		a.ActiveBroker == b.ActiveBroker &&
		a.SelectedBroker == b.SelectedBroker &&
		a.Reason == b.Reason &&
		reflect.DeepEqual(a.Brokers, b.Brokers)
}

// SetupWithManager sets up the controller with the Manager.
func (r *ObservationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&natsv1alpha1.Observation{}).
		Named("observation").
		Complete(r)
}
