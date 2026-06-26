package controller

import (
	"context"
	"fmt"
	"strings"
	"time"

	appsv1 "k8s.io/api/apps/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	logf "sigs.k8s.io/controller-runtime/pkg/log"

	natsv1alpha1 "github.com/eunh0112/EventBus-Failure/api/v1alpha1"
)

const (
	gatewayRestartAnnotation = "failover.nats.io/restartedAt"

	natsAuthSecretName = "nats-auth"
	natsUsernameKey    = "username"
	natsPasswordKey    = "password"
)

type DesignationReconciler struct {
	client.Client
	Scheme *runtime.Scheme
}

// +kubebuilder:rbac:groups=nats.failover.io,resources=observations,verbs=get;list;watch
// +kubebuilder:rbac:groups=nats.failover.io,resources=natsprofiles,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups="",resources=configmaps,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=apps,resources=deployments,verbs=get;list;watch;update;patch
// +kubebuilder:rbac:groups=batch,resources=jobs,verbs=get;list;watch;create;update;patch;delete
// +kubebuilder:rbac:groups=policy.karmada.io,resources=propagationpolicies,verbs=get;list;watch;create;update;patch;delete

func (r *DesignationReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	log := logf.FromContext(ctx)

	var obs natsv1alpha1.Observation
	if err := r.Get(ctx, req.NamespacedName, &obs); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if obs.Status.Phase != "ActiveFailed" {
		return ctrl.Result{}, nil
	}

	selectedBroker := obs.Status.SelectedBroker
	if selectedBroker == "" {
		log.Info("active broker failed, but no selected standby broker exists")
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}

	var profile natsv1alpha1.NatsProfile
	if err := r.Get(ctx, client.ObjectKey{Name: obs.Spec.ProfileName, Namespace: req.Namespace}, &profile); err != nil {
		return ctrl.Result{}, err
	}

	endpoint, ok := brokerEndpoint(profile.Spec.Brokers, selectedBroker)
	if !ok {
		return ctrl.Result{}, fmt.Errorf("selected broker %q is not defined in NatsProfile", selectedBroker)
	}

	gateway := profile.Spec.Gateway
	if gateway.Namespace == "" || gateway.ConfigMapName == "" || gateway.DeploymentName == "" {
		return ctrl.Result{}, fmt.Errorf("NatsProfile %s/%s has incomplete gateway reference", profile.Namespace, profile.Name)
	}

	streamName := profile.Spec.StreamName
	if streamName == "" {
		streamName = "default"
	}

	subject := profile.Spec.Subject
	if subject == "" {
		subject = "default.*.*"
	}

	jobName := promotionJobName(obs.Status.ActiveBroker, selectedBroker)

	if err := r.ensurePromotionJob(ctx, gateway.Namespace, jobName, selectedBroker, streamName, subject); err != nil {
		return ctrl.Result{}, err
	}

	if err := r.ensurePromotionPropagationPolicy(ctx, gateway.Namespace, jobName, selectedBroker); err != nil {
		return ctrl.Result{}, err
	}

	complete, failed, err := r.getPromotionJobState(ctx, gateway.Namespace, jobName)
	if err != nil {
		return ctrl.Result{}, err
	}
	if failed {
		return ctrl.Result{}, fmt.Errorf("promotion job %s/%s failed", gateway.Namespace, jobName)
	}
	if !complete {
		log.Info("waiting for promotion job to complete", "job", jobName, "selectedBroker", selectedBroker)
		return ctrl.Result{RequeueAfter: 3 * time.Second}, nil
	}

	configChanged, err := r.switchGateway(ctx, gateway.Namespace, gateway.ConfigMapName, gateway.DeploymentName, endpoint)
	if err != nil {
		return ctrl.Result{}, err
	}

	if profile.Spec.ActiveBroker != selectedBroker {
		profile.Spec.ActiveBroker = selectedBroker
		if err := r.Update(ctx, &profile); err != nil {
			return ctrl.Result{}, err
		}
	}

	log.Info("failover designation completed",
		"profile", profile.Name,
		"previousActiveBroker", obs.Status.ActiveBroker,
		"selectedBroker", selectedBroker,
		"endpoint", endpoint,
		"promotionJob", jobName,
		"configChanged", configChanged,
	)

	return ctrl.Result{}, nil
}

func (r *DesignationReconciler) ensurePromotionJob(ctx context.Context, namespace, jobName, broker, streamName, subject string) error {
	var existing batchv1.Job
	if err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: namespace}, &existing); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	backoffLimit := int32(0)

	job := &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      jobName,
			Namespace: namespace,
			Labels: map[string]string{
				"failover.nats.io/component": "promotion-job",
				"failover.nats.io/broker":    broker,
			},
		},
		Spec: batchv1.JobSpec{
			BackoffLimit: &backoffLimit,
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:    "nats-box",
							Image:   "natsio/nats-box:latest",
							Command: []string{"sh", "-lc", renderPromotionCommand(broker, streamName, subject)},
							Env: []corev1.EnvVar{
								{
									Name: "NATS_USERNAME",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{Name: natsAuthSecretName},
											Key:                  natsUsernameKey,
										},
									},
								},
								{
									Name: "NATS_PASSWORD",
									ValueFrom: &corev1.EnvVarSource{
										SecretKeyRef: &corev1.SecretKeySelector{
											LocalObjectReference: corev1.LocalObjectReference{Name: natsAuthSecretName},
											Key:                  natsPasswordKey,
										},
									},
								},
							},
							VolumeMounts: []corev1.VolumeMount{
								{
									Name:      "nats-tls",
									MountPath: "/etc/nats-certs",
									ReadOnly:  true,
								},
							},
						},
					},
					Volumes: []corev1.Volume{
						{
							Name: "nats-tls",
							VolumeSource: corev1.VolumeSource{
								Secret: &corev1.SecretVolumeSource{
									SecretName: "nats-" + broker + "-tls",
								},
							},
						},
					},
				},
			},
		},
	}

	return r.Create(ctx, job)
}

func (r *DesignationReconciler) ensurePromotionPropagationPolicy(ctx context.Context, namespace, jobName, targetCluster string) error {
	ppName := jobName + "-pp"

	pp := &unstructured.Unstructured{}
	pp.SetGroupVersionKind(schema.GroupVersionKind{
		Group:   "policy.karmada.io",
		Version: "v1alpha1",
		Kind:    "PropagationPolicy",
	})

	if err := r.Get(ctx, client.ObjectKey{Name: ppName, Namespace: namespace}, pp); err == nil {
		return nil
	} else if !apierrors.IsNotFound(err) {
		return err
	}

	pp.SetName(ppName)
	pp.SetNamespace(namespace)
	pp.Object["spec"] = map[string]interface{}{
		"resourceSelectors": []interface{}{
			map[string]interface{}{
				"apiVersion": "batch/v1",
				"kind":       "Job",
				"name":       jobName,
			},
		},
		"placement": map[string]interface{}{
			"clusterAffinity": map[string]interface{}{
				"clusterNames": []interface{}{targetCluster},
			},
		},
	}

	return r.Create(ctx, pp)
}

func (r *DesignationReconciler) getPromotionJobState(ctx context.Context, namespace, jobName string) (bool, bool, error) {
	var job batchv1.Job
	if err := r.Get(ctx, client.ObjectKey{Name: jobName, Namespace: namespace}, &job); err != nil {
		return false, false, err
	}

	for _, condition := range job.Status.Conditions {
		if condition.Type == batchv1.JobComplete && condition.Status == corev1.ConditionTrue {
			return true, false, nil
		}
		if condition.Type == batchv1.JobFailed && condition.Status == corev1.ConditionTrue {
			return false, true, nil
		}
	}

	return false, false, nil
}

func (r *DesignationReconciler) switchGateway(ctx context.Context, namespace, configMapName, deploymentName, endpoint string) (bool, error) {
	var cm corev1.ConfigMap
	if err := r.Get(ctx, client.ObjectKey{Name: configMapName, Namespace: namespace}, &cm); err != nil {
		return false, err
	}

	desiredConfig := renderGatewayHAProxyConfig(endpoint)

	configChanged := false
	if cm.Data == nil {
		cm.Data = map[string]string{}
	}
	if cm.Data["haproxy.cfg"] != desiredConfig {
		cm.Data["haproxy.cfg"] = desiredConfig
		if err := r.Update(ctx, &cm); err != nil {
			return false, err
		}
		configChanged = true
	}

	if configChanged {
		var deploy appsv1.Deployment
		if err := r.Get(ctx, client.ObjectKey{Name: deploymentName, Namespace: namespace}, &deploy); err != nil {
			return false, err
		}

		if deploy.Spec.Template.Annotations == nil {
			deploy.Spec.Template.Annotations = map[string]string{}
		}
		deploy.Spec.Template.Annotations[gatewayRestartAnnotation] = time.Now().UTC().Format(time.RFC3339)

		if err := r.Update(ctx, &deploy); err != nil {
			return false, err
		}
	}

	return configChanged, nil
}

func brokerEndpoint(brokers []natsv1alpha1.NatsBroker, name string) (string, bool) {
	for _, broker := range brokers {
		if broker.Name == name {
			return broker.Endpoint, true
		}
	}
	return "", false
}

func promotionJobName(activeBroker, selectedBroker string) string {
	return fmt.Sprintf("nats-promote-%s-to-%s", sanitizeName(activeBroker), sanitizeName(selectedBroker))
}

func sanitizeName(value string) string {
	value = strings.ToLower(value)
	value = strings.ReplaceAll(value, "_", "-")
	value = strings.ReplaceAll(value, ".", "-")
	return value
}

func renderPromotionCommand(broker, streamName, subject string) string {
	server := fmt.Sprintf("nats://nats-%s.nats-eventbus.svc.cluster.local:4222", broker)

	return fmt.Sprintf(`set -e

STREAM=%q
SUBJECT=%q

NATS_CMD="nats \
  --server %s \
  --tlsca /etc/nats-certs/ca.crt \
  --tlscert /etc/nats-certs/tls.crt \
  --tlskey /etc/nats-certs/tls.key \
  --user %s \
  --password %s"

echo "===== current stream info ====="
$NATS_CMD stream info "$STREAM" --json > /tmp/stream-info.json
cat /tmp/stream-info.json | jq '.config'

echo "===== generated promotion config ====="
cat /tmp/stream-info.json \
  | jq --arg subject "$SUBJECT" '.config | del(.sources) | .subjects = [$subject]' \
  > /tmp/promote-config.json

cat /tmp/promote-config.json

echo "===== applying promotion config ====="
$NATS_CMD stream edit "$STREAM" \
  --config /tmp/promote-config.json \
  --force \
  --no-interactive

echo "===== promoted stream info ====="
$NATS_CMD stream info "$STREAM"
`, streamName, subject, server)
}

func renderGatewayHAProxyConfig(endpoint string) string {
	return fmt.Sprintf(`global
  log stdout format raw local0

defaults
  log global
  mode tcp
  option tcplog
  timeout connect 5s
  timeout client 1m
  timeout server 1m

frontend nats_front
  bind *:4222
  default_backend active_nats

backend active_nats
  server active %s check
`, endpoint)
}

func (r *DesignationReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&natsv1alpha1.Observation{}).
		Named("designation").
		Complete(r)
}
