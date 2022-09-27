package controllers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/go-logr/logr"
	"github.com/grafana/k6-operator/api/v1alpha1"
	"github.com/grafana/k6-operator/pkg/cloud"
	"github.com/grafana/k6-operator/pkg/types"
	corev1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// k6 Cloud related vars
// Right now operator works with one test at a time so these should be safe.
var (
	testRunId     string
	token         string
	inspectOutput cloud.InspectOutput
)

// WaitInitializationJobs waits created initialization jobs were finished successfully
func WaitInitializationJobs(ctx context.Context, log logr.Logger, k6 *v1alpha1.K6, r *K6Reconciler) (ctrl.Result, error) {
	log.Info("Waiting for initialization jobs to finish")

	var (
		listOpts = &client.ListOptions{
			Namespace: k6.Namespace,
			LabelSelector: labels.SelectorFromSet(map[string]string{
				"app":      "k6",
				"k6_cr":    k6.Name,
				"job-name": fmt.Sprintf("%s-initializer", k6.Name),
			}),
		}
		podList = &corev1.PodList{}
	)

	if err := r.List(ctx, podList, listOpts); err != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, fmt.Errorf("could not list pods: %w", err)
	}

	if len(podList.Items) < 1 {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, errors.New("no initializing pod found yet")
	}

	// there should be only 1 initializer pod
	if podList.Items[0].Status.Phase != "Succeeded" {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, errors.New("waiting for initializing pod to finish")
	}

	// Here we need to get the output of the pod
	// pods/log is not currently supported by controller-runtime client and it is officially
	// recommended to use REST client instead:
	// https://github.com/kubernetes-sigs/controller-runtime/issues/1229

	var opts corev1.PodLogOptions
	config, err := rest.InClusterConfig()
	if err != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, fmt.Errorf("unable to fetch in-cluster REST config: %w", err)
	}

	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, fmt.Errorf("unable to get access to clientset: %w", err)
	}

	req := clientset.CoreV1().Pods(k6.Namespace).GetLogs(podList.Items[0].Name, &opts)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second*60)
	defer cancel()

	podLogs, err := req.Stream(ctx)
	if err != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, fmt.Errorf("unable to stream logs from the pod: %w", err)
	}
	defer podLogs.Close()

	buf := new(bytes.Buffer)
	_, err = io.Copy(buf, podLogs)
	if err != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, fmt.Errorf("unable to copy logs from the pod: %w", err)
	}

	if err := json.Unmarshal(buf.Bytes(), &inspectOutput); err != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, fmt.Errorf("unable to marshal: %w", err)
	}

	log.Info(fmt.Sprintf("k6 inspect: %+v", inspectOutput))

	if int32(inspectOutput.MaxVUs) < k6.Spec.Parallelism {
		// TODO maybe change this to a warning and simply set parallelism = maxVUs and proceed with execution?
		// But logr doesn't seem to have warning level by default, only with V() method...
		// It makes sense to return to this after / during logr VS logrus issue https://github.com/grafana/k6-operator/issues/84
		return ctrl.Result{}, fmt.Errorf("parallelism argument cannot be larger than maximum VUs in the script: parallelism %v, maxVUs %v", k6.Spec.Parallelism, inspectOutput.MaxVUs)
	}

	cli := types.ParseCLI(&k6.Spec)
	if cli.HasCloudOut {
		var (
			secrets    corev1.SecretList
			secretOpts = &client.ListOptions{
				// TODO: find out a better way to get namespace here
				Namespace: "k6-operator-system",
				LabelSelector: labels.SelectorFromSet(map[string]string{
					"k6cloud": "token",
				}),
			}
		)
		if err := r.List(ctx, &secrets, secretOpts); err != nil {
			return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, fmt.Errorf("failed to load k6 Cloud token: %w", err)
		}

		if len(secrets.Items) < 1 {
			return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, errors.New("there are no secrets to hold k6 Cloud token")
		}

		if t, ok := secrets.Items[0].Data["token"]; !ok {
			return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, errors.New("the secret doesn't have a field token for k6 Cloud")
		} else {
			token = string(t)
		}
		log.Info("Token for k6 Cloud was loaded.")

		host := getEnvVar(k6.Spec.Runner.Env, "K6_CLOUD_HOST")

		if refID, err := cloud.CreateTestRun(inspectOutput, k6.Spec.Parallelism, host, token, log); err != nil {
			return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, errors.New("the secret doesn't have a field token for k6 Cloud")
		} else {
			testRunId = refID
			log.Info(fmt.Sprintf("Created cloud test run: %s", testRunId))
		}
	}

	log.Info("Changing stage of K6 status to initialized")
	k6.Status.Stage = "initialized"
	if err = r.Client.Status().Update(ctx, k6); err != nil {
		// Do not requeue
		return ctrl.Result{}, errors.New("the secret doesn't have a field token for k6 Cloud")
	}

	return ctrl.Result{}, nil
}

func getEnvVar(vars []corev1.EnvVar, name string) string {
	for _, v := range vars {
		if v.Name == name {
			return v.Value
		}
	}
	return ""
}
