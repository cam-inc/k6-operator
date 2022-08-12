package controllers

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/go-logr/logr"
	"github.com/grafana/k6-operator/api/v1alpha1"
	"github.com/grafana/k6-operator/pkg/resources/jobs"
	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

func isServiceReady(log logr.Logger, service *v1.Service) bool {
	resp, err := http.Get(fmt.Sprintf("http://%v.%v.svc.cluster.local:6565/v1/status", service.ObjectMeta.Name, service.ObjectMeta.Namespace))

	if err != nil {
		log.Error(err, fmt.Sprintf("failed to get status from %v", service.ObjectMeta.Name))
		return false
	}

	return resp.StatusCode < 400
}

// StartJobs in the Ready phase using a curl container
func StartJobs(ctx context.Context, log logr.Logger, k6 *v1alpha1.K6, r *K6Reconciler) (ctrl.Result, error) {
	log.Info("Waiting for pods to get ready")

	selector := labels.SelectorFromSet(map[string]string{
		"app":    "k6",
		"k6_cr":  k6.Name,
		"runner": "true",
	})

	opts := &client.ListOptions{LabelSelector: selector, Namespace: k6.Namespace}
	pl := &v1.PodList{}
	if e := r.List(ctx, pl, opts); e != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, fmt.Errorf("could not list pods: %w", e)
	}

	var count int
	for _, pod := range pl.Items {
		if pod.Status.Phase != "Running" {
			continue
		}
		count++
	}

	log.Info(fmt.Sprintf("%d/%d pods ready", count, k6.Spec.Parallelism))

	if count != int(k6.Spec.Parallelism) {
		log.Info(fmt.Sprintf("pods %v/%v is not ready", k6.Spec.Parallelism-int32(count), k6.Spec.Parallelism))
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, nil
	}

	var hostnames []string

	sl := &v1.ServiceList{}

	if e := r.List(ctx, sl, opts); e != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, fmt.Errorf("could not list services: %w", e)
	}

	for _, service := range sl.Items {
		hostnames = append(hostnames, service.Spec.ClusterIP)

		if !isServiceReady(log, &service) {
			log.Info(fmt.Sprintf("%v service is not ready, aborting", service.ObjectMeta.Name))
			return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, nil
		}
	}

	starter := jobs.NewStarterJob(k6, hostnames)

	if err := ctrl.SetControllerReference(k6, starter, r.Scheme); err != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, errors.New("failed to set controller reference for the start job")
	}

	if err := r.Create(ctx, starter); err != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, errors.New("failed to launch k6 test starter")
	}

	log.Info("Changing stage of K6 status to started")
	k6.Status.Stage = "started"
	if err := r.Client.Status().Update(ctx, k6); err != nil {
		// do not requeue
		return ctrl.Result{}, fmt.Errorf("could not update status of custom resource: %w", err)
	}

	return ctrl.Result{}, nil
}
