package controllers

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/grafana/k6-operator/api/v1alpha1"
	"github.com/grafana/k6-operator/pkg/cloud"
	"github.com/grafana/k6-operator/pkg/types"
	batchv1 "k8s.io/api/batch/v1"
	"k8s.io/apimachinery/pkg/labels"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// FinishJobs waits for the pods to finish, performs finishing call for cloud output and moves state to "finished".
func FinishJobs(ctx context.Context, log logr.Logger, k6 *v1alpha1.K6, r *K6Reconciler) (ctrl.Result, error) {
	log.Info("Waiting for pods to finish")

	selector := labels.SelectorFromSet(map[string]string{
		"app":    "k6",
		"k6_cr":  k6.Name,
		"runner": "true",
	})

	opts := &client.ListOptions{LabelSelector: selector, Namespace: k6.Namespace}
	jl := &batchv1.JobList{}

	if err := r.List(ctx, jl, opts); err != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, fmt.Errorf("could not list jobs: %w", err)
	}

	if len(jl.Items) < 1 {
		return ctrl.Result{}, errors.New("jobs is not found")
	}

	// TODO: We should distinguish between Succeeded/Failed/Unknown
	var finished int32
	for _, job := range jl.Items {
		if job.Status.Active != 0 {
			continue
		}
		finished++
	}

	log.Info(fmt.Sprintf("%d/%d jobs complete", finished, k6.Spec.Parallelism))

	if finished < k6.Spec.Parallelism {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, nil
	}

	// If this is a test run with cloud output, try to finalize it regardless.
	if cli := types.ParseCLI(&k6.Spec); cli.HasCloudOut {
		if err := cloud.FinishTestRun(testRunId); err != nil {
			log.Error(err, "Could not finalize the test run with cloud output")
		} else {
			log.Info(fmt.Sprintf("Cloud test run %s was finalized successfully", testRunId))
		}
	}

	log.Info("Changing stage of K6 status to finished")
	k6.Status.Stage = "finished"
	if err := r.Client.Status().Update(ctx, k6); err != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, fmt.Errorf("could not update status of custom resource: %w", err)
	}

	return ctrl.Result{}, nil
}
