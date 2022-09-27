package controllers

import (
	"context"
	"fmt"
	"time"

	"github.com/go-logr/logr"
	"github.com/grafana/k6-operator/api/v1alpha1"
	"github.com/grafana/k6-operator/pkg/resources/jobs"
	"github.com/grafana/k6-operator/pkg/types"
	ctrl "sigs.k8s.io/controller-runtime"
)

// InitializeJobs creates jobs that will run initial checks for distributed test if any are necessary
func InitializeJobs(ctx context.Context, log logr.Logger, k6 *v1alpha1.K6, r *K6Reconciler) (ctrl.Result, error) {
	log.Info("Initialize test")

	log.Info("Changing stage of K6 status to initialization")
	k6.Status.Stage = "initialization"
	if err := r.Client.Status().Update(ctx, k6); err != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, fmt.Errorf("could not update status of custom resource: %w", err)
	}

	cli := types.ParseCLI(&k6.Spec)

	initializer, err := jobs.NewInitializerJob(k6, cli.ArchiveArgs)
	if err != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, fmt.Errorf("could not build the initializer job: %w", err)
	}

	log.Info(fmt.Sprintf("Initializer job is ready to start with image `%s` and command `%s`",
		initializer.Spec.Template.Spec.Containers[0].Image, initializer.Spec.Template.Spec.Containers[0].Command))

	if err = ctrl.SetControllerReference(k6, initializer, r.Scheme); err != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, fmt.Errorf("failed to set controller reference for the initialize job: %w", err)
	}

	if err = r.Create(ctx, initializer); err != nil {
		return ctrl.Result{Requeue: true, RequeueAfter: 5 * time.Second}, fmt.Errorf("failed to launch k6 test initializer: %w", err)
	}

	log.Info("Changing stage of K6 status to wait initialization")
	k6.Status.Stage = "wait initialization"
	if err = r.Client.Status().Update(ctx, k6); err != nil {
		// do not requeue
		return ctrl.Result{}, fmt.Errorf("could not update status of custom resource: %w", err)
	}

	return ctrl.Result{}, nil
}
