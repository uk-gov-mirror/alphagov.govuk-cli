package jobrequest

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"sort"
	"time"

	"charm.land/log/v2"
	jrv1 "github.com/alphagov/govuk-job-request-operator/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/watch"
)

// How many times to retry if a job comes back without a name. Each retry is
// delayed by n^2 seconds.
const jobNameRetries = 3

// wait for a JobRequest to enter an actionable state
func awaitJobRequest(c *JobRequestClient, jobRequestName string) (*jrv1.JobRequest, error) {
	i := c.InterfaceFor(JobRequestResourceName)
	w, err := i.Watch(c.ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.namespace=%s,metadata.name=%s", c.namespace, jobRequestName),
	})
	if err != nil {
		return nil, err
	}
	defer w.Stop()
	log.Debug("starting watch for JobRequest", "jobRequest", jobRequestName)
	// Wait for JobRequest to transition to an actionable state
	jobNameRetry := 1
	for {
		event := <-w.ResultChan()
		log.Debug("got watch event for jobrequest", "event", event)
		// Handle non-update events
		switch event.Type {
		case watch.Deleted:
			return nil, errors.New("job request deleted")
		case watch.Error:
			return nil, errors.New("received error from K8s watch API")
		}
		u, isOk := event.Object.(*unstructured.Unstructured)
		if !isOk {
			log.Error("error casting jobrequest event.Object", "object", event.Object)
			return nil, errors.New("failed to cast jobrequest event.Object to unstructured.Unstructured")
		}
		jr := &jrv1.JobRequest{}
		if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, jr); err != nil {
			return nil, err
		}
		switch jr.Status.State {
		case jrv1.JobRequestApproved, jrv1.JobRequestStarted, jrv1.JobRequestComplete, jrv1.JobRequestFailed:
			log.Debug(
				"job request state is actionable",
				"jr", jobRequestName,
				"state", jr.Status.State,
			)
			if jr.Status.JobName != "" {
				log.Debug("breaking JobRequest loop", "jobName", jr.Status.JobName)
				return jr, nil
			} else {
				if jobNameRetry == jobNameRetries+1 {
					return nil, fmt.Errorf("job request '%s' is in actionable state with no jobName", jobRequestName)
				} else {
					// Job does not have a name yet: retry with an exponential
					// backoff
					retryDuration := 1 << jobNameRetry
					log.Debug(
						"job request '%s' does not have a name: %d/%d in %ds",
						jobNameRetry,
						jobNameRetries,
						jobRequestName,
						retryDuration,
					)

					time.Sleep(time.Duration(retryDuration) * time.Second)
					jobNameRetry += 1
				}
			}
		case jrv1.JobRequestRejected:
			log.Debug("job request rejected", "jr", jobRequestName)
			return nil, fmt.Errorf("job request '%s' has been rejected", jobRequestName)
		case jrv1.JobRequestMalformed:
			log.Debug("job request malformed", "jr", jobRequestName)
			return nil, errors.New("malformed job request resource")
		case jrv1.JobRequestPending:
			log.Debug("job request pending", "jr", jobRequestName)
			continue
		}
	}
}

// wait for a Job to spawn pods
func awaitJob(c *JobRequestClient, jobName string) (*batchv1.Job, error) {
	w, err := c.clientSet.BatchV1().Jobs(c.namespace).Watch(c.ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.namespace=%s,metadata.name=%s", c.namespace, jobName),
	})
	if err != nil {
		return nil, err
	}
	defer w.Stop()
	log.Debug("starting watch for Job", "job", jobName)
	for {
		event := <-w.ResultChan()
		log.Debug("got watch event for job", "event", event)
		// Handle non-update events
		switch event.Type {
		case watch.Deleted:
			return nil, errors.New("job deleted")
		case watch.Error:
			return nil, errors.New("received error from K8s watch API")
		}
		j, isOk := event.Object.(*batchv1.Job)
		if !isOk {
			return nil, errors.New("failed to cast watched Job event.Object to  batchv1.Job")
		}
		// If the Job has a StartTime, it is either running or has finished
		if j.Status.StartTime != nil {
			log.Debug("job has start time", "job", jobName, "startTime", j.Status.StartTime)
			return j, nil
		}
		log.Debug("job does not have start time", "job", jobName)
	}
}

// wait for a Pod to become ready and have logs available for streaming
func awaitPod(c *JobRequestClient, jobName string) (*corev1.Pod, error) {
	// A Job can have created more than one Pod for the same jobName (e.g. retries
	// after a failure), all carrying the same job-name label. List first and pick
	// the newest one, then watch it specifically, so we don't race against events
	// from a stale, already-finished pod from an earlier attempt.
	pods, err := c.clientSet.CoreV1().Pods(c.namespace).List(c.ctx, metav1.ListOptions{
		LabelSelector: fmt.Sprintf("job-name=%s", jobName),
	})
	if err != nil {
		return nil, err
	}
	if len(pods.Items) == 0 {
		return nil, fmt.Errorf("no pods found for job '%s'", jobName)
	}
	sort.Slice(pods.Items, func(i, j int) bool {
		return pods.Items[i].CreationTimestamp.After(pods.Items[j].CreationTimestamp.Time)
	})
	podName := pods.Items[0].Name

	w, err := c.clientSet.CoreV1().Pods(c.namespace).Watch(c.ctx, metav1.ListOptions{
		FieldSelector: fmt.Sprintf("metadata.namespace=%s,metadata.name=%s", c.namespace, podName),
	})
	if err != nil {
		return nil, err
	}
	defer w.Stop()
	for {
		event := <-w.ResultChan()
		log.Debug("got watch event for pod", "event", event)
		// Handle non-update events
		switch event.Type {
		case watch.Deleted:
			return nil, errors.New("pod deleted")
		case watch.Error:
			return nil, errors.New("received error from K8s watch API")
		}
		p, isOk := event.Object.(*corev1.Pod)
		if !isOk {
			return nil, errors.New("failed to cast watched Pod event.Object to corev1.Pod")
		}
		// If the Pod is no longer Pending, its containers have started and its logs can be read
		if p.Status.Phase != corev1.PodPending {
			log.Debug("pod is no longer pending", "pod", p.Name, "phase", p.Status.Phase)
			return p, nil
		}
		log.Debug("pod is still pending", "pod", p.Name, "phase", p.Status.Phase)
	}
}

func streamLogs(c *JobRequestClient, podName string) error {
	logs := c.clientSet.CoreV1().Pods(c.namespace).GetLogs(
		podName,
		&corev1.PodLogOptions{Follow: true},
	)
	stream, err := logs.Stream(c.ctx)
	if err != nil {
		return err
	}
	defer stream.Close() //nolint:errcheck
	reader := bufio.NewReader(stream)
	for {
		line, err := reader.ReadString('\n')
		if err != nil {
			if errors.Is(err, io.EOF) {
				return nil
			}
			return err
		}
		print(line)
	}
}

func (c *JobRequestClient) FollowJobRequest(jobRequestName string) error {
	log.Info("Following JobRequest. Logs will stream once Job has started...", "jobRequestName", jobRequestName)
	jr, err := awaitJobRequest(c, jobRequestName)
	if err != nil {
		return err
	}

	_, err = awaitJob(c, jr.Status.JobName)
	if err != nil {
		return err
	}

	log.Info("Job created...", "job", jr.Status.JobName, "jobRequestState", jr.Status.State)

	pod, err := awaitPod(c, jr.Status.JobName)
	if err != nil {
		return err
	}

	log.Info("Tailing logs from pod...", "pod", pod.Name)
	err = streamLogs(c, pod.Name)

	return err
}
