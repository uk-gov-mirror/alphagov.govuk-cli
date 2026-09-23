package integration_tests

import (
	"os"
	"path/filepath"

	jrv1 "github.com/alphagov/govuk-job-request-operator/api/v1"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gbytes"
	"github.com/onsi/gomega/gexec"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// suspendedJob returns a minimal valid Job fixture. The Job is suspended so
// the cluster's job controller never starts it and it stays without a
// StartTime, keeping the CLI's Job watch waiting.
func suspendedJob(name string, namespace string) *batchv1.Job {
	return &batchv1.Job{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
		},
		Spec: batchv1.JobSpec{
			Suspend: new(true),
			Template: corev1.PodTemplateSpec{
				Spec: corev1.PodSpec{
					RestartPolicy: corev1.RestartPolicyNever,
					Containers: []corev1.Container{
						{
							Name:  "app",
							Image: "publishing-api:latest",
						},
					},
				},
			},
		},
	}
}

// pendingPod returns a Pod carrying the job-name label the CLI uses to find a
// Job's pods. It has no node assigned, so nothing in the cluster moves it out
// of the Pending phase.
func pendingPod(name string, namespace string, jobName string) *corev1.Pod {
	return &corev1.Pod{
		ObjectMeta: metav1.ObjectMeta{
			Name:      name,
			Namespace: namespace,
			Labels: map[string]string{
				"job-name": jobName,
			},
		},
		Spec: corev1.PodSpec{
			RestartPolicy: corev1.RestartPolicyNever,
			Containers: []corev1.Container{
				{
					Name:  "app",
					Image: "publishing-api:latest",
				},
			},
		},
	}
}

// kwokNode returns a fake Node managed by the kwok controller. Pods assigned
// to it are moved to Running by kwok's built-in stages, and their logs are
// served by kwok's fake kubelet. The taint stops the scheduler placing other
// tests' pods on it.
func kwokNode(name string) *corev1.Node {
	return &corev1.Node{
		ObjectMeta: metav1.ObjectMeta{
			Name: name,
			Annotations: map[string]string{
				"kwok.x-k8s.io/node":           "fake",
				"node.alpha.kubernetes.io/ttl": "0",
			},
			Labels: map[string]string{
				"type": "kwok",
			},
		},
		Spec: corev1.NodeSpec{
			Taints: []corev1.Taint{
				{
					Key:    "kwok.x-k8s.io/node",
					Value:  "fake",
					Effect: corev1.TaintEffectNoSchedule,
				},
			},
		},
	}
}

var _ = Describe("jobrequest get --follow", func() {
	Context("when the JobRequest does not yet have a name", func() {
		// we can't create a job without a name, so set it then clear it
		const jobRequestName = "temp-name"
		const namespace = "apps"

		BeforeEach(func(ctx SpecContext) {
			err := SwitchToKubernetesUser(JobRequesterUser)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			err := SwitchToKubernetesAdminUser()
			Expect(err).NotTo(HaveOccurred())
		})

		FIt("retries until there is a name", func(ctx SpecContext) {
			// Create a job which is Approved but has no name
			jr := pendingJobRequest(
				jobRequestName,
				namespace,
				JobRequesterUser.ARN,
			)
			jr.Status.State = jrv1.JobRequestApproved
			jr.Status.JobName = ""
			Expect(createJobRequest(ctx, jr)).To(Succeed())

			err := SwitchToKubernetesUser(JobRequesterUser)
			Expect(err).NotTo(HaveOccurred())

			// Start following
			cmd, err := cliCmd(ctx,
				"jobrequest", "get", jobRequestName,
				"--follow",
				"--log-level", "debug",
				"--kubeconfig", kubeconfigPath,
				"--namespace", namespace)
			Expect(err).NotTo(HaveOccurred())

			session, err := gexec.Start(cmd, GinkgoWriter, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())

			DeferCleanup(func() {
				session.Kill().Wait()
			})

			// wait 'til we start watching
			Eventually(session.Err, "5s").Should(gbytes.Say("starting watch for JobRequest"))

			// --- Pre-ticket, incorrect, behaviour
			// Eventually(session).Should(gexec.Exit(1))
			// Expect(string(session.Err.Contents())).To(
			// 	ContainSubstring("is in actionable state with no jobName")
			// )
			// ---

			// set a job name
			err = SwitchToKubernetesAdminUser()
			Expect(err).NotTo(HaveOccurred())

			jobName := "job-" + jobRequestName
			jr.Status.JobName = jobName
			Expect(updateJobRequestStatus(ctx, jr)).To(Succeed())

			// make a job with that name
			job := &batchv1.Job{
				ObjectMeta: metav1.ObjectMeta{
					Name:      jobName,
					Namespace: namespace,
				},
				Spec: batchv1.JobSpec{
					Suspend: new(false),
					Template: corev1.PodTemplateSpec{
						Spec: corev1.PodSpec{
							RestartPolicy: corev1.RestartPolicyNever,
							Containers: []corev1.Container{
								{
									Name:  "app",
									Image: "publishing-api:latest",
								},
							},
						},
					},
				},
			}
			Expect(createJob(ctx, job)).To(Succeed())

			Eventually(session.Err, "10s").Should(gbytes.Say("job request state is actionable"))
			Eventually(session.Err, "10s").Should(gbytes.Say("starting watch for Job"))
		})
	})
})

var _ = Describe("jobrequest get --follow", func() {
	Context("when the JobRequest is deleted while being followed", func() {
		const jobRequestName = "follow-then-deleted"
		const namespace = "apps"

		BeforeEach(func(ctx SpecContext) {
			err := SwitchToKubernetesUser(JobRequesterUser)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			err := SwitchToKubernetesAdminUser()
			Expect(err).NotTo(HaveOccurred())
		})

		It("exits with a job request deleted error", func(ctx SpecContext) {
			jr := pendingJobRequest(jobRequestName, namespace, JobRequesterUser.ARN)
			Expect(createJobRequest(ctx, jr)).To(Succeed())

			cmd, err := cliCmd(ctx, "jobrequest", "get", jobRequestName, "--follow", "--log-level", "debug", "--kubeconfig", kubeconfigPath, "--namespace", namespace)
			Expect(err).NotTo(HaveOccurred())

			session, err := gexec.Start(cmd, GinkgoWriter, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				session.Kill().Wait()
			})

			// The Deleted watch event is only delivered if the JobRequest is
			// deleted after the CLI's watch is established, so wait for the
			// initial Added event to be logged before deleting.
			Eventually(session.Err, "10s").Should(gbytes.Say("got watch event for jobrequest"))

			Expect(deleteJobRequest(ctx, jr)).To(Succeed())

			Eventually(session, "10s").Should(gexec.Exit(1))
			Expect(session.Err).To(gbytes.Say("job request deleted"))
		})
	})

	Context("when the JobRequest is in Rejected state", func() {
		const jobRequestName = "follow-rejected"
		const namespace = "apps"

		BeforeEach(func(ctx SpecContext) {
			err := SwitchToKubernetesUser(JobRequesterUser)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			err := SwitchToKubernetesAdminUser()
			Expect(err).NotTo(HaveOccurred())
		})

		It("exits with a rejected error", func(ctx SpecContext) {
			jr := pendingJobRequest(jobRequestName, namespace, JobRequesterUser.ARN)
			jr.Status.State = jrv1.JobRequestRejected
			Expect(createJobRequest(ctx, jr)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(deleteJobRequest(ctx, jr)).To(Succeed())
			})

			cmd, err := cliCmd(ctx, "jobrequest", "get", jobRequestName, "--follow", "--kubeconfig", kubeconfigPath, "--namespace", namespace)
			Expect(err).NotTo(HaveOccurred())

			output, err := cmd.CombinedOutput()
			Expect(err).To(MatchError("exit status 1"))

			Expect(string(output)).To(ContainSubstring("job request '" + jobRequestName + "' has been rejected"))
		})
	})

	Context("when the JobRequest is in Malformed state", func() {
		const jobRequestName = "follow-malformed"
		const namespace = "apps"

		BeforeEach(func(ctx SpecContext) {
			err := SwitchToKubernetesUser(JobRequesterUser)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			err := SwitchToKubernetesAdminUser()
			Expect(err).NotTo(HaveOccurred())
		})

		It("exits with a malformed error", func(ctx SpecContext) {
			jr := pendingJobRequest(jobRequestName, namespace, JobRequesterUser.ARN)
			jr.Status.State = jrv1.JobRequestMalformed
			Expect(createJobRequest(ctx, jr)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(deleteJobRequest(ctx, jr)).To(Succeed())
			})

			cmd, err := cliCmd(ctx, "jobrequest", "get", jobRequestName, "--follow", "--kubeconfig", kubeconfigPath, "--namespace", namespace)
			Expect(err).NotTo(HaveOccurred())

			output, err := cmd.CombinedOutput()
			Expect(err).To(MatchError("exit status 1"))

			Expect(string(output)).To(ContainSubstring("malformed job request resource"))
		})
	})

	Context("when the JobRequest is in an actionable state", func() {
		const jobRequestName = "follow-actionable"
		const jobName = "follow-actionable-x7k2p"
		const namespace = "apps"

		BeforeEach(func(ctx SpecContext) {
			err := SwitchToKubernetesUser(JobRequesterUser)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			err := SwitchToKubernetesAdminUser()
			Expect(err).NotTo(HaveOccurred())
		})

		It("detects the actionable state and starts watching the Job", func(ctx SpecContext) {
			jr := pendingJobRequest(jobRequestName, namespace, JobRequesterUser.ARN)
			jr.Status.State = jrv1.JobRequestApproved
			jr.Status.JobName = jobName
			Expect(createJobRequest(ctx, jr)).To(Succeed())

			cmd, err := cliCmd(ctx, "jobrequest", "get", jobRequestName, "--follow", "--log-level", "debug", "--kubeconfig", kubeconfigPath, "--namespace", namespace)
			Expect(err).NotTo(HaveOccurred())

			session, err := gexec.Start(cmd, GinkgoWriter, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				session.Kill().Wait()
			})

			Eventually(session.Err, "10s").Should(gbytes.Say("job request state is actionable"))
			Eventually(session.Err, "10s").Should(gbytes.Say("starting watch for Job"))
		})
	})

	Context("when the Job is deleted while being followed", func() {
		const jobRequestName = "follow-job-deleted"
		const jobName = "follow-job-deleted-x7k2p"
		const namespace = "apps"

		BeforeEach(func(ctx SpecContext) {
			err := SwitchToKubernetesUser(JobRequesterUser)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			err := SwitchToKubernetesAdminUser()
			Expect(err).NotTo(HaveOccurred())
		})

		It("exits with a job deleted error", func(ctx SpecContext) {
			jr := pendingJobRequest(jobRequestName, namespace, JobRequesterUser.ARN)
			jr.Status.State = jrv1.JobRequestStarted
			jr.Status.JobName = jobName
			Expect(createJobRequest(ctx, jr)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(deleteJobRequest(ctx, jr)).To(Succeed())
			})

			job := suspendedJob(jobName, namespace)
			Expect(createJob(ctx, job)).To(Succeed())

			cmd, err := cliCmd(ctx, "jobrequest", "get", jobRequestName, "--follow", "--log-level", "debug", "--kubeconfig", kubeconfigPath, "--namespace", namespace)
			Expect(err).NotTo(HaveOccurred())

			session, err := gexec.Start(cmd, GinkgoWriter, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				session.Kill().Wait()
			})

			// The Deleted watch event is only delivered if the Job is deleted
			// after the CLI's Job watch is established, so wait for the
			// initial Added event to be logged before deleting. The trailing
			// "event=" avoids matching the "got watch event for jobrequest"
			// log line, which shares the prefix.
			Eventually(session.Err, "10s").Should(gbytes.Say(`got watch event for job event=`))

			Expect(deleteJob(ctx, job)).To(Succeed())

			Eventually(session, "10s").Should(gexec.Exit(1))
			Expect(session.Err).To(gbytes.Say("job deleted"))
		})
	})

	Context("when the Job gains a start time while being followed", func() {
		const jobRequestName = "follow-job-starts"
		const jobName = "follow-job-starts-x7k2p"
		const namespace = "apps"

		BeforeEach(func(ctx SpecContext) {
			err := SwitchToKubernetesUser(JobRequesterUser)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			err := SwitchToKubernetesAdminUser()
			Expect(err).NotTo(HaveOccurred())
		})

		It("finishes the Job watch loop", func(ctx SpecContext) {
			jr := pendingJobRequest(jobRequestName, namespace, JobRequesterUser.ARN)
			jr.Status.State = jrv1.JobRequestStarted
			jr.Status.JobName = jobName
			Expect(createJobRequest(ctx, jr)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(deleteJobRequest(ctx, jr)).To(Succeed())
			})

			job := suspendedJob(jobName, namespace)
			Expect(createJob(ctx, job)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(deleteJob(ctx, job)).To(Succeed())
			})

			cmd, err := cliCmd(ctx, "jobrequest", "get", jobRequestName, "--follow", "--log-level", "debug", "--kubeconfig", kubeconfigPath, "--namespace", namespace)
			Expect(err).NotTo(HaveOccurred())

			session, err := gexec.Start(cmd, GinkgoWriter, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				session.Kill().Wait()
			})

			// The suspended Job has no StartTime, so the CLI's Job watch keeps
			// waiting. Wait for the watch to be established before setting the
			// start time, so the update event is delivered to the CLI.
			Eventually(session.Err, "10s").Should(gbytes.Say("job does not have start time"))

			Expect(setJobStartTime(ctx, job)).To(Succeed())

			Eventually(session.Err, "10s").Should(gbytes.Say("job has start time"))
			Eventually(session.Err, "10s").Should(gbytes.Say("Job created"))
		})
	})

	Context("when the started Job has no pods", func() {
		const jobRequestName = "follow-no-pods"
		const jobName = "follow-no-pods-x7k2p"
		const namespace = "apps"

		BeforeEach(func(ctx SpecContext) {
			err := SwitchToKubernetesUser(JobRequesterUser)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			err := SwitchToKubernetesAdminUser()
			Expect(err).NotTo(HaveOccurred())
		})

		It("exits with a no pods found error", func(ctx SpecContext) {
			jr := pendingJobRequest(jobRequestName, namespace, JobRequesterUser.ARN)
			jr.Status.State = jrv1.JobRequestStarted
			jr.Status.JobName = jobName
			Expect(createJobRequest(ctx, jr)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(deleteJobRequest(ctx, jr)).To(Succeed())
			})

			job := suspendedJob(jobName, namespace)
			Expect(createJob(ctx, job)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(deleteJob(ctx, job)).To(Succeed())
			})

			cmd, err := cliCmd(ctx, "jobrequest", "get", jobRequestName, "--follow", "--log-level", "debug", "--kubeconfig", kubeconfigPath, "--namespace", namespace)
			Expect(err).NotTo(HaveOccurred())

			session, err := gexec.Start(cmd, GinkgoWriter, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				session.Kill().Wait()
			})

			Eventually(session.Err, "10s").Should(gbytes.Say("job does not have start time"))

			Expect(setJobStartTime(ctx, job)).To(Succeed())

			Eventually(session, "10s").Should(gexec.Exit(1))
			Expect(session.Err).To(gbytes.Say("no pods found for job '" + jobName + "'"))
		})
	})

	Context("when the Pod is deleted while being followed", func() {
		const jobRequestName = "follow-pod-deleted"
		const jobName = "follow-pod-deleted-x7k2p"
		const podName = "follow-pod-deleted-x7k2p-9fh3s"
		const namespace = "apps"

		BeforeEach(func(ctx SpecContext) {
			err := SwitchToKubernetesUser(JobRequesterUser)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			err := SwitchToKubernetesAdminUser()
			Expect(err).NotTo(HaveOccurred())
		})

		It("exits with a pod deleted error", func(ctx SpecContext) {
			jr := pendingJobRequest(jobRequestName, namespace, JobRequesterUser.ARN)
			jr.Status.State = jrv1.JobRequestStarted
			jr.Status.JobName = jobName
			Expect(createJobRequest(ctx, jr)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(deleteJobRequest(ctx, jr)).To(Succeed())
			})

			job := suspendedJob(jobName, namespace)
			Expect(createJob(ctx, job)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(deleteJob(ctx, job)).To(Succeed())
			})

			pod := pendingPod(podName, namespace, jobName)
			Expect(createPod(ctx, pod)).To(Succeed())

			cmd, err := cliCmd(ctx, "jobrequest", "get", jobRequestName, "--follow", "--log-level", "debug", "--kubeconfig", kubeconfigPath, "--namespace", namespace)
			Expect(err).NotTo(HaveOccurred())

			session, err := gexec.Start(cmd, GinkgoWriter, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				session.Kill().Wait()
			})

			Eventually(session.Err, "10s").Should(gbytes.Say("job does not have start time"))
			Expect(setJobStartTime(ctx, job)).To(Succeed())

			// The Deleted watch event is only delivered if the Pod is deleted
			// after the CLI's Pod watch is established, so wait for the initial
			// Added event to be logged before deleting.
			Eventually(session.Err, "10s").Should(gbytes.Say("pod is still pending"))

			Expect(deletePod(ctx, pod)).To(Succeed())

			Eventually(session, "10s").Should(gexec.Exit(1))
			Expect(session.Err).To(gbytes.Say("pod deleted"))
		})
	})

	Context("when the Pod leaves the Pending phase", func() {
		const jobRequestName = "follow-pod-starts"
		const jobName = "follow-pod-starts-x7k2p"
		const podName = "follow-pod-starts-x7k2p-9fh3s"
		const namespace = "apps"

		BeforeEach(func(ctx SpecContext) {
			err := SwitchToKubernetesUser(JobRequesterUser)
			Expect(err).NotTo(HaveOccurred())
		})

		AfterEach(func() {
			err := SwitchToKubernetesAdminUser()
			Expect(err).NotTo(HaveOccurred())
		})

		It("finishes the Pod watch loop and starts tailing logs", func(ctx SpecContext) {
			jr := pendingJobRequest(jobRequestName, namespace, JobRequesterUser.ARN)
			jr.Status.State = jrv1.JobRequestStarted
			jr.Status.JobName = jobName
			Expect(createJobRequest(ctx, jr)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(deleteJobRequest(ctx, jr)).To(Succeed())
			})

			job := suspendedJob(jobName, namespace)
			Expect(createJob(ctx, job)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(deleteJob(ctx, job)).To(Succeed())
			})

			pod := pendingPod(podName, namespace, jobName)
			Expect(createPod(ctx, pod)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(deletePod(ctx, pod)).To(Succeed())
			})

			cmd, err := cliCmd(ctx, "jobrequest", "get", jobRequestName, "--follow", "--log-level", "debug", "--kubeconfig", kubeconfigPath, "--namespace", namespace)
			Expect(err).NotTo(HaveOccurred())

			session, err := gexec.Start(cmd, GinkgoWriter, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				session.Kill().Wait()
			})

			Eventually(session.Err, "10s").Should(gbytes.Say("job does not have start time"))
			Expect(setJobStartTime(ctx, job)).To(Succeed())

			// Wait for the CLI's Pod watch to see the pod in the Pending phase
			// before moving it to Running.
			Eventually(session.Err, "10s").Should(gbytes.Say("pod is still pending"))

			Expect(setPodPhase(ctx, pod, corev1.PodRunning)).To(Succeed())

			Eventually(session.Err, "10s").Should(gbytes.Say("pod is no longer pending"))
			Eventually(session.Err, "10s").Should(gbytes.Say("Tailing logs from pod"))
		})
	})

	Context("when the Pod has logs available", func() {
		const jobRequestName = "follow-pod-logs"
		const jobName = "follow-pod-logs-x7k2p"
		const podName = "follow-pod-logs-x7k2p-9fh3s"
		const nodeName = "kwok-node-follow-logs"
		const namespace = "apps"

		AfterEach(func() {
			err := SwitchToKubernetesAdminUser()
			Expect(err).NotTo(HaveOccurred())
		})

		It("streams the pod's logs", func(ctx SpecContext) {
			logsFile := filepath.Join(GinkgoT().TempDir(), "pod.log")
			logLines := "2026-07-14T15:00:00.000000001Z stdout F migration started\n" +
				"2026-07-14T15:00:00.000000002Z stdout F migration finished\n"
			Expect(os.WriteFile(logsFile, []byte(logLines), 0o644)).To(Succeed())

			node := kwokNode(nodeName)
			Expect(createNode(ctx, node)).To(Succeed())

			err := SwitchToKubernetesUser(JobRequesterUser)
			Expect(err).NotTo(HaveOccurred())

			DeferCleanup(func(ctx SpecContext) {
				Expect(deleteNode(ctx, node)).To(Succeed())
			})

			Expect(createPodLogs(ctx, podName, namespace, "app", logsFile)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(deletePodLogs(ctx, podName, namespace)).To(Succeed())
			})

			jr := pendingJobRequest(jobRequestName, namespace, JobRequesterUser.ARN)
			jr.Status.State = jrv1.JobRequestStarted
			jr.Status.JobName = jobName
			Expect(createJobRequest(ctx, jr)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(deleteJobRequest(ctx, jr)).To(Succeed())
			})

			job := suspendedJob(jobName, namespace)
			Expect(createJob(ctx, job)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(deleteJob(ctx, job)).To(Succeed())
			})

			// Assigning the pod to the kwok-managed node means kwok moves it
			// to Running and serves its logs, so no manual phase update is
			// needed. The toleration lets it stay on the tainted fake node.
			pod := pendingPod(podName, namespace, jobName)
			pod.Spec.NodeName = nodeName
			pod.Spec.Tolerations = []corev1.Toleration{
				{
					Key:      "kwok.x-k8s.io/node",
					Operator: corev1.TolerationOpExists,
				},
			}
			Expect(createPod(ctx, pod)).To(Succeed())

			DeferCleanup(func(ctx SpecContext) {
				Expect(deletePod(ctx, pod)).To(Succeed())
			})

			cmd, err := cliCmd(ctx, "jobrequest", "get", jobRequestName, "--follow", "--log-level", "debug", "--kubeconfig", kubeconfigPath, "--namespace", namespace)
			Expect(err).NotTo(HaveOccurred())

			session, err := gexec.Start(cmd, GinkgoWriter, GinkgoWriter)
			Expect(err).NotTo(HaveOccurred())
			DeferCleanup(func() {
				session.Kill().Wait()
			})

			Eventually(session.Err, "10s").Should(gbytes.Say("job does not have start time"))
			Expect(setJobStartTime(ctx, job)).To(Succeed())

			Eventually(session.Err, "10s").Should(gbytes.Say("Tailing logs from pod"))

			// The CLI streams log lines with the builtin print, which writes
			// to stderr.
			Eventually(session.Err, "10s").Should(gbytes.Say("migration started"))
			Eventually(session.Err, "10s").Should(gbytes.Say("migration finished"))
		})
	})
})
