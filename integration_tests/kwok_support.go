package integration_tests

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	jrv1 "github.com/alphagov/govuk-job-request-operator/api/v1"
	batchv1 "k8s.io/api/batch/v1"
	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apiextensionsclient "k8s.io/apiextensions-apiserver/pkg/client/clientset/clientset"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/apis/meta/v1/unstructured"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/client-go/dynamic"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"sigs.k8s.io/yaml"
)

const clusterName = "govuk-cli-test"

var (
	kubeconfigPath string
	dynamicClient  *dynamic.DynamicClient
)

// kwokctl runs a kwokctl command against the test cluster.
func kwokctl(ctx context.Context, args ...string) ([]byte, error) {
	kwokctlArgs := append([]string{"tool", "kwokctl", "--name", clusterName}, args...)
	cmd := exec.CommandContext(ctx, "go", kwokctlArgs...)
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil, fmt.Errorf("go %s: %w\n%s", strings.Join(kwokctlArgs, " "), err, exitErr.Stderr)
		}
		return nil, fmt.Errorf("go %s: %w", strings.Join(kwokctlArgs, " "), err)
	}
	return out, nil
}

func kubectl(ctx context.Context, args ...string) (string, error) {
	kubectlArgs := append([]string{"kubectl", "--kubeconfig", kubeconfigPath}, args...)

	output, err := kwokctl(ctx, kubectlArgs...)
	if err != nil {
		return "", err
	}

	return string(output), nil
}

// operatorCRDPath resolves the path to the CRD manifests shipped inside the
// govuk-job-request-operator module.
func operatorCRDPath(ctx context.Context) (string, error) {
	out, err := exec.CommandContext(ctx, "go", "list", "-m", "-f", "{{.Dir}}", "github.com/alphagov/govuk-job-request-operator").Output()
	if err != nil {
		return "", fmt.Errorf("resolving operator module dir: %w", err)
	}
	return filepath.Join(strings.TrimSpace(string(out)), "config", "crd", "bases"), nil
}

type KwokPkiFiles struct {
	KeyFile  string
	CertFile string
}

// startTestCluster creates a kwokctl-managed cluster with the JobRequest CRDs
// installed and writes out a kubeconfig for it.
func startTestCluster(ctx context.Context) error {
	// Remove any cluster left behind by an earlier interrupted run; deleting
	// a cluster that doesn't exist is a no-op.
	if _, err := kwokctl(ctx, "delete", "cluster"); err != nil {
		return err
	}

	pkiFiles, err := getKwokPkiFiles()
	if err != nil {
		return err
	}

	kubeconfigFile, err := os.CreateTemp("", "govuk-cli-kwok-kubeconfig-*")
	if err != nil {
		return err
	}
	err = kubeconfigFile.Close()
	if err != nil {
		return err
	}
	kubeconfigPath = kubeconfigFile.Name()

	// The Logs CRD lets tests serve a local file as a pod's container logs
	// through kwok's fake kubelet, so log streaming can be tested end-to-end.
	_, err = kwokctl(
		ctx,
		"create", "cluster",
		"--kubeconfig", kubeconfigPath,
		"--runtime", "binary",
		"--enable-crds", "Logs",
		"--extra-args", fmt.Sprintf("kube-controller-manager=cluster-signing-cert-file=%s", pkiFiles.CertFile),
		"--extra-args", fmt.Sprintf("kube-controller-manager=cluster-signing-key-file=%s", pkiFiles.KeyFile),
		"--wait", "120s",
	)
	if err != nil {
		return err
	}

	kubeconfig, err := kwokctl(ctx, "get", "kubeconfig")
	if err != nil {
		return err
	}

	config, err := clientcmd.RESTConfigFromKubeConfig(kubeconfig)
	if err != nil {
		return err
	}

	if err := installCRDs(ctx, config); err != nil {
		return err
	}

	dynamicClient, err = dynamic.NewForConfig(config)
	if err != nil {
		return err
	}

	_, err = kubectl(ctx, "create", "namespace", "apps")
	return err
}

func stopTestCluster(ctx context.Context) error {
	if kubeconfigPath != "" {
		err := os.Remove(kubeconfigPath)
		if err != nil {
			return err
		}
	}
	_, err := kwokctl(ctx, "delete", "cluster")
	return err
}

// installCRDs installs the operator's CRD manifests into the test cluster and
// waits for them to become established, so tests can create custom resources
// as soon as the suite setup returns.
func installCRDs(ctx context.Context, config *rest.Config) error {
	crdDir, err := operatorCRDPath(ctx)
	if err != nil {
		return err
	}

	manifests, err := filepath.Glob(filepath.Join(crdDir, "*.yaml"))
	if err != nil {
		return err
	}
	if len(manifests) == 0 {
		return fmt.Errorf("no CRD manifests found in %s", crdDir)
	}

	client, err := apiextensionsclient.NewForConfig(config)
	if err != nil {
		return err
	}

	crdNames := make([]string, 0, len(manifests))
	for _, manifest := range manifests {
		data, err := os.ReadFile(manifest)
		if err != nil {
			return err
		}
		crd := &apiextensionsv1.CustomResourceDefinition{}
		if err := yaml.Unmarshal(data, crd); err != nil {
			return fmt.Errorf("unmarshalling CRD manifest %s: %w", manifest, err)
		}
		if _, err := client.ApiextensionsV1().CustomResourceDefinitions().Create(ctx, crd, metav1.CreateOptions{}); err != nil {
			return fmt.Errorf("creating CRD %s: %w", crd.Name, err)
		}
		crdNames = append(crdNames, crd.Name)
	}

	for _, name := range crdNames {
		err := wait.PollUntilContextTimeout(ctx, 100*time.Millisecond, 30*time.Second, true, func(ctx context.Context) (bool, error) {
			crd, err := client.ApiextensionsV1().CustomResourceDefinitions().Get(ctx, name, metav1.GetOptions{})
			if err != nil {
				return false, err
			}
			for _, condition := range crd.Status.Conditions {
				if condition.Type == apiextensionsv1.Established && condition.Status == apiextensionsv1.ConditionTrue {
					return true, nil
				}
			}
			return false, nil
		})
		if err != nil {
			return fmt.Errorf("waiting for CRD %s to become established: %w", name, err)
		}
	}

	return nil
}

// createJobRequest creates the given JobRequest in the test cluster. The
// status subresource is enabled on the CRD, so any status on the fixture is
// dropped by the create and has to be applied with a second, status-only
// update.
func createJobRequest(ctx context.Context, jr *jrv1.JobRequest) error {
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(jr)
	if err != nil {
		return err
	}
	u := &unstructured.Unstructured{Object: obj}
	u.SetAPIVersion(jrv1.SchemeGroupVersion.String())
	u.SetKind("JobRequest")

	i := dynamicClient.Resource(jrv1.SchemeGroupVersion.WithResource("jobrequests")).Namespace(jr.Namespace)
	created, err := i.Create(ctx, u, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	status, hasStatus := obj["status"]
	if !hasStatus {
		return nil
	}
	created.Object["status"] = status
	_, err = i.UpdateStatus(ctx, created, metav1.UpdateOptions{})
	return err
}

// updateJobRequestStatus updates the status subresource of an existing
// JobRequest in the test cluster.
func updateJobRequestStatus(ctx context.Context, jr *jrv1.JobRequest) error {
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(jr)
	if err != nil {
		return err
	}

	i := dynamicClient.Resource(
		jrv1.SchemeGroupVersion.WithResource("jobrequests"),
	).Namespace(jr.Namespace)

	// fetch
	existing, err := i.Get(ctx, jr.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}

	// update status
	status, hasStatus := obj["status"]
	if !hasStatus {
		return nil
	}
	existing.Object["status"] = status

	// commit
	_, err = i.UpdateStatus(ctx, existing, metav1.UpdateOptions{})
	return err
}

// getJobRequest fetches a JobRequest by name from the test cluster, so tests
// can assert on the fields the CLI actually persisted rather than just its
// printed output.
func getJobRequest(ctx context.Context, name string, namespace string) (*jrv1.JobRequest, error) {
	u, err := dynamicClient.
		Resource(jrv1.SchemeGroupVersion.WithResource("jobrequests")).
		Namespace(namespace).
		Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	jr := &jrv1.JobRequest{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, jr); err != nil {
		return nil, err
	}
	return jr, nil
}

func deleteJobRequest(ctx context.Context, jr *jrv1.JobRequest) error {
	return dynamicClient.
		Resource(jrv1.SchemeGroupVersion.WithResource("jobrequests")).
		Namespace(jr.Namespace).
		Delete(ctx, jr.Name, metav1.DeleteOptions{})
}

func createJob(ctx context.Context, job *batchv1.Job) error {
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(job)
	if err != nil {
		return err
	}
	u := &unstructured.Unstructured{Object: obj}
	u.SetAPIVersion(batchv1.SchemeGroupVersion.String())
	u.SetKind("Job")

	_, err = dynamicClient.
		Resource(batchv1.SchemeGroupVersion.WithResource("jobs")).
		Namespace(job.Namespace).
		Create(ctx, u, metav1.CreateOptions{})
	return err
}

func deleteJob(ctx context.Context, job *batchv1.Job) error {
	propPolicy := metav1.DeletePropagationBackground
	return dynamicClient.
		Resource(batchv1.SchemeGroupVersion.WithResource("jobs")).
		Namespace(job.Namespace).
		Delete(ctx, job.Name, metav1.DeleteOptions{PropagationPolicy: &propPolicy})
}

// setJobStartTime sets status.startTime on an existing Job.
// Doing it directly rather than unsuspending
// the Job keeps the cluster's job controller from also creating pods.
func setJobStartTime(ctx context.Context, job *batchv1.Job) error {
	i := dynamicClient.Resource(batchv1.SchemeGroupVersion.WithResource("jobs")).Namespace(job.Namespace)
	current, err := i.Get(ctx, job.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := unstructured.SetNestedField(current.Object, time.Now().UTC().Format(time.RFC3339), "status", "startTime"); err != nil {
		return err
	}
	_, err = i.UpdateStatus(ctx, current, metav1.UpdateOptions{})
	return err
}

func createPod(ctx context.Context, pod *corev1.Pod) error {
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(pod)
	if err != nil {
		return err
	}
	u := &unstructured.Unstructured{Object: obj}
	u.SetAPIVersion(corev1.SchemeGroupVersion.String())
	u.SetKind("Pod")

	_, err = dynamicClient.
		Resource(corev1.SchemeGroupVersion.WithResource("pods")).
		Namespace(pod.Namespace).
		Create(ctx, u, metav1.CreateOptions{})
	return err
}

func deletePod(ctx context.Context, pod *corev1.Pod) error {
	return dynamicClient.
		Resource(corev1.SchemeGroupVersion.WithResource("pods")).
		Namespace(pod.Namespace).
		Delete(ctx, pod.Name, metav1.DeleteOptions{})
}

// setPodPhase sets status.phase on an existing Pod, standing in for the
// kubelet that would normally move a scheduled pod out of Pending.
func setPodPhase(ctx context.Context, pod *corev1.Pod, phase corev1.PodPhase) error {
	i := dynamicClient.Resource(corev1.SchemeGroupVersion.WithResource("pods")).Namespace(pod.Namespace)
	current, err := i.Get(ctx, pod.Name, metav1.GetOptions{})
	if err != nil {
		return err
	}
	if err := unstructured.SetNestedField(current.Object, string(phase), "status", "phase"); err != nil {
		return err
	}
	_, err = i.UpdateStatus(ctx, current, metav1.UpdateOptions{})
	return err
}

func createNode(ctx context.Context, node *corev1.Node) error {
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(node)
	if err != nil {
		return err
	}
	u := &unstructured.Unstructured{Object: obj}
	u.SetAPIVersion(corev1.SchemeGroupVersion.String())
	u.SetKind("Node")

	_, err = dynamicClient.
		Resource(corev1.SchemeGroupVersion.WithResource("nodes")).
		Create(ctx, u, metav1.CreateOptions{})
	return err
}

func deleteNode(ctx context.Context, node *corev1.Node) error {
	return dynamicClient.
		Resource(corev1.SchemeGroupVersion.WithResource("nodes")).
		Delete(ctx, node.Name, metav1.DeleteOptions{})
}

var kwokLogsGVR = schema.GroupVersionResource{Group: "kwok.x-k8s.io", Version: "v1alpha1", Resource: "logs"}

// createPodLogs registers a kwok Logs resource that serves the given local
// file as the named pod's container logs. The file must be in CRI log format:
// "<RFC3339Nano timestamp> stdout F <content>" per line. kwok's controller
// runs as a local process, so any file readable by the test can be used.
func createPodLogs(ctx context.Context, podName string, namespace string, containerName string, logsFile string) error {
	u := &unstructured.Unstructured{Object: map[string]any{
		"apiVersion": "kwok.x-k8s.io/v1alpha1",
		"kind":       "Logs",
		"metadata": map[string]any{
			"name":      podName,
			"namespace": namespace,
		},
		"spec": map[string]any{
			"logs": []any{
				map[string]any{
					"containers": []any{containerName},
					"logsFile":   logsFile,
					"follow":     true,
				},
			},
		},
	}}
	_, err := dynamicClient.Resource(kwokLogsGVR).Namespace(namespace).Create(ctx, u, metav1.CreateOptions{})
	return err
}

func deletePodLogs(ctx context.Context, podName string, namespace string) error {
	return dynamicClient.Resource(kwokLogsGVR).Namespace(namespace).Delete(ctx, podName, metav1.DeleteOptions{})
}

func createJobRequestReview(ctx context.Context, jrr *jrv1.JobRequestReview) error {
	obj, err := runtime.DefaultUnstructuredConverter.ToUnstructured(jrr)
	if err != nil {
		return err
	}
	u := &unstructured.Unstructured{Object: obj}
	u.SetAPIVersion(jrv1.SchemeGroupVersion.String())
	u.SetKind("JobRequestReview")

	i := dynamicClient.
		Resource(jrv1.SchemeGroupVersion.WithResource("jobrequestreviews")).
		Namespace(jrr.Namespace)

	created, err := i.Create(ctx, u, metav1.CreateOptions{})
	if err != nil {
		return err
	}

	status, hasStatus := obj["status"]
	if !hasStatus {
		return nil
	}
	created.Object["status"] = status
	_, err = i.UpdateStatus(ctx, created, metav1.UpdateOptions{})
	return err
}

// getJobRequestReview fetches a JobRequestReview by name from the test cluster,
// so tests can assert on the fields the CLI actually persisted rather than just
// its printed output.
func getJobRequestReview(ctx context.Context, name string, namespace string) (*jrv1.JobRequestReview, error) {
	u, err := dynamicClient.
		Resource(jrv1.SchemeGroupVersion.WithResource("jobrequestreviews")).
		Namespace(namespace).
		Get(ctx, name, metav1.GetOptions{})
	if err != nil {
		return nil, err
	}

	jrr := &jrv1.JobRequestReview{}
	if err := runtime.DefaultUnstructuredConverter.FromUnstructured(u.Object, jrr); err != nil {
		return nil, err
	}
	return jrr, nil
}

func deleteJobRequestReview(ctx context.Context, jrr *jrv1.JobRequestReview) error {
	return dynamicClient.
		Resource(jrv1.SchemeGroupVersion.WithResource("jobrequestreviews")).
		Namespace(jrr.Namespace).
		Delete(ctx, jrr.Name, metav1.DeleteOptions{})
}

func getKwokPkiFiles() (*KwokPkiFiles, error) {
	workdirPath, ok := os.LookupEnv("KWOK_WORKDIR")
	if !ok {
		homeDir, err := os.UserHomeDir()
		if err != nil {
			return nil, err
		}
		workdirPath = filepath.Join(homeDir, ".kwok")
	}

	kwokClusterPkiPath := filepath.Join(workdirPath, "clusters", clusterName, "pki")

	kwokPkiFiles := &KwokPkiFiles{
		CertFile: filepath.Join(kwokClusterPkiPath, "ca.crt"),
		KeyFile:  filepath.Join(kwokClusterPkiPath, "ca.key"),
	}

	return kwokPkiFiles, nil
}
