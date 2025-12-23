package aks

import (
	"context"
	"encoding/base64"
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/azcore/runtime"
	"github.com/Azure/azure-sdk-for-go/sdk/azidentity"
	"github.com/Azure/azure-sdk-for-go/sdk/resourcemanager/containerservice/armcontainerservice"
	"github.com/chainguard-dev/clog"
	"github.com/chainguard-dev/terraform-provider-imagetest/internal/docker"
	"github.com/chainguard-dev/terraform-provider-imagetest/internal/drivers"
	"github.com/chainguard-dev/terraform-provider-imagetest/internal/drivers/pod"
	"github.com/charmbracelet/log"
	"github.com/google/go-containerregistry/pkg/name"
	"github.com/google/uuid"
	"k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
)

const (
	locationDefault   = "westeurope"
	nodeCountDefault  = 1
	nodeVMSizeDefault = "Standard_DS2_v2"
	timeoutDefault    = 30 * time.Minute
)

type driver struct {
	name string

	resourceGroup     string
	location          string
	nodeCount         int32
	nodeVMSize        string
	nodeDiskSize      int32
	nodeDiskType      string
	nodePoolName      string
	timeout           time.Duration
	subscriptionID    string
	kubernetesVersion string
	tags              map[string]string

	clusterName string
	kubeconfig  string
	kcli        kubernetes.Interface
	kcfg        *rest.Config

	aksClient *armcontainerservice.ManagedClustersClient

	registries map[string]*RegistryConfig
}

type Options struct {
	// REQUIRED. An existing Azure resource group that will hold the AKS
	// cluster resources.
	ResourceGroup string
	// Azure region.
	// Default: westeurope
	Location string
	// The AKS cluster node count.
	// Default: 1.
	NodeCount int32
	// The Azure VM size used by the AKS cluster nodes.
	// Default: "Standard_DS2_v2"
	NodeVMSize string
	// Use a custom VM disk size (GB) instead of the one defined by the VM size.
	NodeDiskSize int32
	// The disk type: "Ephemeral" or "Managed".
	// Defaults to "Ephemeral", which provide better performance but aren't persistent.
	NodeDiskType string
	// The node pool name. Will use the cluster name as a default.
	NodePoolName string
	// Go duration format for long running operations, such as AKS cluster
	// provisioning.
	// Default: "20m".
	Timeout string
	// The Azure subscription ID.
	// Defaults to the "AZURE_SUBSCRIPTION_ID" environment value.
	SubscriptionID string
	// The Kubernetes version to deploy.
	// Uses the Azure default if unspecified.
	KubernetesVersion string
	Tags              map[string]string

	Registries map[string]*RegistryConfig

	// TODO: check if the following are required.
	// DNSPrefix string
	//
	// Other possible settings:
	// node_image_reference - Custom VM image for nodes (for Chainguard images)
	// workload_identity_associations - Azure Workload Identity (parity with EKS Pod Identity)
}

// RegistryConfig holds authentication configuration for a container registry.
type RegistryConfig struct {
	Auth *RegistryAuthConfig
}

// RegistryAuthConfig holds the credentials for authenticating to a container registry.
type RegistryAuthConfig struct {
	Username string
	Password string
	Auth     string
}

// NewDriver creates a new AKS driver instance that uses the Azure SDK to
// provision and manage an Azure AKS cluster for running tests.
func NewDriver(name string, opts Options) (drivers.Tester, error) {
	k := &driver{
		name:              name,
		location:          opts.Location,
		nodeCount:         opts.NodeCount,
		nodeVMSize:        opts.NodeVMSize,
		nodePoolName:      opts.NodePoolName,
		nodeDiskSize:      opts.NodeDiskSize,
		nodeDiskType:      opts.NodeDiskType,
		subscriptionID:    opts.SubscriptionID,
		kubernetesVersion: opts.KubernetesVersion,
		tags:              opts.Tags,
	}
	if k.location == "" {
		k.location = locationDefault
	}
	if k.nodeCount <= 0 {
		k.nodeCount = nodeCountDefault
	}
	if k.nodeVMSize == "" {
		k.nodeVMSize = nodeVMSizeDefault
	}
	if opts.Timeout != "" {
		timeout, err := time.ParseDuration(opts.Timeout)
		if err != nil {
			return nil, fmt.Errorf("unable to parse timeout setting: %s %v", opts.Timeout, err)
		}
		k.timeout = timeout
	} else {
		k.timeout = timeoutDefault
	}
	if opts.Registries != nil {
		k.registries = opts.Registries
	}
	if k.subscriptionID == "" {
		if v, ok := os.LookupEnv("AZURE_SUBSCRIPTION_ID"); ok {
			log.Infof("Using subscription from AZURE_SUBSCRIPTION_ID")
			k.subscriptionID = v
		} else {
			return nil, fmt.Errorf("no Azure subscription specified")
		}
	}
	switch k.nodeDiskType {
	case "", "Ephemeral", "Managed":
	}
	return nil, fmt.Errorf(
		"invalid node disk type: %s, supported types: Ephemeral, Managed", k.nodeDiskType)
	return k, nil
}

func (k *driver) Setup(ctx context.Context) error {
	log := clog.FromContext(ctx)

	// Obtain Azure credentials based on environment variables.
	cred, err := azidentity.NewDefaultAzureCredential(nil)
	if err != nil {
		return fmt.Errorf("unable to obtain Azure credentials: %v", err)
	}

	aksClient, err := armcontainerservice.NewManagedClustersClient(
		k.subscriptionID, cred, nil)
	if err != nil {
		return fmt.Errorf("unable to create AKS client: %v", err)
	}
	k.aksClient = aksClient

	if n, ok := os.LookupEnv("IMAGETEST_AKS_CLUSTER"); ok {
		log.Infof("Using cluster name from IMAGETEST_AKS_CLUSTER: %s", n)
		k.clusterName = n
	} else {
		uid := "imagetest-" + uuid.New().String()
		log.Infof("Using random cluster name: %s", uid)
		k.clusterName = uid
	}

	if k.nodePoolName == "" {
		k.nodePoolName = k.clusterName
	}

	cfg, err := os.Create(filepath.Join(os.TempDir(), k.clusterName))
	if err != nil {
		return fmt.Errorf("failed creating temp dir: %w", err)
	}

	log.Infof("Using kubeconfig: %s", cfg.Name())
	k.kubeconfig = cfg.Name()

	if _, ok := os.LookupEnv("IMAGETEST_AKS_CLUSTER"); ok {
		log.Infof("Using existing AKS cluster.")
	} else {
		err = k.createCluster(ctx)
		if err != nil {
			return err
		}
	}

	err = k.writeKubeConfig(ctx)
	if err != nil {
		return err
	}

	// TODO: handle workload identity.

	config, err := clientcmd.BuildConfigFromFlags("", k.kubeconfig)
	if err != nil {
		return fmt.Errorf("building kubeconfig: %w", err)
	}
	k.kcfg = config

	kcli, err := kubernetes.NewForConfig(config)
	if err != nil {
		return fmt.Errorf("creating kubernetes client: %w", err)
	}
	k.kcli = kcli

	return nil
}

func (k *driver) createCluster(ctx context.Context) error {
	log := clog.FromContext(ctx)
	log.Infof("Creating AKS cluster.")

	var nodeDiskType *armcontainerservice.OSDiskType = nil
	switch k.nodeDiskType {
	case "Ephemeral":
		nodeDiskType = ptr(armcontainerservice.OSDiskTypeEphemeral)
	case "Managed":
		nodeDiskType = ptr(armcontainerservice.OSDiskTypeManaged)
	}

	poller, err := k.aksClient.BeginCreateOrUpdate(
		ctx,
		k.resourceGroup,
		k.clusterName,
		armcontainerservice.ManagedCluster{
			Location: &k.location,
			Tags:     k.buildTags(),
			Properties: &armcontainerservice.ManagedClusterProperties{
				// DNSPrefix: k.dnsPrefix,
				AgentPoolProfiles: []*armcontainerservice.ManagedClusterAgentPoolProfile{
					{
						Name:         &k.nodePoolName,
						Count:        &k.nodeCount,
						VMSize:       &k.nodeVMSize,
						OSDiskSizeGB: &k.nodeDiskSize,
						OSDiskType:   nodeDiskType,
						Mode:         ptr(armcontainerservice.AgentPoolModeSystem),
						OSType:       ptr(armcontainerservice.OSTypeLinux),
						Type:         ptr(armcontainerservice.AgentPoolTypeVirtualMachineScaleSets),
					},
				},
				// Identity: &armcontainerservice.ManagedClusterIdentity{
				// 	Type: ptr(armcontainerservice.ResourceIdentityTypeSystemAssigned),
				// },
				NetworkProfile: &armcontainerservice.NetworkProfile{
					NetworkPlugin: ptr(armcontainerservice.NetworkPluginAzure),
				},
			},
		},
		nil,
	)
	if err != nil {
		return fmt.Errorf("failed to iniate AKS cluster creation: %v", err)
	}

	resp, err := poller.PollUntilDone(ctx, &runtime.PollUntilDoneOptions{
		Frequency: k.timeout,
	})
	if err != nil {
		return fmt.Errorf("failed to create AKS cluster: %v", err)
	}

	// TODO: store the cluster id.
	log.Infof("Created AKS cluster: %s", resp.ID)
	return nil
}

func (k *driver) writeKubeConfig(ctx context.Context) error {
	creds, err := k.aksClient.ListClusterAdminCredentials(
		ctx, k.resourceGroup, k.clusterName, nil)
	if err != nil {
		return fmt.Errorf("failed to retrieve kubeconfig: %v", err)
	}

	if len(creds.Kubeconfigs) == 0 {
		return fmt.Errorf("no kubeconfigs retrieved")
	}

	kubeconfigBytes, err := base64.StdEncoding.DecodeString(string(creds.Kubeconfigs[0].Value))
	if err != nil {
		return fmt.Errorf("failed to decode kubeconfig: %v", err)
	}

	err = os.WriteFile(k.kubeconfig, kubeconfigBytes, 0o644)
	if err != nil {
		return fmt.Errorf("unable to write kubeconfig: %s %v", k.kubeconfig, err)
	}

	return nil
}

func (k *driver) buildTags() map[string]*string {
	tags := map[string]*string{
		"imagetest":              ptr("true"),
		"imagetest:test-name":    &k.name,
		"imagetest:cluster-name": &k.clusterName,
	}
	for k, v := range k.tags {
		tags[k] = &v
	}
	return tags
}

func ptr[T any](v T) *T {
	return &v
}

func (k *driver) Teardown(ctx context.Context) error {
	log := clog.FromContext(ctx)
	if v := os.Getenv("IMAGETEST_EKS_SKIP_TEARDOWN"); v == "true" {
		log.Info("Skipping AKS teardown due to IMAGETEST_EKS_SKIP_TEARDOWN=true")
		return nil
	}
	if _, ok := os.LookupEnv("IMAGETEST_AKS_CLUSTER"); ok {
		log.Infof("Skipping AKS teardown due to existing cluster: IMAGETEST_AKS_CLUSTER.")
		return nil
	}

	poller, err := k.aksClient.BeginDelete(ctx, k.resourceGroup, k.clusterName, nil)
	if err != nil {
		return fmt.Errorf("failed to initate AKS cluster teardown: %v", err)
	}

	_, err = poller.PollUntilDone(ctx, &runtime.PollUntilDoneOptions{
		Frequency: k.timeout,
	})
	if err != nil {
		return fmt.Errorf("failed to delete AKS cluster: %v", err)
	}

	return nil
}

func (k *driver) Run(ctx context.Context, ref name.Reference) (*drivers.RunResult, error) {
	// Build docker config from registries for pod authentication
	// TODO: consider reusing the registry related code since it's not driver
	// specific.
	dcfg := &docker.DockerConfig{
		Auths: make(map[string]docker.DockerAuthConfig, len(k.registries)),
	}
	for reg, cfg := range k.registries {
		if cfg.Auth == nil {
			continue
		}
		dcfg.Auths[reg] = docker.DockerAuthConfig{
			Username: cfg.Auth.Username,
			Password: cfg.Auth.Password,
			Auth:     cfg.Auth.Auth,
		}
	}

	return pod.Run(ctx, k.kcfg,
		pod.WithImageRef(ref),
		pod.WithExtraEnvs(map[string]string{
			"IMAGETEST_DRIVER": "aks",
		}),
		pod.WithRegistryStaticAuth(dcfg),
	)
}
