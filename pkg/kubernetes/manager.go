package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/containers/kubernetes-mcp-server/pkg/api"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
	"k8s.io/klog/v2"
)

type Manager struct {
	kubernetes *Kubernetes

	config api.BaseConfig
}

var _ api.Openshift = (*Manager)(nil)

var (
	ErrorKubeconfigInClusterNotAllowed = errors.New("kubeconfig manager cannot be used in in-cluster deployments")
	ErrorInClusterNotInCluster         = errors.New("in-cluster manager cannot be used outside of a cluster")
)

func NewKubeconfigManager(ctx context.Context, config api.BaseConfig, kubeconfigContext string) (*Manager, error) {
	if IsInCluster(config) {
		return nil, ErrorKubeconfigInClusterNotAllowed
	}

	// When the full kubeconfig is provided inline via KUBECONFIG_YAML, load it
	// from memory instead of from a file. This avoids materializing a temp file
	// and sidesteps client-go's KUBECONFIG env var, which is parsed as a
	// colon-separated path list and breaks when the path itself contains a ':'.
	if inline := os.Getenv("KUBECONFIG_YAML"); inline != "" {
		return newKubeconfigManagerFromBytes(ctx, config, []byte(inline), kubeconfigContext)
	}

	pathOptions := clientcmd.NewDefaultPathOptions()
	if config.GetKubeConfigPath() != "" {
		pathOptions.LoadingRules.ExplicitPath = config.GetKubeConfigPath()
	}

	resolvedContext, err := resolveKubeconfigContext(ctx, pathOptions.LoadingRules, kubeconfigContext)
	if err != nil {
		return nil, err
	}

	clientCmdConfig := clientcmd.NewNonInteractiveDeferredLoadingClientConfig(
		pathOptions.LoadingRules,
		&clientcmd.ConfigOverrides{
			ClusterInfo:    clientcmdapi.Cluster{Server: ""},
			CurrentContext: resolvedContext,
		})

	restConfig, err := clientCmdConfig.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes rest config from kubeconfig: %w", err)
	}

	return NewManager(ctx, config, restConfig, clientCmdConfig)
}

// newKubeconfigManagerFromBytes builds a Manager from an in-memory kubeconfig
// (the raw YAML/JSON content), mirroring NewKubeconfigManager's path-based flow.
// A non-nil but empty ClientConfigLoadingRules is used as the ConfigAccess so the
// kubeconfig file watcher finds zero files to watch and no-ops cleanly.
//
// Each mutateRest fn is applied to the resolved restConfig before NewManager
// runs, i.e. before NewKubernetes builds the clientset/dynamic/discovery
// clients from it — mutating restConfig after NewManager returns would be too
// late, since those clients are already constructed from a copy of it.
func newKubeconfigManagerFromBytes(ctx context.Context, config api.BaseConfig, raw []byte, kubeconfigContext string, mutateRest ...func(*rest.Config)) (*Manager, error) {
	apiConfig, err := clientcmd.Load(raw)
	if err != nil {
		return nil, fmt.Errorf("failed to load kubeconfig from KUBECONFIG_YAML: %w", err)
	}

	resolvedContext, err := resolveContextFromRawConfig(ctx, apiConfig, kubeconfigContext)
	if err != nil {
		return nil, err
	}

	clientCmdConfig := clientcmd.NewNonInteractiveClientConfig(
		*apiConfig,
		resolvedContext,
		&clientcmd.ConfigOverrides{ClusterInfo: clientcmdapi.Cluster{Server: ""}},
		&clientcmd.ClientConfigLoadingRules{},
	)

	restConfig, err := clientCmdConfig.ClientConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to create kubernetes rest config from kubeconfig: %w", err)
	}

	for _, mutate := range mutateRest {
		mutate(restConfig)
	}

	return NewManager(ctx, config, restConfig, clientCmdConfig)
}

// NewInlineKubeconfigManager builds a Manager from request-supplied kubeconfig
// bytes and an optional per-tenant socket dial override. This is the
// multi-tenant HTTP path: unlike NewKubeconfigManager it never consults the
// KUBECONFIG_YAML / KUBERNETES_DIAL_ADDR environment variables — a process-wide
// override would leak across tenants on a shared server.
func NewInlineKubeconfigManager(ctx context.Context, config api.BaseConfig, kubeconfig []byte, dialAddr string) (*Manager, error) {
	// Always set restConfig.Dial explicitly, even when dialAddr is empty: this
	// marks the dial decision as already made, so NewManager's
	// KUBERNETES_DIAL_ADDR fallback (guarded on restConfig.Dial == nil) never
	// fires for a request-supplied kubeconfig, regardless of the shared
	// server's process environment. When dialAddr is empty, the dialer simply
	// dials whatever address is requested (identical to the client-go default).
	d := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	mutateRest := func(restConfig *rest.Config) {
		restConfig.Dial = func(ctx context.Context, network, address string) (net.Conn, error) {
			if dialAddr != "" {
				address = dialAddr
			}
			return d.DialContext(ctx, network, address)
		}
	}
	return newKubeconfigManagerFromBytes(ctx, config, kubeconfig, "", mutateRest)
}

// resolveKubeconfigContext determines which kubeconfig context to use.
// If kubeconfigContext is explicitly set, it is returned as-is.
// If it is empty, the function loads the kubeconfig and:
//   - returns the current-context if set
//   - auto-selects the only available context if there is exactly one
//   - returns a descriptive error if there are zero or multiple contexts
func resolveKubeconfigContext(ctx context.Context, loadingRules *clientcmd.ClientConfigLoadingRules, kubeconfigContext string) (string, error) {
	if kubeconfigContext != "" {
		return kubeconfigContext, nil
	}

	rawConfig, err := loadingRules.Load()
	if err != nil {
		return "", fmt.Errorf("failed to load kubeconfig: %w", err)
	}

	return resolveContextFromRawConfig(ctx, rawConfig, kubeconfigContext)
}

// resolveContextFromRawConfig applies the context-selection rules to an
// already-loaded kubeconfig (see resolveKubeconfigContext).
func resolveContextFromRawConfig(ctx context.Context, rawConfig *clientcmdapi.Config, kubeconfigContext string) (string, error) {
	if kubeconfigContext != "" {
		return kubeconfigContext, nil
	}

	if rawConfig.CurrentContext != "" {
		return rawConfig.CurrentContext, nil
	}

	switch len(rawConfig.Contexts) {
	case 0:
		return "", fmt.Errorf( //nolint:ST1005 // user-facing error with actionable guidance
			"no current-context is set and no contexts are defined in kubeconfig.\n" +
				"Configure a context with 'kubectl config set-context <name>' and 'kubectl config use-context <name>'")
	case 1:
		for name := range rawConfig.Contexts {
			klog.FromContext(ctx).Info(
				"current-context is not set in kubeconfig, auto-selecting the only available context",
				"context_name", name,
			)
			return name, nil
		}
	}

	names := make([]string, 0, len(rawConfig.Contexts))
	for name := range rawConfig.Contexts {
		names = append(names, name)
	}
	slices.Sort(names)
	return "", fmt.Errorf( //nolint:ST1005 // user-facing error with actionable guidance
		"current-context is not set in kubeconfig and multiple contexts are available (%s).\n"+
			"Set one with 'kubectl config use-context <context-name>'",
		strings.Join(names, ", "))
}

func NewInClusterManager(ctx context.Context, config api.BaseConfig) (*Manager, error) {
	if config.GetKubeConfigPath() != "" {
		return nil, fmt.Errorf("kubeconfig file %s cannot be used with the in-cluster deployments: %w", config.GetKubeConfigPath(), ErrorKubeconfigInClusterNotAllowed)
	}

	if !IsInCluster(config) {
		return nil, ErrorInClusterNotInCluster
	}

	restConfig, err := InClusterConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to create in-cluster kubernetes rest config: %w", err)
	}

	// Create a dummy kubeconfig clientcmdapi.Config for in-cluster config to be used in places where clientcmd.ClientConfig is required
	clientCmdConfig := clientcmdapi.NewConfig()
	clientCmdConfig.Clusters["cluster"] = &clientcmdapi.Cluster{
		Server:                restConfig.Host,
		InsecureSkipTLSVerify: restConfig.Insecure,
	}
	clientCmdConfig.AuthInfos["user"] = &clientcmdapi.AuthInfo{
		Token: restConfig.BearerToken,
	}
	clientCmdConfig.Contexts[inClusterKubeConfigDefaultContext] = &clientcmdapi.Context{
		Cluster:  "cluster",
		AuthInfo: "user",
	}
	clientCmdConfig.CurrentContext = inClusterKubeConfigDefaultContext

	return NewManager(ctx, config, restConfig, clientcmd.NewDefaultClientConfig(*clientCmdConfig, nil))
}

// dialAddrOverride returns a rest.Config Dial func that routes every API-server
// connection to the address in KUBERNETES_DIAL_ADDR, leaving Host / TLS
// ServerName / CA verification untouched. Returns nil when the env var is unset.
func dialAddrOverride() func(ctx context.Context, network, address string) (net.Conn, error) {
	dialAddr := os.Getenv("KUBERNETES_DIAL_ADDR")
	if dialAddr == "" {
		return nil
	}
	d := &net.Dialer{Timeout: 30 * time.Second, KeepAlive: 30 * time.Second}
	return func(ctx context.Context, network, _ string) (net.Conn, error) {
		return d.DialContext(ctx, network, dialAddr)
	}
}

func NewManager(ctx context.Context, config api.BaseConfig, restConfig *rest.Config, clientCmdConfig clientcmd.ClientConfig) (*Manager, error) {
	if config == nil {
		return nil, errors.New("config cannot be nil")
	}
	if restConfig == nil {
		return nil, errors.New("restConfig cannot be nil")
	}
	if clientCmdConfig == nil {
		return nil, errors.New("clientCmdConfig cannot be nil")
	}

	// Apply QPS and Burst from environment variables if set (primarily for testing)
	applyRateLimitFromEnv(restConfig)

	// KUBERNETES_DIAL_ADDR overrides only the TCP socket destination: the API
	// server connection is dialed to this host:port instead of the kubeconfig
	// server host, while Host, TLS ServerName and CA verification all stay
	// derived from the real kubeconfig (so TLS verifies end-to-end). This is the
	// connector case: the kubeconfig server stays the real https API-server host
	// but the socket lands on the loopback tunnel proxy (127.0.0.1:PORT).
	// rest.CopyConfig (in NewKubernetes) copies Dial, so this propagates to the
	// clientset, dynamic, discovery and metrics clients built from restConfig.
	// Skipped when restConfig.Dial is already set (e.g. by
	// NewInlineKubeconfigManager's per-tenant override): a process-wide env
	// knob must never clobber a caller-supplied dial override.
	if restConfig.Dial == nil {
		if dial := dialAddrOverride(); dial != nil {
			restConfig.Dial = dial
		}
	}

	k8s := &Manager{
		config: config,
	}
	var err error
	// TODO: Won't work because not all client-go clients use the shared context (e.g. discovery client uses context.TODO())
	//k8s.restConfig.Wrap(func(original http.RoundTripper) http.RoundTripper {
	//	return &impersonateRoundTripper{original}
	//})
	k8s.kubernetes, err = NewKubernetes(ctx, k8s.config, clientCmdConfig, restConfig)
	if err != nil {
		return nil, err
	}
	return k8s, nil
}

func (m *Manager) Derived(ctx context.Context) (*Kubernetes, error) {
	authorization, ok := ctx.Value(OAuthAuthorizationHeader).(string)
	hasToken := ok && strings.HasPrefix(authorization, "Bearer ")
	logger := klog.FromContext(ctx)

	// No token: fall back to kubeconfig credentials, unless require_oauth=true.
	// In kubeconfig mode, the token exchange layer clears the auth header before we get here,
	// so this branch handles both "no token sent" and "kubeconfig mode cleared it".
	// The require_oauth guard is defense-in-depth: the HTTP middleware already rejects
	// token-less requests with 401, but this protects STDIO / internal paths and
	// preserves the operator contract that require_oauth=true never falls through to kubeconfig.
	if !hasToken {
		if m.config.IsRequireOAuth() {
			return nil, errors.New("oauth token required")
		}
		logger.V(5).Info("No bearer token in context, falling back to kubeconfig credentials")
		return m.kubernetes, nil
	}

	logger.V(5).Info("Authorization header found (Bearer), using provided bearer token")
	userAgent := CustomUserAgent
	if ua, ok := ctx.Value(UserAgentHeader).(string); ok && ua != "" {
		userAgent = ua
	}
	derivedCfg := &rest.Config{
		Host:    m.kubernetes.RESTConfig().Host,
		APIPath: m.kubernetes.RESTConfig().APIPath,
		// Copy only server verification TLS settings (CA bundle and server name)
		TLSClientConfig: rest.TLSClientConfig{
			Insecure:   m.kubernetes.RESTConfig().Insecure,
			ServerName: m.kubernetes.RESTConfig().ServerName,
			CAFile:     m.kubernetes.RESTConfig().CAFile,
			CAData:     m.kubernetes.RESTConfig().CAData,
		},
		BearerToken: strings.TrimPrefix(authorization, "Bearer "),
		// pass custom UserAgent to identify the client
		UserAgent:   userAgent,
		QPS:         m.kubernetes.RESTConfig().QPS,
		Burst:       m.kubernetes.RESTConfig().Burst,
		Timeout:     m.kubernetes.RESTConfig().Timeout,
		Impersonate: rest.ImpersonationConfig{},
		// Preserve any KUBERNETES_DIAL_ADDR socket override on the derived
		// (OAuth bearer-token) config too, so it dials through the same proxy.
		Dial: m.kubernetes.RESTConfig().Dial,
	}
	clientCmdApiConfig, err := m.kubernetes.clientCmdConfig.RawConfig()
	if err != nil {
		return nil, fmt.Errorf("failed to get kubeconfig: %w", err)
	}
	clientCmdApiConfig.AuthInfos = make(map[string]*clientcmdapi.AuthInfo)
	derived, err := NewKubernetes(ctx, m.config, clientcmd.NewDefaultClientConfig(clientCmdApiConfig, nil), derivedCfg)
	if err != nil {
		return nil, fmt.Errorf("failed to create derived client: %w", err)
	}
	context.AfterFunc(ctx, derived.close)
	return derived, nil
}

// Close releases HTTP transport resources held by this manager.
func (m *Manager) Close() {
	if m != nil {
		m.kubernetes.close()
	}
}

// Invalidate invalidates the cached discovery information.
func (m *Manager) Invalidate() {
	m.kubernetes.DiscoveryClient().Invalidate()
}

// applyRateLimitFromEnv applies QPS and Burst rate limits from environment variables if set.
// This is primarily useful for tests to avoid client-side rate limiting.
// Environment variables:
//   - KUBE_CLIENT_QPS: Sets the QPS (queries per second) limit
//   - KUBE_CLIENT_BURST: Sets the burst limit
func applyRateLimitFromEnv(cfg *rest.Config) {
	if qpsStr := os.Getenv("KUBE_CLIENT_QPS"); qpsStr != "" {
		if qps, err := strconv.ParseFloat(qpsStr, 32); err == nil {
			cfg.QPS = float32(qps)
		}
	}
	if burstStr := os.Getenv("KUBE_CLIENT_BURST"); burstStr != "" {
		if burst, err := strconv.Atoi(burstStr); err == nil {
			cfg.Burst = burst
		}
	}
}
