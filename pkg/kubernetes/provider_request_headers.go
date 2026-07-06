package kubernetes

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/containers/kubernetes-mcp-server/pkg/api"
	"k8s.io/apimachinery/pkg/runtime/schema"
)

func init() {
	RegisterProvider(api.ClusterProviderRequestHeaders, newRequestHeadersClusterProvider)
}

const (
	requestHeadersManagerCap = 64
	requestHeadersManagerTTL = 30 * time.Minute
)

type cachedManager struct {
	manager  *Manager
	lastUsed time.Time
}

// requestHeadersClusterProvider serves one shared multi-tenant HTTP process:
// every request carries its own kubeconfig (X-Kubeconfig) and optional socket
// dial override (X-Kubernetes-Dial-Addr). Managers are cached per
// (kubeconfig, dialAddr) hash because each Manager owns discovery/RESTMapper
// caches and a TLS transport that are expensive to rebuild per call; the cache
// is bounded and idle entries are Closed on eviction.
type requestHeadersClusterProvider struct {
	mu       sync.Mutex
	config   api.BaseConfig
	managers map[string]*cachedManager
}

var _ Provider = &requestHeadersClusterProvider{}

func newRequestHeadersClusterProvider(_ context.Context, cfg api.BaseConfig) (Provider, error) {
	return &requestHeadersClusterProvider{
		config:   cfg,
		managers: make(map[string]*cachedManager),
	}, nil
}

// requestHeadersCacheKey identifies one tenant connection: the same kubeconfig
// tunneled through different connectors (dial addrs) must not share a manager.
func requestHeadersCacheKey(kubeconfig, dialAddr string) string {
	sum := sha256.Sum256([]byte(kubeconfig + "\x00" + dialAddr))
	return hex.EncodeToString(sum[:])
}

func (p *requestHeadersClusterProvider) managerForRequest(ctx context.Context) (*Manager, error) {
	kubeconfig, _ := ctx.Value(KubeconfigHeader).(string)
	if kubeconfig == "" {
		return nil, fmt.Errorf("missing %s header: the request-headers cluster provider requires a per-request kubeconfig", KubeconfigHeader)
	}
	dialAddr, _ := ctx.Value(DialAddrHeader).(string)

	key := requestHeadersCacheKey(kubeconfig, dialAddr)

	p.mu.Lock()
	defer p.mu.Unlock()
	now := time.Now()
	if entry, ok := p.managers[key]; ok {
		entry.lastUsed = now
		return entry.manager, nil
	}
	p.evictLocked(now)
	m, err := NewInlineKubeconfigManager(ctx, p.config, []byte(kubeconfig), dialAddr)
	if err != nil {
		return nil, err
	}
	p.managers[key] = &cachedManager{manager: m, lastUsed: now}
	return m, nil
}

// evictLocked drops idle managers past TTL, then the least-recently-used
// entries until there is room for one more insert. Callers hold p.mu.
func (p *requestHeadersClusterProvider) evictLocked(now time.Time) {
	for key, entry := range p.managers {
		if now.Sub(entry.lastUsed) > requestHeadersManagerTTL {
			entry.manager.Close()
			delete(p.managers, key)
		}
	}
	for len(p.managers) >= requestHeadersManagerCap {
		oldestKey := ""
		var oldest time.Time
		for key, entry := range p.managers {
			if oldestKey == "" || entry.lastUsed.Before(oldest) {
				oldestKey, oldest = key, entry.lastUsed
			}
		}
		p.managers[oldestKey].manager.Close()
		delete(p.managers, oldestKey)
	}
}

func (p *requestHeadersClusterProvider) GetDerivedKubernetes(ctx context.Context, _ string) (*Kubernetes, error) {
	m, err := p.managerForRequest(ctx)
	if err != nil {
		return nil, err
	}
	return m.Derived(ctx)
}

func (p *requestHeadersClusterProvider) IsOpenShift(ctx context.Context) bool {
	// Toolset registration probes this at startup with no tenant on the
	// context; the shared tool surface is the vanilla-Kubernetes one, so a
	// missing kubeconfig is soft-false rather than an error.
	m, err := p.managerForRequest(ctx)
	if err != nil {
		return false
	}
	return m.IsOpenShift(ctx)
}

func (p *requestHeadersClusterProvider) IsMultiTarget() bool {
	return false
}

func (p *requestHeadersClusterProvider) GetDefaultTarget() string {
	return ""
}

func (p *requestHeadersClusterProvider) GetTargetParameterName() string {
	return ""
}

func (p *requestHeadersClusterProvider) GetTargets(_ context.Context) ([]string, error) {
	return []string{""}, nil
}

func (p *requestHeadersClusterProvider) WatchTargets(_ context.Context, _ McpReload) {
	// The tool surface is tenant-independent; there is nothing to watch.
}

// HasGVKs reports true unconditionally: GVK availability is per-tenant and
// unknowable at (startup-time) tool registration, so all tools stay visible.
func (p *requestHeadersClusterProvider) HasGVKs(_ context.Context, _ []schema.GroupVersionKind) bool {
	return true
}

func (p *requestHeadersClusterProvider) Close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for key, entry := range p.managers {
		entry.manager.Close()
		delete(p.managers, key)
	}
}
