package kubernetes

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/containers/kubernetes-mcp-server/pkg/config"
)

// requestHeadersCtx plants the decoded kubeconfig (and optional dial-addr
// override) on the context exactly as kubeconfigPropagationMiddleware does.
func requestHeadersCtx(t *testing.T, kubeconfig []byte, dialAddr string) context.Context {
	t.Helper()
	ctx := context.WithValue(t.Context(), KubeconfigHeader, string(kubeconfig))
	if dialAddr != "" {
		ctx = context.WithValue(ctx, DialAddrHeader, dialAddr)
	}
	return ctx
}

func TestRequestHeadersProviderRequiresKubeconfig(t *testing.T) {
	p, err := newRequestHeadersClusterProvider(t.Context(), &config.StaticConfig{})
	require.NoError(t, err)
	t.Cleanup(p.Close)

	_, err = p.GetDerivedKubernetes(t.Context(), "")
	require.ErrorContains(t, err, "X-Kubeconfig")
}

func TestRequestHeadersProviderCachesByConfigAndDialAddr(t *testing.T) {
	p, err := newRequestHeadersClusterProvider(t.Context(), &config.StaticConfig{})
	require.NoError(t, err)
	t.Cleanup(p.Close)
	rhp := p.(*requestHeadersClusterProvider)

	kubeconfig := fakeInlineKubeconfig(t, "https://kubernetes.does-not-resolve.internal:6443")

	// Two requests with the same kubeconfig + dialAddr share one manager.
	k1, err := p.GetDerivedKubernetes(requestHeadersCtx(t, kubeconfig, "127.0.0.1:19999"), "")
	require.NoError(t, err)
	require.NotNil(t, k1)
	k2, err := p.GetDerivedKubernetes(requestHeadersCtx(t, kubeconfig, "127.0.0.1:19999"), "")
	require.NoError(t, err)
	require.Same(t, k1, k2)
	require.Len(t, rhp.managers, 1)

	// A different dialAddr is a different tenant socket: second manager.
	k3, err := p.GetDerivedKubernetes(requestHeadersCtx(t, kubeconfig, "127.0.0.1:29999"), "")
	require.NoError(t, err)
	require.NotSame(t, k1, k3)
	require.Len(t, rhp.managers, 2)

	// A different kubeconfig is a different tenant: third manager.
	otherKubeconfig := fakeInlineKubeconfig(t, "https://kubernetes.also-does-not-resolve.internal:6443")
	_, err = p.GetDerivedKubernetes(requestHeadersCtx(t, otherKubeconfig, "127.0.0.1:19999"), "")
	require.NoError(t, err)
	require.Len(t, rhp.managers, 3)
}

func TestRequestHeadersProviderEviction(t *testing.T) {
	p, err := newRequestHeadersClusterProvider(t.Context(), &config.StaticConfig{})
	require.NoError(t, err)
	t.Cleanup(p.Close)
	rhp := p.(*requestHeadersClusterProvider)

	kubeconfig := fakeInlineKubeconfig(t, "https://kubernetes.does-not-resolve.internal:6443")

	// Fill the cache to capacity with distinct dial addrs.
	for i := 0; i < requestHeadersManagerCap; i++ {
		_, err := p.GetDerivedKubernetes(requestHeadersCtx(t, kubeconfig, fmt.Sprintf("127.0.0.1:%d", 10000+i)), "")
		require.NoError(t, err)
	}
	require.Len(t, rhp.managers, requestHeadersManagerCap)

	// Touch entry 0 so entry 1 becomes the least-recently-used.
	_, err = p.GetDerivedKubernetes(requestHeadersCtx(t, kubeconfig, "127.0.0.1:10000"), "")
	require.NoError(t, err)
	lruKey := requestHeadersCacheKey(string(kubeconfig), "127.0.0.1:10001")
	require.Contains(t, rhp.managers, lruKey)

	// The next insert evicts the LRU entry; the map stays at capacity.
	_, err = p.GetDerivedKubernetes(requestHeadersCtx(t, kubeconfig, "127.0.0.1:20000"), "")
	require.NoError(t, err)
	require.Len(t, rhp.managers, requestHeadersManagerCap)
	require.NotContains(t, rhp.managers, lruKey)
	require.Contains(t, rhp.managers, requestHeadersCacheKey(string(kubeconfig), "127.0.0.1:10000"))
	require.Contains(t, rhp.managers, requestHeadersCacheKey(string(kubeconfig), "127.0.0.1:20000"))
}
