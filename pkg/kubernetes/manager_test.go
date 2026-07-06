package kubernetes

import (
	"net"
	"os"
	"path/filepath"
	"runtime"
	"testing"
	"time"

	"github.com/containers/kubernetes-mcp-server/internal/test"
	"github.com/containers/kubernetes-mcp-server/pkg/config"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	clientcmdapi "k8s.io/client-go/tools/clientcmd/api"
)

type ManagerTestSuite struct {
	suite.Suite
	originalEnv             []string
	originalInClusterConfig func() (*rest.Config, error)
	mockServer              *test.MockServer
}

func (s *ManagerTestSuite) SetupTest() {
	s.originalEnv = os.Environ()
	s.originalInClusterConfig = InClusterConfig
	s.mockServer = test.NewMockServer()
}

func (s *ManagerTestSuite) TearDownTest() {
	test.RestoreEnv(s.originalEnv)
	InClusterConfig = s.originalInClusterConfig
	if s.mockServer != nil {
		s.mockServer.Close()
	}
}

func (s *ManagerTestSuite) TestNewInClusterManager() {
	s.Run("In cluster", func() {
		InClusterConfig = func() (*rest.Config, error) {
			return &rest.Config{}, nil
		}
		s.Run("with default StaticConfig (empty kubeconfig)", func() {
			manager, err := NewInClusterManager(s.T().Context(), &config.StaticConfig{})
			s.Require().NoError(err)
			s.Require().NotNil(manager)
			s.Run("behaves as in cluster", func() {
				rawConfig, err := manager.kubernetes.ToRawKubeConfigLoader().RawConfig()
				s.Require().NoError(err)
				s.Equal("in-cluster", rawConfig.CurrentContext, "expected current context to be 'in-cluster'")
			})
			s.Run("sets default user-agent", func() {
				s.Contains(manager.kubernetes.RESTConfig().UserAgent, "("+runtime.GOOS+"/"+runtime.GOARCH+")")
			})
		})
		s.Run("with explicit kubeconfig", func() {
			manager, err := NewInClusterManager(s.T().Context(), &config.StaticConfig{
				KubeConfig: s.mockServer.KubeconfigFile(s.T()),
			})
			s.Run("returns error", func() {
				s.Error(err)
				s.Nil(manager)
				s.Regexp("kubeconfig file .+ cannot be used with the in-cluster deployments", err.Error())
			})
		})
	})
	s.Run("Out of cluster", func() {
		InClusterConfig = func() (*rest.Config, error) {
			return nil, rest.ErrNotInCluster
		}
		manager, err := NewInClusterManager(s.T().Context(), &config.StaticConfig{})
		s.Run("returns error", func() {
			s.Error(err)
			s.Nil(manager)
			s.ErrorIs(err, ErrorInClusterNotInCluster)
			s.ErrorContains(err, "in-cluster manager cannot be used outside of a cluster")
		})
	})
}

func (s *ManagerTestSuite) TestNewKubeconfigManager() {
	s.Run("Out of cluster", func() {
		InClusterConfig = func() (*rest.Config, error) {
			return nil, rest.ErrNotInCluster
		}
		s.Run("with valid kubeconfig in env", func() {
			kubeconfig := s.mockServer.KubeconfigFile(s.T())
			s.Require().NoError(os.Setenv("KUBECONFIG", kubeconfig))
			manager, err := NewKubeconfigManager(s.T().Context(), &config.StaticConfig{}, "")
			s.Require().NoError(err)
			s.Require().NotNil(manager)
			s.Run("behaves as NOT in cluster", func() {
				rawConfig, err := manager.kubernetes.ToRawKubeConfigLoader().RawConfig()
				s.Require().NoError(err)
				s.NotEqual("in-cluster", rawConfig.CurrentContext, "expected current context to NOT be 'in-cluster'")
				s.Equal("fake-context", rawConfig.CurrentContext, "expected current context to be 'fake-context' as in kubeconfig")
			})
			s.Run("loads correct config", func() {
				s.Contains(manager.kubernetes.ToRawKubeConfigLoader().ConfigAccess().GetLoadingPrecedence(), kubeconfig, "expected kubeconfig path to match")
			})
			s.Run("sets default user-agent", func() {
				s.Contains(manager.kubernetes.RESTConfig().UserAgent, "("+runtime.GOOS+"/"+runtime.GOARCH+")")
			})
			s.Run("rest config host points to mock server", func() {
				s.Equal(s.mockServer.Config().Host, manager.kubernetes.RESTConfig().Host, "expected rest config host to match mock server")
			})
		})
		s.Run("with valid kubeconfig in env and explicit kubeconfig in config", func() {
			kubeconfigInEnv := s.mockServer.KubeconfigFile(s.T())
			s.Require().NoError(os.Setenv("KUBECONFIG", kubeconfigInEnv))
			kubeconfigExplicit := s.mockServer.KubeconfigFile(s.T())
			manager, err := NewKubeconfigManager(s.T().Context(), &config.StaticConfig{
				KubeConfig: kubeconfigExplicit,
			}, "")
			s.Require().NoError(err)
			s.Require().NotNil(manager)
			s.Run("behaves as NOT in cluster", func() {
				rawConfig, err := manager.kubernetes.ToRawKubeConfigLoader().RawConfig()
				s.Require().NoError(err)
				s.NotEqual("in-cluster", rawConfig.CurrentContext, "expected current context to NOT be 'in-cluster'")
				s.Equal("fake-context", rawConfig.CurrentContext, "expected current context to be 'fake-context' as in kubeconfig")
			})
			s.Run("loads correct config (explicit)", func() {
				s.NotContains(manager.kubernetes.ToRawKubeConfigLoader().ConfigAccess().GetLoadingPrecedence(), kubeconfigInEnv, "expected kubeconfig path to NOT match env")
				s.Contains(manager.kubernetes.ToRawKubeConfigLoader().ConfigAccess().GetLoadingPrecedence(), kubeconfigExplicit, "expected kubeconfig path to match explicit")
			})
			s.Run("rest config host points to mock server", func() {
				s.Equal(s.mockServer.Config().Host, manager.kubernetes.RESTConfig().Host, "expected rest config host to match mock server")
			})
		})
		s.Run("with valid kubeconfig in env and explicit kubeconfig context (valid)", func() {
			kubeconfig := s.mockServer.Kubeconfig()
			kubeconfig.Contexts["not-the-mock-server"] = clientcmdapi.NewContext()
			kubeconfig.Contexts["not-the-mock-server"].Cluster = "not-the-mock-server"
			kubeconfig.Clusters["not-the-mock-server"] = clientcmdapi.NewCluster()
			kubeconfig.Clusters["not-the-mock-server"].Server = "https://not-the-mock-server:6443" // REST configuration should point to mock server, not this
			kubeconfig.CurrentContext = "not-the-mock-server"
			kubeconfigFile := test.KubeconfigFile(s.T(), kubeconfig)
			s.Require().NoError(os.Setenv("KUBECONFIG", kubeconfigFile))
			manager, err := NewKubeconfigManager(s.T().Context(), &config.StaticConfig{}, "fake-context") // fake-context is the one mock-server serves
			s.Require().NoError(err)
			s.Require().NotNil(manager)
			s.Run("behaves as NOT in cluster", func() {
				rawConfig, err := manager.kubernetes.ToRawKubeConfigLoader().RawConfig()
				s.Require().NoError(err)
				s.NotEqual("in-cluster", rawConfig.CurrentContext, "expected current context to NOT be 'in-cluster'")
				s.Equal("not-the-mock-server", rawConfig.CurrentContext, "expected current context to be 'not-the-mock-server' as in explicit context")
			})
			s.Run("loads correct config", func() {
				s.Contains(manager.kubernetes.ToRawKubeConfigLoader().ConfigAccess().GetLoadingPrecedence(), kubeconfigFile, "expected kubeconfig path to match")
			})
			s.Run("rest config host points to mock server", func() {
				s.Equal(s.mockServer.Config().Host, manager.kubernetes.RESTConfig().Host, "expected rest config host to match mock server")
			})
		})
		s.Run("with valid kubeconfig in env and explicit kubeconfig context (invalid)", func() {
			kubeconfigInEnv := s.mockServer.KubeconfigFile(s.T())
			s.Require().NoError(os.Setenv("KUBECONFIG", kubeconfigInEnv))
			manager, err := NewKubeconfigManager(s.T().Context(), &config.StaticConfig{}, "i-do-not-exist")
			s.Run("returns error", func() {
				s.Error(err)
				s.Nil(manager)
				s.ErrorContains(err, `failed to create kubernetes rest config from kubeconfig: context "i-do-not-exist" does not exist`)
			})
		})
		s.Run("with invalid path kubeconfig in env", func() {
			s.Require().NoError(os.Setenv("KUBECONFIG", "i-dont-exist"))
			manager, err := NewKubeconfigManager(s.T().Context(), &config.StaticConfig{}, "")
			s.Run("returns error", func() {
				s.Error(err)
				s.Nil(manager)
				s.ErrorContains(err, "no current-context is set and no contexts are defined")
			})
		})
		s.Run("with empty kubeconfig in env", func() {
			kubeconfigPath := filepath.Join(s.T().TempDir(), "config")
			s.Require().NoError(os.WriteFile(kubeconfigPath, []byte(""), 0644))
			s.Require().NoError(os.Setenv("KUBECONFIG", kubeconfigPath))
			manager, err := NewKubeconfigManager(s.T().Context(), &config.StaticConfig{}, "")
			s.Run("returns error", func() {
				s.Error(err)
				s.Nil(manager)
				s.ErrorContains(err, "no current-context is set and no contexts are defined")
			})
		})
		s.Run("with empty current-context and single context auto-selects it", func() {
			kubeconfig := s.mockServer.Kubeconfig()
			kubeconfig.CurrentContext = ""
			kubeconfigFile := test.KubeconfigFile(s.T(), kubeconfig)
			s.Require().NoError(os.Setenv("KUBECONFIG", kubeconfigFile))
			manager, err := NewKubeconfigManager(s.T().Context(), &config.StaticConfig{}, "")
			s.Require().NoError(err)
			s.Require().NotNil(manager)
			s.Run("rest config host points to mock server", func() {
				s.Equal(s.mockServer.Config().Host, manager.kubernetes.RESTConfig().Host)
			})
		})
		s.Run("with empty current-context and multiple contexts returns error", func() {
			kubeconfig := s.mockServer.Kubeconfig()
			kubeconfig.CurrentContext = ""
			kubeconfig.Contexts["another-context"] = clientcmdapi.NewContext()
			kubeconfigFile := test.KubeconfigFile(s.T(), kubeconfig)
			s.Require().NoError(os.Setenv("KUBECONFIG", kubeconfigFile))
			manager, err := NewKubeconfigManager(s.T().Context(), &config.StaticConfig{}, "")
			s.Run("returns error listing available contexts", func() {
				s.Error(err)
				s.Nil(manager)
				s.ErrorContains(err, "current-context is not set")
				s.ErrorContains(err, "kubectl config use-context")
			})
		})
		s.Run("with empty current-context and no contexts returns error", func() {
			kubeconfig := clientcmdapi.NewConfig()
			kubeconfigFile := test.KubeconfigFile(s.T(), kubeconfig)
			s.Require().NoError(os.Setenv("KUBECONFIG", kubeconfigFile))
			manager, err := NewKubeconfigManager(s.T().Context(), &config.StaticConfig{}, "")
			s.Run("returns error", func() {
				s.Error(err)
				s.Nil(manager)
				s.ErrorContains(err, "no current-context is set and no contexts are defined")
			})
		})
	})
	s.Run("In cluster", func() {
		InClusterConfig = func() (*rest.Config, error) {
			return &rest.Config{}, nil
		}
		manager, err := NewKubeconfigManager(s.T().Context(), &config.StaticConfig{}, "")
		s.Run("returns error", func() {
			s.Error(err)
			s.Nil(manager)
			s.ErrorIs(err, ErrorKubeconfigInClusterNotAllowed)
			s.ErrorContains(err, "kubeconfig manager cannot be used in in-cluster deployments")
		})
	})
}

func (s *ManagerTestSuite) TestNewManager() {
	s.Run("with nil config returns error", func() {
		manager, err := NewManager(s.T().Context(), nil, &rest.Config{}, clientcmd.NewDefaultClientConfig(clientcmdapi.Config{}, nil))
		s.Require().Error(err)
		s.EqualError(err, "config cannot be nil", "expected 'config cannot be nil' error")
		s.Nil(manager, "expected nil manager when config is nil")
	})

	s.Run("with nil restConfig returns error", func() {
		manager, err := NewManager(s.T().Context(), &config.StaticConfig{}, nil, clientcmd.NewDefaultClientConfig(clientcmdapi.Config{}, nil))
		s.Require().Error(err)
		s.EqualError(err, "restConfig cannot be nil", "expected 'restConfig cannot be nil' error")
		s.Nil(manager, "expected nil manager when restConfig is nil")
	})

	s.Run("with nil clientCmdConfig returns error", func() {
		manager, err := NewManager(s.T().Context(), &config.StaticConfig{}, &rest.Config{}, nil)
		s.Require().Error(err)
		s.EqualError(err, "clientCmdConfig cannot be nil", "expected 'clientCmdConfig cannot be nil' error")
		s.Nil(manager, "expected nil manager when clientCmdConfig is nil")
	})

	s.Run("with all nil parameters returns config error first", func() {
		manager, err := NewManager(s.T().Context(), nil, nil, nil)
		s.Require().Error(err)
		s.EqualError(err, "config cannot be nil", "expected 'config cannot be nil' error as first check")
		s.Nil(manager, "expected nil manager when all parameters are nil")
	})
}

func TestManager(t *testing.T) {
	suite.Run(t, new(ManagerTestSuite))
}

// fakeInlineKubeconfig builds a minimal single-cluster/single-context/token-auth
// kubeconfig, serialized to bytes as NewInlineKubeconfigManager consumes it.
// The host deliberately does not resolve, so any test that reaches out over
// the network without a dial-addr override would hang/fail on DNS, proving the
// override (not the real host) is what's actually dialed.
func fakeInlineKubeconfig(t *testing.T, server string) []byte {
	t.Helper()
	cfg := clientcmdapi.NewConfig()
	cfg.Clusters["fake"] = clientcmdapi.NewCluster()
	cfg.Clusters["fake"].Server = server
	cfg.AuthInfos["fake"] = clientcmdapi.NewAuthInfo()
	cfg.AuthInfos["fake"].Token = "fake-token"
	cfg.Contexts["fake-context"] = clientcmdapi.NewContext()
	cfg.Contexts["fake-context"].Cluster = "fake"
	cfg.Contexts["fake-context"].AuthInfo = "fake"
	cfg.CurrentContext = "fake-context"
	raw, err := clientcmd.Write(*cfg)
	require.NoError(t, err)
	return raw
}

func TestNewInlineKubeconfigManager(t *testing.T) {
	kubeconfig := fakeInlineKubeconfig(t, "https://kubernetes.does-not-resolve.internal:6443")
	m, err := NewInlineKubeconfigManager(t.Context(), &config.StaticConfig{}, kubeconfig, "")
	require.NoError(t, err)
	require.NotNil(t, m)
	t.Cleanup(m.Close)
}

func TestNewInlineKubeconfigManagerDialAddr(t *testing.T) {
	// A local TCP listener plays the tunnel proxy. Building the manager with
	// dialAddr and making any API call must open a socket to the listener
	// even though the kubeconfig host cannot resolve.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	connCh := make(chan struct{}, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			connCh <- struct{}{}
			_ = conn.Close()
		}
	}()

	kubeconfig := fakeInlineKubeconfig(t, "https://kubernetes.does-not-resolve.internal:6443")
	m, err := NewInlineKubeconfigManager(t.Context(), &config.StaticConfig{}, kubeconfig, ln.Addr().String())
	require.NoError(t, err)
	t.Cleanup(m.Close)

	// Trigger one real connection attempt (TLS will fail — we only assert the socket landed on the listener).
	_, _ = m.kubernetes.DiscoveryClient().ServerVersion()

	select {
	case <-connCh:
	case <-time.After(5 * time.Second):
		t.Fatal("no connection reached the dial-addr listener")
	}
}

func TestNewInlineKubeconfigManagerIgnoresEnv(t *testing.T) {
	// The inline/multi-tenant path must never be influenced by the process-wide
	// stdio knobs: a stray KUBECONFIG_YAML/KUBERNETES_DIAL_ADDR in the shared
	// server's environment must not leak into (or override) a tenant's
	// request-supplied kubeconfig and dial-addr. A listener stands in for
	// KUBERNETES_DIAL_ADDR's target: it must never see a connection, since the
	// caller passed dialAddr="" and the kubeconfig host cannot resolve.
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	connCh := make(chan struct{}, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			connCh <- struct{}{}
			_ = conn.Close()
		}
	}()

	t.Setenv("KUBECONFIG_YAML", "not valid kubeconfig yaml")
	t.Setenv("KUBERNETES_DIAL_ADDR", ln.Addr().String())

	kubeconfig := fakeInlineKubeconfig(t, "https://kubernetes.does-not-resolve.internal:6443")
	m, err := NewInlineKubeconfigManager(t.Context(), &config.StaticConfig{}, kubeconfig, "")
	require.NoError(t, err)
	t.Cleanup(m.Close)

	_, _ = m.kubernetes.DiscoveryClient().ServerVersion()

	select {
	case <-connCh:
		t.Fatal("KUBERNETES_DIAL_ADDR env leaked into the inline manager's dial")
	case <-time.After(3 * time.Second):
		// No connection reached the env-configured listener, as expected.
	}
}
