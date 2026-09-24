package vsphere

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestNewSessionManagerTransportHonoursProxyEnv guards against a bare
// &http.Transport{} literal, which leaves Proxy nil and silently ignores
// HTTP_PROXY/HTTPS_PROXY/NO_PROXY -- unlike http.DefaultTransport, which sets
// Proxy: http.ProxyFromEnvironment. A deployment that reaches the session
// manager through an egress proxy would otherwise always connect directly and
// fail (or bypass network policy meant to route through the proxy).
//
// Asserted by comparing function pointers via reflection rather than a live
// request: ProxyFromEnvironment's environment lookup is cached process-wide
// behind a sync.Once the first time it's actually invoked, and every other
// test in this file calls GetSharedToken for real, so a live end-to-end proxy
// test would be order-dependent and unreliable.
func TestNewSessionManagerTransportHonoursProxyEnv(t *testing.T) {
	transport := newSessionManagerTransport(SharedTokenOptions{})

	require.NotNil(t, transport.Proxy, "Proxy must not be nil, or HTTP_PROXY/HTTPS_PROXY/NO_PROXY are silently ignored")
	assert.Equal(t,
		reflect.ValueOf(http.ProxyFromEnvironment).Pointer(),
		reflect.ValueOf(transport.Proxy).Pointer(),
		"Proxy should be http.ProxyFromEnvironment")
}
