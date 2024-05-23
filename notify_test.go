package fdstore

import (
	"net"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNotifyConn(t *testing.T) {
	remote, addr := testSocket(t)

	conn, err := net.DialUnix(addr.Net, nil, addr)
	require.NoError(t, err)
	defer conn.Close()

	err = NotifyConn(conn, Ready, Stopping)
	require.NoError(t, err)

	p := make([]byte, 512)
	n, _, err := readMsg(remote, p)
	require.NoError(t, err)
	require.Equal(t, Combine(Ready, Stopping), State(p[:n]))
}

func TestNotify(t *testing.T) {
	remote, addr := testSocket(t)

	err := os.Setenv(NOTIFY_SOCKET, addr.String())
	require.NoError(t, err)

	err = Notify(Ready, Stopping)
	require.NoError(t, err)

	p := make([]byte, 512)
	n, _, err := readMsg(remote, p)
	require.NoError(t, err)
	require.Equal(t, Combine(Ready, Stopping), State(p[:n]))
}

func TestWaitBarrier(t *testing.T) {
	remote, addr := testSocket(t)

	conn, err := net.DialUnix(addr.Net, nil, addr)
	require.NoError(t, err)

	checker := func() {
		p := make([]byte, 512)
		n, barrierFd, err := readMsg(remote, p)
		if !assert.NoError(t, err) {
			return
		}
		if !assert.NotNil(t, barrierFd) {
			return
		}
		if !assert.Equal(t, p[:n], []byte(Barrier)) {
			return
		}
		barrierFd.Close()
	}

	go checker()
	// this will send a fd over the wire it expects to be closed once received
	err = WaitBarrierConn(conn, time.Second*5)
	require.NoError(t, err)

	os.Setenv(NOTIFY_SOCKET, addr.String())
	go checker()
	err = WaitBarrier(time.Second * 5)
	require.NoError(t, err)
}

// Test that WaitBarrier times out if there is no reader
func TestWaitBarrierTimeout(t *testing.T) {
	t.Parallel()

	_, addr := testSocket(t)

	conn, err := net.DialUnix(addr.Net, nil, addr)
	require.NoError(t, err)

	err = WaitBarrierConn(conn, time.Second)
	require.NoError(t, err)
}
