package fdstore

import (
	"crypto/rand"
	"net"
	"os"
	"syscall"
	"testing"

	"github.com/justincormack/go-memfd"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func testSocket(t *testing.T) (*net.UnixConn, *net.UnixAddr) {
	dir, err := os.MkdirTemp("", "")
	require.NoError(t, err)
	t.Cleanup(func() {
		os.RemoveAll(dir)
	})

	addr := &net.UnixAddr{
		Name: dir + "sock",
		Net:  "unixgram",
	}
	conn, err := net.DialUnix(addr.Net, addr, nil)
	require.NoError(t, err)
	t.Cleanup(func() {
		conn.Close()

	})
	return conn, conn.LocalAddr().(*net.UnixAddr)
}
func TestSendReadMsg(t *testing.T) {
	// generate a socket for testing
	remote, addr := testSocket(t)

	// connect to our remote test socket
	conn, err := net.DialUnix(addr.Net, nil, addr)
	require.NoError(t, err)
	defer conn.Close()

	t.Run("OnlyData", func(t *testing.T) {
		// generate some random data
		data := make([]byte, 1024)
		_, err := rand.Read(data)
		require.NoError(t, err)

		// send the data with no file
		err = sendMsg(conn, data, nil)
		require.NoError(t, err)

		// see if we can receive it back
		p := make([]byte, len(data))
		n, file, err := readMsg(remote, p)
		require.NoError(t, err)
		require.Equal(t, data, p[:n])
		require.Nil(t, file)
	})

	t.Run("WithFile", func(t *testing.T) {
		// generate some random data
		data := make([]byte, 1024)
		_, err := rand.Read(data)
		require.NoError(t, err)

		// create a memfd file we can pass over
		mfd, err := memfd.Create()
		require.NoError(t, err)
		defer mfd.Close()
		inFile := mfd.File

		// send the data and file
		err = sendMsg(conn, data, inFile)
		require.NoError(t, err)

		// see if we get it back
		p := make([]byte, len(data))
		n, outFile, err := readMsg(remote, p)
		require.NoError(t, err)
		require.Equal(t, data, p[:n])
		require.NotNil(t, outFile)
		compareFiles(t, inFile, outFile)
	})
}

// compareFiles checks if two files point to the same underlying file
func compareFiles(t *testing.T, a, b *os.File) bool {
	ai, _ := a.Stat()
	bi, _ := b.Stat()

	return assert.Equal(t, ai.Sys().(*syscall.Stat_t).Dev, bi.Sys().(*syscall.Stat_t).Dev)
}

func TestNotifySocket(t *testing.T) {
	_, addr := testSocket(t)

	err := os.Setenv(NOTIFY_SOCKET, addr.String())
	require.NoError(t, err)

	path := os.Getenv(NOTIFY_SOCKET)
	require.Equal(t, addr.String(), path)

	conn, err := NotifySocket()
	require.NoError(t, err)
	defer conn.Close()
	require.NotNil(t, conn)
	require.Equal(t, addr.String(), conn.RemoteAddr().String())
}
