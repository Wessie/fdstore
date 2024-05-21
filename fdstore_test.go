package fdstore

import (
	"fmt"
	"os"
	"syscall"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseName(t *testing.T) {
	p, err := parseName("SOURCE-50-data")
	fmt.Println(p, err)
	t.Fail()
}

func TestFDNameSuffixLength(t *testing.T) {
	require.Equal(t, len(fileSuffix), len(dataSuffix), "file and data suffix should have the same length")
}

func TestSendReadMsg(t *testing.T) {
	a, b, err := SocketPair()
	require.NoError(t, err)

	f, err := os.CreateTemp("", "test")
	require.NoError(t, err)
	defer f.Close()
	defer os.Remove(f.Name())

	original := []byte("garbage")
	err = sendMsg(a, nil, original, f)
	require.NoError(t, err)

	state := make([]byte, 32000)

	n, bf, err := readMsg(b, state)
	require.NoError(t, err)
	defer bf.Close()

	require.Equal(t, original, state[:n])
	compareFiles(t, f, bf)
}

func compareFiles(t *testing.T, a, b *os.File) bool {
	ai, _ := a.Stat()
	bi, _ := b.Stat()

	return assert.Equal(t, ai.Sys().(*syscall.Stat_t).Dev, bi.Sys().(*syscall.Stat_t).Dev)
}
