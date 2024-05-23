package fdstore

import (
	"fmt"
	"os"
	"testing"

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

func TestStoreAddRemove(t *testing.T) {
	store := NewStore(nil)
	defer store.Close()

	a, data := testFile(t), []byte(nil)
	store.AddFile(a, "A", data)
	entries := store.RemoveFile("A")
	require.Len(t, entries, 1)
	require.Equal(t, entries[0].File, a)
	require.Equal(t, entries[0].Data, data)
}

func testFile(t *testing.T) *os.File {
	f, err := os.CreateTemp("", "test")
	require.NoError(t, err)
	t.Cleanup(func() {
		f.Close()
		os.Remove(f.Name())
	})
	return f
}
