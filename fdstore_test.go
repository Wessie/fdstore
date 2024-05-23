package fdstore

import (
	"os"
	"testing"
	"testing/quick"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseName(t *testing.T) {
	f := func(data shared) bool {
		entry := Entry{shared: data}

		filename := entry.fileName()
		p, err := parseName(filename)
		if err != nil {
			return false
		}

		if !assert.Equal(t, entry.ID, p.id) {
			return false
		}
		if !assert.Equal(t, entry.Name, p.name) {
			return false
		}
		if !assert.True(t, p.isFile, "isFile should be true") {
			return false
		}

		dataname := entry.dataName()
		p, err = parseName(dataname)
		if err != nil {
			return false
		}

		if !assert.Equal(t, entry.ID, p.id) {
			return false
		}
		if !assert.Equal(t, entry.Name, p.name) {
			return false
		}
		if !assert.True(t, p.isData, "isData should be true") {
			return false
		}
		return true
	}
	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
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
