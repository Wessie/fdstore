package fdstore

import (
	"crypto/rand"
	"io"
	"os"
	"testing"
	"testing/quick"

	"github.com/justincormack/go-memfd"
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
	store := NewStore()
	defer store.Close()

	a, data := testFile(t), []byte(nil)
	err := store.AddFile(a, "A", data)
	require.NoError(t, err)
	entries := store.RemoveFile("A")
	require.Len(t, entries, 1)
	compareFiles(t, entries[0].File, a)
	require.Equal(t, entries[0].Data, data)
}

func testFile(t *testing.T) *os.File {
	f, err := memfd.Create()
	require.NoError(t, err)
	t.Cleanup(func() {
		f.Close()
	})

	return f.File
}

func TestBytesToFd(t *testing.T) {
	data := make([]byte, 4096)
	_, err := rand.Read(data)
	require.NoError(t, err)

	f, err := bytesToFd(data, "test")
	require.NoError(t, err)
	defer f.Close()

	out, err := io.ReadAll(f)
	require.NoError(t, err)

	require.Equal(t, data, out)
}

func TestDataToFd(t *testing.T) {
	data := make([]byte, 4096)
	_, err := rand.Read(data)
	require.NoError(t, err)

	entry := newEntry("test", nil, data)
	f, err := entry.dataToFd()
	require.NoError(t, err)
	defer f.Close()

	out, err := io.ReadAll(f)
	require.NoError(t, err)

	require.Equal(t, data, out)
}

func TestStoreReadFilelist(t *testing.T) {
	var entries []Entry

	for range 20 {
		data := make([]byte, 4096)
		_, err := rand.Read(data)
		require.NoError(t, err)
		file := testFile(t)

		entries = append(entries, newEntry("test", file, data))
	}

	filelist := make(map[string][]*os.File)
	for _, e := range entries {
		filelist[e.fileName()] = append(filelist[e.fileName()], e.File)
		data, err := e.dataToFd()
		require.NoError(t, err)
		filelist[e.dataName()] = append(filelist[e.dataName()], data)
	}

	store := NewStore()
	require.NotNil(t, store)
	store.RestoreFilelist(filelist)

	out := store.RemoveFile("test")
	require.Len(t, out, len(entries))
	require.Len(t, filelist, 0)
}
