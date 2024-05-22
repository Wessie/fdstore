package fdstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/justincormack/go-memfd"
)

var (
	ErrShortWrite = errors.New("short write")
	ErrNoFile     = errors.New("File method missing")
	ErrNoFiles    = errors.New("received no files from parent")
	ErrNoSocket   = errors.New("NOTIFY_SOCKET envvar is empty")
	ErrNotOurName = errors.New("FDNAME was not the correct format")
	ErrWrongType  = errors.New("fd is of the wrong type")
	ErrDataRead   = errors.New("failed to read data from memfd")
)

const (
	// suffixes we use to append to the FDNAME's we send, these should
	// be both the same length
	dataSuffix = "-data"
	fileSuffix = "-file"
)

type Filer interface {
	File() (*os.File, error)
}

func NewEntry(name string, file *os.File, data []byte) Entry {
	return Entry{
		shared: shared{
			ID:   incrementalID.Add(1),
			Name: name,
			Data: data,
		},
		File: file,
	}
}

type shared struct {
	// ID is a 'unique' id for this entry, it's an atomically incrementing integer,
	// used to match the File and Data together after passing over
	ID uint64
	// Name is the name for this entry, this is how the user looks us up
	Name string
	// Data is the data associated with this entry, this can be anything
	Data []byte
}

type Entry struct {
	shared
	// File is the file associated with this entry
	File *os.File
}

// ConnEntry is the type returned by the RemoveConn helper
type ConnEntry struct {
	shared
	Conn net.Conn
}

// ListenerEntry is the type returned by the RemoveListener helper
type ListenerEntry struct {
	shared
	Listener net.Listener
}

func (e Entry) dataToFd() (*os.File, error) {
	return bytesToFd(e.Data, e.Name)
}

func (e Entry) dataName() string {
	return e.Name + "-" + strconv.FormatUint(e.ID, 10) + dataSuffix
}

func (e Entry) fileName() string {
	return e.Name + "-" + strconv.FormatUint(e.ID, 10) + fileSuffix
}

func bytesToFd(data []byte, name string) (*os.File, error) {
	fd, err := memfd.CreateNameFlags(name, memfd.Cloexec|memfd.AllowSealing)
	if err != nil {
		return nil, err
	}

	n, err := io.Copy(fd, bytes.NewReader(data))
	if err != nil {
		fd.Close()
		return nil, err
	}
	if n != int64(len(data)) {
		fd.Close()
		return nil, ErrShortWrite
	}
	return fd.File, nil
}

var incrementalID = new(atomic.Uint64)

type parsedName struct {
	id     uint64
	name   string
	isData bool
	isFile bool
}

func parseName(name string) (parsedName, error) {
	pn := parsedName{
		isFile: strings.HasSuffix(name, fileSuffix),
		isData: strings.HasSuffix(name, dataSuffix),
	}

	if !pn.isFile && !pn.isData {
		// neither suffix was found, this probably means we're not handling names
		// we made, so error out
		return parsedName{}, fmt.Errorf("%w: expected file or data suffix: %s", ErrNotOurName, name)
	}

	// suffix was found, cut it off and continue
	name = name[:len(name)-5]

	// next should be our ID suffixed to the end
	i := strings.LastIndexByte(name, '-')
	if i < 0 {
		// no other '-' found, again probably means this name wasn't ours
		return parsedName{}, fmt.Errorf("%w: expected id suffix: %s", ErrNotOurName, name)
	}

	// parse our ID as an uint64
	id, err := strconv.ParseUint(name[i+1:], 10, 64)
	if err != nil {
		// the suffix we did find wasn't an integer
		return parsedName{}, fmt.Errorf("%w: expected id to be uint64: %s", ErrNotOurName, name)
	}
	pn.id = id
	// and cut off the id to get back our original name
	pn.name = name[:i]
	return pn, nil
}

type Store struct {
	mu      sync.Mutex
	entries map[uint64]Entry
}

// Close calls close on all the entries contained in the store and makes the store
// invalid to use
func (s *Store) Close() {
	s.mu.Lock()
	defer s.mu.Unlock()

	for _, entry := range s.entries {
		entry.File.Close()
	}

	s.entries = nil
}

// NewStore takes a map of `name: files` and tries to turn them back into a Store instance, this
// generally should be the output of ListenFDs after a store by SendStore in a previous process.
// If filelist is nil an empty Store is returned.
//
// Entries that do not match the `{name}-{id}-[data|file]` format are ignored, entries that are interpreted
// successfully are removed from the input filelist and added to the returned Store instead.
func NewStore(filelist map[string][]*os.File) *Store {
	store := Store{
		entries: make(map[uint64]Entry),
	}

	for name, files := range filelist {
		// we only expect one file per name due to our ID and suffix system, but there could
		// be fds stored that we didn't add so we just ignore those
		if len(files) != 1 {
			continue
		}
		file := files[0]

		p, err := parseName(name)
		if err != nil {
			// if we're unable to parse the name it could be an fd stored by someone else, so
			// again we're gonna ignore it
			log.Println(err)
			continue
		}

		entry := store.entries[p.id]
		entry.ID = p.id
		entry.Name = p.name

		if p.isData { // data fd
			defer file.Close()
			// read the whole file into memory
			data, err := io.ReadAll(file)
			if err != nil {
				log.Println(fmt.Errorf("%w: %w", ErrDataRead, err))
				continue
			}
			entry.Data = data
		}

		if p.isFile {
			entry.File = file
		}

		store.entries[p.id] = entry
		// once we're done, remove us from the fileList
		delete(filelist, name)
	}

	return &store
}

func asConnWithoutClose(file *os.File) (net.Conn, error) {
	conn, err := net.FileConn(file)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWrongType, err)
	}
	return conn, nil
}

// AsConn is a helper function that tries to convert the file given
// to a net.Conn, the passed in file is closed on success
func AsConn(file *os.File) (net.Conn, error) {
	conn, err := asConnWithoutClose(file)
	if err != nil {
		return nil, err
	}
	file.Close()
	return conn, nil
}

func asListenerWithoutClose(file *os.File) (net.Listener, error) {
	ln, err := net.FileListener(file)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrWrongType, err)
	}
	return ln, nil
}

// AsListener is a helper function that tries to convert the file given
// to a net.Listener, the passed in file is closed on success
func AsListener(file *os.File) (net.Listener, error) {
	ln, err := asListenerWithoutClose(file)
	if err != nil {
		return nil, err
	}
	file.Close()
	return ln, nil
}

func SendStore(conn *net.UnixConn, s *Store) error {
	addr := conn.RemoteAddr().(*net.UnixAddr)

	for _, e := range s.entries {
		// create our sd_notify state
		state := combine(FDStore, FDName(e.fileName()))

		err := sendMsg(conn, addr, []byte(state), e.File)
		if err != nil {
			return fmt.Errorf("failed to send file message: %w", err)
		}

		// then prep our data
		dataFd, err := e.dataToFd()
		if err != nil {
			return fmt.Errorf("failed to prep data memfd: %w", err)
		}
		defer dataFd.Close()

		// create our sd_notify state
		state = combine(FDStore, FDName(e.dataName()))

		err = sendMsg(conn, addr, []byte(state), dataFd)
		if err != nil {
			return fmt.Errorf("failed to send data message: %w", err)
		}
	}

	return nil
}

// RemoveFile finds and removes the entries associated with the name given
// and returns them.
func (s *Store) RemoveFile(name string) []Entry {
	s.mu.Lock()
	defer s.mu.Unlock()

	var res []Entry
	for id, entry := range s.entries {
		if entry.Name != name {
			continue
		}

		res = append(res, entry)
		delete(s.entries, id)
	}
	return res
}

// RemoveConn finds and removes the entries associated with the name given
// and tries to return them as net.Conn, entries are not removed or returned
// if an error occurs.
//
// Returns error matching ErrWrongType if an entry could not be converted
func (s *Store) RemoveConn(name string) (res []ConnEntry, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	defer func() {
		// if we error, we need to close all the conns that we already made
		// before returning, or we would be leaking their fds
		if err == nil {
			return
		}

		for _, e := range res {
			e.Conn.Close()
		}

		res = nil
	}()

	var ids []uint64 // ids that match our name, for deletion later
	for id, entry := range s.entries {
		if entry.Name != name {
			continue
		}

		// this will dup(2) the file
		conn, err := asConnWithoutClose(entry.File)
		if err != nil {
			return nil, err
		}

		res = append(res, ConnEntry{
			shared: entry.shared,
			Conn:   conn,
		})
		ids = append(ids, id)
	}

	// only once we've collected all the entries with no
	// error do we remove them from the map and close the
	// original file
	for _, id := range ids {
		s.entries[id].File.Close()
		delete(s.entries, id)
	}

	return res, nil
}

// RemoveConn finds and removes the entries associated with the name given
// and tries to return them as net.Listener, entries are not removed or returned
// if an error occurs.
//
// Returns error matching ErrWrongType if an entry could not be converted
func (s *Store) RemoveListener(name string) (res []ListenerEntry, err error) {
	s.mu.Lock()
	defer s.mu.Unlock()

	defer func() {
		// if we error, we need to close all the listeners that we already made
		// before returning, or we would be leaking their fds
		if err == nil {
			return
		}

		for _, e := range res {
			e.Listener.Close()
		}

		res = nil
	}()

	var ids []uint64 // ids that match our name, for deletion later
	for id, entry := range s.entries {
		if entry.Name != name {
			continue
		}

		// this will dup(2) the file
		ln, err := asListenerWithoutClose(entry.File)
		if err != nil {
			return nil, err
		}

		res = append(res, ListenerEntry{
			shared:   entry.shared,
			Listener: ln,
		})
		ids = append(ids, id)
	}

	// only once we've collected all the entries with no
	// error do we remove them from the map and close the
	// original file
	for _, id := range ids {
		s.entries[id].File.Close()
		delete(s.entries, id)
	}

	return res, nil
}

// AddFile adds a file to the store with the name given and associated data.
// The data is stored in an memfd when passed through the socket and can be
// any kind of data.
func (s *Store) AddFile(fd *os.File, name string, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()

	entry := NewEntry(name, fd, data)
	s.entries[entry.ID] = entry
}

// AddFiler is like AddFile but takes any type with a File() (*os.File, error) method
//
// Returns error from File() if any.
func (s *Store) AddFiler(filer Filer, name string, data []byte) error {
	fd, err := filer.File()
	if err != nil {
		return err
	}
	s.AddFile(fd, name, data)
	return nil
}

// AddConn is like AddFile but takes a net.Conn, it is expected that the net.Conn
// given implements Filer. Types from the net pkg generally do.
//
// Returns error matching ErrNoFile if no File method was found or whatever the call
// to File() itself returns if it errors
func (s *Store) AddConn(conn net.Conn, name string, data []byte) error {
	if fder, ok := conn.(Filer); ok {
		return s.AddFiler(fder, name, data)
	}
	return fmt.Errorf("%w: found %T", ErrNoFile, conn)
}

// AddListener is like AddFile but takes a net.Listener, is is expected that the net.Listener
// given implements Filer. Types from the net pkg generally do.
//
// Returns error matching ErrNoFile if no File method was found or whatever the call
// to File() itself returns if it errors
func (s *Store) AddListener(ln net.Listener, name string, state []byte) error {
	if fder, ok := ln.(Filer); ok {
		return s.AddFiler(fder, name, state)
	}
	return fmt.Errorf("%w: found %T", ErrNoFile, ln)
}
