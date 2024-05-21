package fdstore

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/justincormack/go-memfd"
	"golang.org/x/sys/unix"
)

var (
	ErrShortWrite = errors.New("short write")
	ErrNoFile     = errors.New("File method missing")
	ErrNoSocket   = errors.New("NOTIFY_SOCKET is empty")
	ErrNotOurName = errors.New("FDNAME was not the correct format")
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
		ID:   incrementalID.Add(1),
		Name: name,
		File: file,
		Data: data,
	}
}

type Entry struct {
	ID   uint64
	Name string
	File *os.File
	Data []byte
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
		return parsedName{}, fmt.Errorf("%w: expected uint64 suffix: %s", ErrNotOurName, name)
	}
	pn.id = id
	// and cut off the id to get back our original name
	pn.name = name[:i]
	return pn, nil
}

type Store struct {
	entries map[uint64]Entry
}

func NewStoreFromListenFDS() (*Store, error) {
	var store Store

	// get our files from the systemd environment variables
	files := ListenFDs()
	if files == nil {
		return nil, ErrNoFile
	}

	for name, file := range files {
		p, err := parseName(name)
		if err != nil {
			return nil, err
		}

		entry := store.entries[p.id]
		entry.ID = p.id
		entry.Name = p.name

		if p.isData { // data fd
			// close the data file after we're done, so it can be cleaned up
			defer file.Close()
			// read the whole file into memory
			data, err := io.ReadAll(file)
			if err != nil {
				return nil, err
			}
			entry.Data = data
		}

		if p.isFile {
			entry.File = file
		}

		store.entries[p.id] = entry
	}

	return &store, nil
}

func AsConn(file *os.File) (net.Conn, error) {
	conn, err := net.FileConn(file)
	if err != nil {
		return nil, err
	}
	file.Close()
	return conn, nil
}

func AsListener(file *os.File) (net.Listener, error) {
	ln, err := net.FileListener(file)
	if err != nil {
		return nil, err
	}
	file.Close()
	return ln, nil
}

func SendStore(conn *net.UnixConn, s Store) error {
	addr := conn.RemoteAddr().(*net.UnixAddr)

	for _, e := range s.entries {
		// create our sd_notify state
		state := combine(FDStore, FDName(e.fileName()))

		err := sendMsg(conn, addr, []byte(state), e.File)
		if err != nil {
			return err
		}

		// then prep our data
		dataFd, err := e.dataToFd()
		if err != nil {
			return err
		}
		defer dataFd.Close()

		// create our sd_notify state
		state = combine(FDStore, FDName(e.dataName()))

		err = sendMsg(conn, addr, []byte(state), dataFd)
		if err != nil {
			return err
		}
	}

	return nil
}

func sendMsg(conn *net.UnixConn, addr *net.UnixAddr, state []byte, file *os.File) error {
	oob := unix.UnixRights(int(file.Fd()))
	n, oobn, err := conn.WriteMsgUnix(state, oob, addr)
	if err != nil {
		return err
	}
	if n < len(state) {
		return ErrShortWrite
	}
	if oobn < len(oob) {
		return ErrShortWrite
	}
	return nil
}

const scmBufSize = 64

func readMsg(conn *net.UnixConn, state []byte) (n int, fd *os.File, err error) {
	oob := make([]byte, scmBufSize)
	n, oobn, _, _, err := conn.ReadMsgUnix(state, oob)
	if err != nil {
		return n, nil, err
	}
	oob = oob[:oobn]
	if len(oob) > 0 {
		scm, err := syscall.ParseSocketControlMessage(oob)
		if err != nil {
			return n, nil, err
		}
		if len(scm) != 1 {
			return n, nil, fmt.Errorf("%d socket control messages", len(scm))
		}
		fds, err := syscall.ParseUnixRights(&scm[0])
		if err != nil {
			return n, nil, fmt.Errorf("parsing unix rights: %s", err)
		}
		if len(fds) == 1 {
			// try to set this fd as non-blocking
			syscall.SetNonblock(fds[0], true)
			return n, os.NewFile(uintptr(fds[0]), "<socketconn>"), nil
		}
		if len(fds) > 1 {
			for i := range fds {
				syscall.Close(fds[i])
			}
			return n, nil, fmt.Errorf("control message sent %d fds", len(fds))
		}
		// fallthrough; len(fds) == 0
	}
	return n, nil, nil
}

func (s *Store) AddFile(fd *os.File, name string, data []byte) {
	entry := NewEntry(name, fd, data)
	s.entries[entry.ID] = entry
}

func (s *Store) AddFiler(filer Filer, name string, data []byte) error {
	fd, err := filer.File()
	if err != nil {
		return err
	}
	s.AddFile(fd, name, data)
	return nil
}

func (s *Store) AddConn(conn net.Conn, name string, data []byte) error {
	if fder, ok := conn.(Filer); ok {
		return s.AddFiler(fder, name, data)
	}
	return ErrNoFile
}

func (s *Store) AddListener(ln net.Listener, name string, state []byte) error {
	if fder, ok := ln.(Filer); ok {
		return s.AddFiler(fder, name, state)
	}
	return ErrNoFile
}
