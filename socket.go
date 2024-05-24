package fdstore

import (
	"errors"
	"fmt"
	"net"
	"os"
	"syscall"

	"golang.org/x/sys/unix"
)

var (
	NOTIFY_SOCKET  = "NOTIFY_SOCKET"
	LISTEN_PID     = "LISTEN_PID"
	LISTEN_FDS     = "LISTEN_FDS"
	LISTEN_FDNAMES = "LISTEN_FDNAMES"
)

// NotifySocket returns a connected UnixConn to the path in env NOTIFY_SOCKET
func NotifySocket() (*net.UnixConn, error) {
	return NotifySocketNamed(os.Getenv(NOTIFY_SOCKET))
}

// NotifySocketNamed is like NotifySocket but lets you pick the name of the socket
func NotifySocketNamed(name string) (*net.UnixConn, error) {
	addr := &net.UnixAddr{
		Name: name,
		Net:  "unixgram",
	}

	if addr.Name == "" {
		return nil, ErrNoSocket
	}

	conn, err := net.DialUnix(addr.Net, nil, addr)
	if err != nil {
		return nil, err
	}

	return conn, nil
}

func sendMsg(conn *net.UnixConn, state []byte, file *os.File) error {
	var files []*os.File
	if file != nil {
		files = []*os.File{file}
	}
	return sendMsgF(conn, state, files)
}

func sendMsgF(conn *net.UnixConn, state []byte, files []*os.File) error {
	raw, err := conn.SyscallConn()
	if err != nil {
		return err
	}

	var ids []int
	for _, file := range files {
		ids = append(ids, int(file.Fd()))
	}

	var uerr error
	err = raw.Write(func(fd uintptr) bool {
		var oob []byte
		if len(ids) > 0 {
			oob = unix.UnixRights(ids...)
		}
		uerr = unix.Sendmsg(int(fd), state, oob, nil, 0)
		return !errors.Is(uerr, syscall.EAGAIN)
	})
	if err != nil { // control error
		return err
	}

	if uerr != nil { // Sendmsg error
		return uerr
	}
	return nil
}

const scmBufSize = 64 // needs to be big enough to receive one scm with an fd in it

func readMsg(conn *net.UnixConn, state []byte) (n int, fd *os.File, err error) {
	raw, err := conn.SyscallConn()
	if err != nil {
		return 0, nil, err
	}

	var oobn int
	var uerr error
	oob := make([]byte, scmBufSize)
	err = raw.Read(func(fd uintptr) bool {
		n, oobn, _, _, uerr = unix.Recvmsg(int(fd), state, oob, 0)
		return !errors.Is(uerr, syscall.EAGAIN)
	})
	if err != nil {
		return 0, nil, err
	}
	if uerr != nil {
		return 0, nil, uerr
	}

	oob = oob[:oobn]
	if len(oob) > 0 {
		scm, err := syscall.ParseSocketControlMessage(oob)
		if err != nil {
			return n, nil, fmt.Errorf("failed to parse socket control message: %w", err)
		}
		if len(scm) != 1 {
			return n, nil, fmt.Errorf("too many socket control messages: %d", len(scm))
		}
		fds, err := syscall.ParseUnixRights(&scm[0])
		if err != nil {
			return n, nil, fmt.Errorf("failed to parse unix rights: %w", err)
		}
		if len(fds) == 0 { // no files, just return state only
			return n, nil, nil
		}
		if len(fds) > 1 { // too many files, we only support 1 file per message right now
			for i := range fds {
				syscall.Close(fds[i])
			}
			return n, nil, fmt.Errorf("socket control message contained too many fds: %d", len(fds))
		}

		// try to set this fd as non-blocking
		_ = syscall.SetNonblock(fds[0], true)
		return n, os.NewFile(uintptr(fds[0]), "<socketconn>"), nil
	}
	return n, nil, nil
}
