package fdstore

import (
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"

	"golang.org/x/sys/unix"
)

// ListenFDs is like sd_listen_fds_with_names (https://www.freedesktop.org/software/systemd/man/latest/sd_listen_fds_with_names.html)
func ListenFDs() map[string][]*os.File {
	pid, err := strconv.Atoi(os.Getenv("LISTEN_PID"))
	if err != nil || pid != os.Getpid() {
		return nil
	}

	numFds, err := strconv.Atoi(os.Getenv("LISTEN_FDS"))
	if err != nil || numFds < 1 {
		return nil
	}

	names := strings.Split(os.Getenv("LISTEN_FDNAMES"), ":")

	files := make(map[string][]*os.File, numFds)
	for i := 0; i < numFds; i++ {
		// 0, 1, 2 are reserved for stdin, stdout, stderr; start from 3
		fd := i + 3
		// systemd should be setting this already
		unix.CloseOnExec(fd)

		name := "unknown"
		if i < len(names) {
			name = names[i]
		}
		files[name] = append(files[name], os.NewFile(uintptr(fd), name))
	}
	return files
}

func NotifySocket() (*net.UnixConn, error) {
	addr := &net.UnixAddr{
		Name: os.Getenv("NOTIFY_SOCKET"),
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

// Send is like sd_notify (https://www.freedesktop.org/software/systemd/man/latest/sd_notify.html#Description),
// as a special case, if conn is nil Send will try to use the return value of NotifySocket
func Send(conn *net.UnixConn, vars ...Variable) error {
	// grab the notify socket if we got passed nil
	if conn == nil {
		ns, err := NotifySocket()
		if err != nil {
			return err
		}
		defer conn.Close()
		conn = ns
	}

	// construct our state line, these should be new-line separated
	state := combine(vars...)

	_, err := conn.Write([]byte(state))
	return err
}

// SocketPair returns a pair of connected unix sockets.
func SocketPair() (*net.UnixConn, *net.UnixConn, error) {
	fds, err := syscall.Socketpair(syscall.AF_UNIX, syscall.SOCK_DGRAM|syscall.SOCK_NONBLOCK, 0)
	if err != nil {
		return nil, nil, err
	}
	left, err := FD2Unix(fds[0])
	if err != nil {
		syscall.Close(fds[0])
		syscall.Close(fds[1])
		return nil, nil, err
	}
	right, err := FD2Unix(fds[1])
	if err != nil {
		left.Close()
		syscall.Close(fds[1])
		return nil, nil, err
	}
	return left, right, nil
}

// FD2Unix returns the fd given as a unix conn, errors if the fd
// is not a unix conn
func FD2Unix(fd int) (*net.UnixConn, error) {
	osf := os.NewFile(uintptr(fd), "")
	if osf == nil {
		return nil, fmt.Errorf("bad file descriptor %d", fd)
	}
	defer osf.Close() // net.FileConn will dup(2) the fd
	fc, err := net.FileConn(osf)
	if err != nil {
		return nil, err
	}
	uc, ok := fc.(*net.UnixConn)
	if !ok {
		fc.Close()
		return nil, fmt.Errorf("couldn't convert %T to net.UnixConn", fc)
	}
	return uc, nil
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

const scmBufSize = 64 // needs to be big enough to receive one scm with an fd in it

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
