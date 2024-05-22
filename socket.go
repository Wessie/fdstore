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

func ListenFDs() map[string]*os.File {
	pid, err := strconv.Atoi(os.Getenv("LISTEN_PID"))
	if err != nil || pid != os.Getpid() {
		return nil
	}

	numFds, err := strconv.Atoi(os.Getenv("LISTEN_FDS"))
	if err != nil || numFds < 1 {
		return nil
	}

	names := strings.Split(os.Getenv("LISTEN_FDNAMES"), ":")

	files := make(map[string]*os.File, numFds)
	for i := 0; i < numFds; i++ {
		// 0, 1, 2 are reserved for stdin, stdout, stderr; start from 3
		fd := i + 3
		// systemd should be setting this already
		unix.CloseOnExec(fd)

		name := "unknown"
		if i < len(names) {
			name = names[i]
		}
		files[name] = os.NewFile(uintptr(fd), name)
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
