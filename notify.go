package fdstore

import (
	"errors"
	"io"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
)

type State string

const (
	Ready           = "READY=1"
	Reloading       = "RELOADING=1"
	Stopping        = "STOPPING=1"
	Watchdog        = "WATCHDOG=1"
	WatchdogTrigger = "WATCHDOG=trigger"
	FDStore         = "FDSTORE=1"
	FDPoll          = "FDPOLL=0"
	Barrier         = "BARRIER=1"
)

func IsValidFDName(name string) bool {
	if len(name) > 255 {
		return false
	}
	if strings.Contains(name, ":") {
		return false
	}
	return true
}

func FDName(name string) State {
	if len(name) > 255 {
		return ""
	}
	// remove illegal characters
	// TODO: add control codes
	name = strings.ReplaceAll(name, ":", "")
	return State("FDNAME=" + name)
}

func FDStoreRemove(name string) State {
	return Combine("FDSTOREREMOVE=1", FDName(name))
}

func MainPID(pid uint) State {
	return State("MAINPID=" + strconv.FormatUint(uint64(pid), 10))
}

func Status(status string) State {
	return State("STATUS=" + status)
}

// MonotonicUsec is not implemented, since there is no clear way to get the value of CLOCK_MONOTONIC right now
func MonotonicUsec() State {
	panic("not implemented")
	return State("MONOTONIC_USEC=")
}

type NotifyAccessOption string

const (
	NotifyAccessNone NotifyAccessOption = "none"
	NotifyAccessAll                     = "all"
	NotifyAccessMain                    = "main"
	NotifyAccessExec                    = "exec"
)

func NotifyAccess(na NotifyAccessOption) State {
	return State("NOTIFYACCESS=" + State(na))
}

func Errno(errno syscall.Errno) State {
	return State("ERRNO=" + strconv.FormatUint(uint64(errno), 10))
}

func Combine(state ...State) State {
	var b strings.Builder

	for _, v := range state {
		b.WriteString(string(v))
		if v[len(v)-1] != '\n' {
			b.WriteRune('\n')
		}
	}

	return State(b.String())
}

// Notify is like sd_notify (https://www.freedesktop.org/software/systemd/man/latest/sd_notify.html#Description)
func Notify(state ...State) error {
	conn, err := NotifySocket()
	if err != nil {
		return err
	}
	defer conn.Close()

	return NotifyConn(conn, state...)
}

// NotifyConn is like sd_notify (https://www.freedesktop.org/software/systemd/man/latest/sd_notify.html#Description),
// but lets you pass in your own connection to use to write to
func NotifyConn(conn *net.UnixConn, state ...State) error {
	return sendMsg(conn, []byte(Combine(state...)), nil)
}

// NotifyWithFDs is like sd_notify_with_fds
// We add FDSTORE=1 and FDNAME={name} for you with the name given, the rest of state
// is prepended to the other two
func NotifyWithFDs(name string, files []*os.File, state ...State) error {
	conn, err := NotifySocket()
	if err != nil {
		return err
	}
	defer conn.Close()

	return NotifyWithFDsConn(conn, name, files, state...)
}

// NotifyWithFDsConn is NotifyWithFDs but lets you pass your own connection
func NotifyWithFDsConn(conn *net.UnixConn, name string, files []*os.File, state ...State) error {
	state = append(state, FDStore, FDName(name))

	return sendMsgF(conn, []byte(Combine(state...)), files)
}

// WaitBarrier is like sd_notify_barrier
func WaitBarrier(timeout time.Duration) error {
	conn, err := NotifySocket()
	if err != nil {
		return err
	}
	defer conn.Close()

	return WaitBarrierConn(conn, timeout)
}

// WaitBarrierConn is WaitBarrier but lets you pass your own connection
func WaitBarrierConn(conn *net.UnixConn, timeout time.Duration) error {
	// Create a pipe for communicating with systemd daemon.
	pipeR, pipeW, err := os.Pipe()
	if err != nil {
		return err
	}
	defer pipeR.Close() // close the read side for if the other side is missing

	err = sendMsg(conn, []byte(Barrier), pipeW)
	if err != nil {
		return err
	}

	// Close our copy of pipeW.
	err = pipeW.Close()
	if err != nil {
		return err
	}

	// Expect the read end of the pipe to be closed after the timeout
	err = pipeR.SetReadDeadline(time.Now().Add(timeout))
	if err != nil {
		return nil
	}

	// Read a single byte expecting EOF.
	var buf [1]byte
	n, err := pipeR.Read(buf[:])
	if n != 0 || err == nil {
		return ErrUnexpectedRead
	} else if errors.Is(err, os.ErrDeadlineExceeded) || errors.Is(err, io.EOF) {
		return nil
	} else {
		return err
	}
}

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
		file := os.NewFile(uintptr(fd), name)
		if file == nil {
			continue
		}
		files[name] = append(files[name], file)
	}
	return files
}
