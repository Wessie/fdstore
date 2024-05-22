package fdstore

import (
	"strconv"
	"strings"
	"syscall"
)

type Variable = string

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

func FDName(name string) Variable {
	if len(name) > 255 {
		return ""
	}
	// TODO: add name checking
	return Variable("FDNAME=" + name)
}

func FDStoreRemove(name string) Variable {
	return combine("FDSTOREREMOVE=1", FDName(name))
}

func MainPID(pid uint) Variable {
	return Variable("MAINPID=" + strconv.FormatUint(uint64(pid), 10))
}

func Status(status string) Variable {
	return Variable("STATUS=" + status)
}

// MonotonicUsec is not implemented, since there is no clear way to get the value of CLOCK_MONOTONIC right now
func MonotonicUsec() Variable {
	panic("not implemented")
	return Variable("MONOTONIC_USEC=")
}

type NotifyAccessOption = Variable

const (
	NotifyAccessNone = "none"
	NotifyAccessAll  = "all"
	NotifyAccessMain = "main"
	NotifyAccessExec = "exec"
)

func NotifyAccess(na NotifyAccessOption) Variable {
	return Variable("NOTIFYACCESS=" + Variable(na))
}

func Errno(errno syscall.Errno) Variable {
	return Variable("ERRNO=" + strconv.FormatUint(uint64(errno), 10))
}

func combine(vv ...Variable) Variable {
	var b strings.Builder

	for _, v := range vv {
		b.WriteString(v)
		if !strings.HasSuffix(v, "\n") {
			b.WriteRune('\n')
		}
	}

	return b.String()
}
