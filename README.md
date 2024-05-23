# fdstore

[![Go Reference](https://pkg.go.dev/badge/github.com/Wessie/fdstore.svg)](https://pkg.go.dev/github.com/Wessie/fdstore)
![Test](https://github.com/Wessie/fdstore/actions/workflows/test.yml/badge.svg)

A helper package to communicate with systemd through the sd_notify api, implements an abstraction layer on top of
the [file descriptor store](https://systemd.io/FILE_DESCRIPTOR_STORE/) of systemd.