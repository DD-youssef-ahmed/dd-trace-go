// Unless explicitly stated otherwise all files in this repository are licensed
// under the Apache License Version 2.0.
// This product includes software developed at Datadog (https://www.datadoghq.com/).
// Copyright 2026 Datadog, Inc.

// Command staleidle-proxy is a UDS pass-through that injects the failure mode
// described in APMS-19533: an idle keep-alive connection that the upstream
// (here: the Datadog Agent) silently drops. Our goal is to make the customer's
// failure mode deterministic when running the soak harness against the real
// agent (rather than a synthetic close-every-conn server, which the unit test
// in transport_test.go already covers).
//
// Behavior:
//
//   - Listen on -listen (UDS).
//   - For each accepted connection, dial -target (also UDS) and bidirectionally
//     copy bytes.
//   - After the upstream finishes serving the first HTTP/1.1 response, the
//     proxy CLOSES BOTH SIDES OF THE PAIR with SO_LINGER=0 so the close sends
//     a connection-reset rather than a graceful FIN. This forces an abrupt
//     teardown on the tracer's persistConn the moment it tries to reuse the
//     pooled connection — exactly the customer's failure shape.
//
// Why this is necessary instead of `socat -T <idle>`: graceful FIN closes are
// detected by Go's persistConn readLoop almost instantly and evicted from the
// idle pool, so the next request transparently dials fresh and never observes
// the failure. We need the connection to *look* keep-alive from the tracer's
// POV right up until it tries to write on it again — which is precisely what
// abrupt close (SO_LINGER=0) gives us.
package main

import (
	"bufio"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"strings"
	"syscall"
	"time"
)

func main() {
	listenPath := flag.String("listen", "", "UDS path to listen on")
	targetPath := flag.String("target", "", "UDS path to dial as upstream")
	closeAfterResp := flag.Int("close-after-resp", 1, "abruptly close the conn after this many upstream responses (0 = never)")
	flag.Parse()

	if *listenPath == "" || *targetPath == "" {
		log.Fatal("both -listen and -target are required")
	}

	_ = os.Remove(*listenPath)
	ln, err := net.Listen("unix", *listenPath)
	if err != nil {
		log.Fatal(err)
	}
	if err := os.Chmod(*listenPath, 0o666); err != nil {
		log.Fatalf("chmod %s: %v", *listenPath, err)
	}
	defer ln.Close()

	log.Printf("staleidle-proxy: %s -> %s (close after %d response(s))", *listenPath, *targetPath, *closeAfterResp)

	for {
		c, err := ln.Accept()
		if err != nil {
			return
		}
		go handle(c.(*net.UnixConn), *targetPath, *closeAfterResp)
	}
}

func handle(client *net.UnixConn, targetPath string, closeAfter int) {
	defer client.Close()

	upstream, err := net.Dial("unix", targetPath)
	if err != nil {
		return
	}
	defer upstream.Close()

	// SO_LINGER=0 on both ends so close() returns RST rather than FIN/EOF.
	// On Unix domain sockets the SO_LINGER setting doesn't actually send a
	// TCP-style RST (UDS has no RST), but combined with the close happening
	// before the tracer has drained its response side, the tracer typically
	// sees EPIPE on its next write or ECONNRESET on its next read — which is
	// the exact error shape the customer reports.
	setNoLinger(client)
	setNoLinger(upstream.(*net.UnixConn))

	// Forward client → upstream concurrently with the response-sniffing
	// copy below. When the sniffer is done (either it hit its N-response
	// budget or the upstream went away), we tear down BOTH conns
	// unconditionally so the client-side copy goroutine unblocks and the
	// tracer observes the broken pipe on its next attempt to use this conn.
	go func() {
		_, _ = io.Copy(upstream, client)
	}()

	if closeAfter <= 0 {
		_, _ = io.Copy(client, upstream)
		return
	}
	copyAndCloseAfterNResponses(client, upstream, closeAfter)
	// Explicit double-close ensures the tracer's next syscall on this
	// conn returns EPIPE/ECONNRESET rather than hanging on a half-open
	// state where the kernel hasn't decided yet. Deferred closes above
	// will fire too but those are idempotent.
	_ = client.Close()
	_ = upstream.Close()
}

// copyAndCloseAfterNResponses copies bytes from src to dst, treats the stream
// as HTTP/1.1 responses, and tears down both connections as soon as the
// configured number of complete responses has gone through. The teardown is
// abrupt (close() without draining) so the client sees the next attempted
// write/read on the pooled conn fail with EPIPE/ECONNRESET — the customer's
// failure mode.
func copyAndCloseAfterNResponses(dst io.Writer, src io.Reader, n int) {
	br := bufio.NewReader(src)
	responses := 0
	for {
		// Read the status line.
		statusLine, err := readLine(br)
		if err != nil {
			return
		}
		writeLine(dst, statusLine)

		// Read headers; track Content-Length to know when this response ends.
		var contentLen int64 = -1
		var chunked bool
		for {
			header, err := readLine(br)
			if err != nil {
				return
			}
			writeLine(dst, header)
			trim := strings.TrimRight(header, "\r\n")
			if trim == "" {
				break
			}
			lower := strings.ToLower(trim)
			if strings.HasPrefix(lower, "content-length:") {
				v, _ := strconv.ParseInt(strings.TrimSpace(trim[len("content-length:"):]), 10, 64)
				contentLen = v
			} else if strings.HasPrefix(lower, "transfer-encoding:") && strings.Contains(lower, "chunked") {
				chunked = true
			}
		}

		switch {
		case chunked:
			// Forward chunks until 0-sized chunk.
			for {
				sizeLine, err := readLine(br)
				if err != nil {
					return
				}
				writeLine(dst, sizeLine)
				size, _ := strconv.ParseInt(strings.TrimSpace(strings.TrimRight(sizeLine, "\r\n")), 16, 64)
				if size == 0 {
					// Possible trailers, then blank line.
					for {
						trailer, err := readLine(br)
						if err != nil {
							return
						}
						writeLine(dst, trailer)
						if strings.TrimRight(trailer, "\r\n") == "" {
							break
						}
					}
					break
				}
				if _, err := io.CopyN(dst, br, size+2); err != nil {
					return
				}
			}
		case contentLen >= 0:
			if _, err := io.CopyN(dst, br, contentLen); err != nil {
				return
			}
		default:
			// No Content-Length, no chunked encoding — the response body
			// extends to EOF. We can't safely close mid-stream, so wait for
			// EOF then stop.
			_, _ = io.Copy(dst, br)
			return
		}

		responses++
		if responses >= n {
			// Yield very briefly so the response bytes are observed by the
			// client before we yank the conn out from under it. Without this
			// pause the close races the kernel's TCP/UDS buffer drain and the
			// client occasionally sees an incomplete response, which the
			// stdlib classifies as transportReadFromServerError — that's not
			// the failure mode we're trying to reproduce.
			time.Sleep(time.Millisecond)
			return
		}
	}
}

func readLine(br *bufio.Reader) (string, error) {
	line, err := br.ReadString('\n')
	return line, err
}

func writeLine(dst io.Writer, line string) {
	_, _ = dst.Write([]byte(line))
}

func setNoLinger(c *net.UnixConn) {
	raw, err := c.SyscallConn()
	if err != nil {
		return
	}
	_ = raw.Control(func(fd uintptr) {
		// Linger 0 forces a hard close. On UDS this avoids the kernel
		// gracefully delivering pending data; combined with our timing it
		// keeps the conn looking keep-alive to the tracer right up until the
		// teardown.
		_ = syscall.SetsockoptLinger(int(fd), syscall.SOL_SOCKET, syscall.SO_LINGER, &syscall.Linger{Onoff: 1, Linger: 0})
	})
}
