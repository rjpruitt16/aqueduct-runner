package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const subprotocol = "aqueduct.v1"

type envelope struct {
	Type         string          `json:"type"`
	State        string          `json:"state,omitempty"`
	MessageID    string          `json:"message_id,omitempty"`
	CausedBy     string          `json:"caused_by,omitempty"`
	StreamID     string          `json:"stream_id,omitempty"`
	Payload      json.RawMessage `json:"payload,omitempty"`
	Reason       string          `json:"reason,omitempty"`
	Position     int             `json:"position,omitempty"`
	Generation   int64           `json:"generation,omitempty"`
	RetryAfterMS int64           `json:"retry_after_ms,omitempty"`
}

var backendConnections = struct {
	sync.Mutex
	counts map[string]int
}{counts: make(map[string]int)}

func main() {
	if len(os.Args) < 2 {
		log.Fatal("usage: websocket-fixture server|test")
	}
	switch os.Args[1] {
	case "server":
		serveBackend()
	case "test":
		if err := runContract(os.Args[2:]); err != nil {
			log.Fatal(err)
		}
	default:
		log.Fatalf("unknown mode %q", os.Args[1])
	}
}

func serveBackend() {
	upgrader := websocket.Upgrader{
		Subprotocols: []string{subprotocol},
		CheckOrigin:  func(*http.Request) bool { return true },
	}
	http.HandleFunc("/socket", func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") != "Bearer gateway-auth" {
			http.Error(w, "missing forwarded gateway identity", http.StatusUnauthorized)
			return
		}
		sessionID := r.Header.Get("X-Aqueduct-Session-ID")
		if sessionID == "" {
			http.Error(w, "missing session id", http.StatusBadRequest)
			return
		}
		mode := r.URL.Query().Get("mode")
		responseHeaders := http.Header{}
		maxConnections := "1"
		if mode == "slow-start" {
			maxConnections = "4"
		}
		responseHeaders.Set("X-Aqueduct-WS-Max-Connections", maxConnections)
		responseHeaders.Set("X-Aqueduct-WS-Connect-Rps", "50")
		conn, err := upgrader.Upgrade(w, r, responseHeaders)
		if err != nil {
			return
		}
		defer conn.Close()

		switch mode {
		case "basic":
			serveBasic(conn)
		case "reconnect":
			serveReconnect(conn, sessionID)
		case "hold", "slow-start":
			for {
				if _, _, err := conn.ReadMessage(); err != nil {
					return
				}
			}
		default:
			_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.ClosePolicyViolation, "unknown mode"), time.Now().Add(time.Second))
		}
	})
	log.Print("websocket fixture listening on :6060")
	log.Fatal(http.ListenAndServe(":6060", nil))
}

func serveBasic(conn *websocket.Conn) {
	for {
		var command envelope
		if err := conn.ReadJSON(&command); err != nil {
			return
		}
		if command.Type != "command" || command.MessageID == "" {
			return
		}
		messages := []envelope{
			{Type: "ack", MessageID: "ack-" + command.MessageID, CausedBy: command.MessageID},
			{Type: "event", MessageID: "event-1-" + command.MessageID, CausedBy: command.MessageID, Payload: json.RawMessage(`{"sequence":1}`)},
			{Type: "event", MessageID: "event-2-" + command.MessageID, CausedBy: command.MessageID, Payload: json.RawMessage(`{"sequence":2}`)},
		}
		for _, message := range messages {
			if err := conn.WriteJSON(message); err != nil {
				return
			}
		}
	}
}

func serveReconnect(conn *websocket.Conn, sessionID string) {
	backendConnections.Lock()
	backendConnections.counts[sessionID]++
	connection := backendConnections.counts[sessionID]
	backendConnections.Unlock()

	if connection == 1 {
		_ = conn.WriteJSON(envelope{Type: "event", MessageID: "before-reconnect", Payload: json.RawMessage(`{"phase":"before"}`)})
		time.Sleep(100 * time.Millisecond)
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseServiceRestart, "fixture restart"), time.Now().Add(time.Second))
		return
	}
	_ = conn.WriteJSON(envelope{Type: "event", MessageID: "after-reconnect", Payload: json.RawMessage(`{"phase":"after"}`)})
	for {
		if _, _, err := conn.ReadMessage(); err != nil {
			return
		}
	}
}

type contractClient struct {
	aquiferURL string
	backendURL string
	valkeyAddr string
	dialer     websocket.Dialer
}

func runContract(args []string) error {
	flags := flag.NewFlagSet("test", flag.ContinueOnError)
	aquiferURL := flags.String("aquifer", "ws://aquifer:8080/websocket", "Aquifer WebSocket endpoint")
	backendURL := flags.String("backend", "ws://backend:6060/socket", "fixture backend endpoint")
	valkeyAddr := flags.String("valkey", "valkey:6379", "Valkey address")
	if err := flags.Parse(args); err != nil {
		return err
	}
	c := &contractClient{
		aquiferURL: *aquiferURL,
		backendURL: *backendURL,
		valkeyAddr: *valkeyAddr,
		dialer: websocket.Dialer{
			HandshakeTimeout: 5 * time.Second,
			Subprotocols:     []string{subprotocol},
		},
	}
	if err := c.testSlowStart(); err != nil {
		return fmt.Errorf("slow start: %w", err)
	}
	if err := c.testDurabilityAndReplay(); err != nil {
		return fmt.Errorf("durability and replay: %w", err)
	}
	if err := c.testReconnect(); err != nil {
		return fmt.Errorf("upstream reconnect: %w", err)
	}
	if err := c.testPerNodeLimits(); err != nil {
		return fmt.Errorf("per-node limits: %w", err)
	}
	fmt.Println(`{"result":"PASS","checks":["slow_start","durable_ordering","cursor_replay","upstream_reconnect","live_queue_positions","per_node_limits","valkey_transcript","stream_ttl"]}`)
	return nil
}

func (c *contractClient) testSlowStart() error {
	first, err := c.dialReady("runner-slow-start-1", "0-0", "slow-start")
	if err != nil {
		return err
	}
	defer closeSocket(first)
	if err := waitForStatus(first, "connected", 5*time.Second); err != nil {
		return err
	}

	started := time.Now()
	second, err := c.dialReady("runner-slow-start-2", "0-0", "slow-start")
	if err != nil {
		return err
	}
	defer closeSocket(second)
	if err := waitForStatus(second, "connected", 5*time.Second); err != nil {
		return err
	}
	if elapsed := time.Since(started); elapsed < 700*time.Millisecond {
		return fmt.Errorf("second upstream opened after %v; expected initial 1 RPS slow-start pacing", elapsed)
	}
	return nil
}

func (c *contractClient) dial(sessionID, after, mode string) (*websocket.Conn, *http.Response, error) {
	endpoint, err := url.Parse(c.aquiferURL)
	if err != nil {
		return nil, nil, err
	}
	query := endpoint.Query()
	query.Set("session_id", sessionID)
	query.Set("after", after)
	endpoint.RawQuery = query.Encode()
	headers := http.Header{}
	headers.Set("Authorization", "Bearer gateway-auth")
	headers.Set("X-Aqueduct-Upstream-URL", c.backendURL+"?mode="+mode)
	return c.dialer.Dial(endpoint.String(), headers)
}

func (c *contractClient) dialReady(sessionID, after, mode string) (*websocket.Conn, error) {
	deadline := time.Now().Add(5 * time.Second)
	for {
		conn, response, err := c.dial(sessionID, after, mode)
		if err == nil {
			return conn, nil
		}
		status := 0
		if response != nil {
			status = response.StatusCode
			response.Body.Close()
		}
		if status != http.StatusTooManyRequests && status != http.StatusServiceUnavailable {
			return nil, fmt.Errorf("websocket handshake status=%d: %w", status, err)
		}
		if time.Now().After(deadline) {
			return nil, fmt.Errorf("websocket admission did not recover, last status=%d: %w", status, err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (c *contractClient) testDurabilityAndReplay() error {
	conn, err := c.dialReady("runner-basic", "0-0", "basic")
	if err != nil {
		return err
	}
	if err := waitForStatus(conn, "connected", 5*time.Second); err != nil {
		conn.Close()
		return err
	}
	command := envelope{Type: "command", MessageID: "command-1", Payload: json.RawMessage(`{"action":"expand"}`)}
	if err := conn.WriteJSON(command); err != nil {
		conn.Close()
		return err
	}

	seen := make(map[string]envelope)
	deadline := time.Now().Add(5 * time.Second)
	for len(seen) < 4 {
		message, err := readEnvelope(conn, time.Until(deadline))
		if err != nil {
			conn.Close()
			return fmt.Errorf("waiting for durable messages: %w", err)
		}
		switch message.Type {
		case "command_recorded":
			seen[message.Type] = message
		case "ack", "event":
			seen[message.MessageID] = message
		}
	}
	closeSocket(conn)

	recorded := seen["command_recorded"]
	ack := seen["ack-command-1"]
	first := seen["event-1-command-1"]
	second := seen["event-2-command-1"]
	if recorded.StreamID == "" || ack.StreamID == "" || first.StreamID == "" || second.StreamID == "" {
		return fmt.Errorf("missing Redis stream ids: %#v", seen)
	}
	if ack.CausedBy != command.MessageID || first.CausedBy != command.MessageID || second.CausedBy != command.MessageID {
		return fmt.Errorf("one-to-many caused_by correlation was not preserved: %#v", seen)
	}
	if !(streamIDLess(recorded.StreamID, ack.StreamID) && streamIDLess(ack.StreamID, first.StreamID) && streamIDLess(first.StreamID, second.StreamID)) {
		return fmt.Errorf("messages were not durably ordered: %s %s %s %s", recorded.StreamID, ack.StreamID, first.StreamID, second.StreamID)
	}

	replay, err := c.dialReady("runner-basic", first.StreamID, "basic")
	if err != nil {
		return fmt.Errorf("reconnect for replay: %w", err)
	}
	defer closeSocket(replay)
	var replayed []envelope
	deadline = time.Now().Add(5 * time.Second)
	for {
		message, err := readEnvelope(replay, time.Until(deadline))
		if err != nil {
			return fmt.Errorf("reading replay: %w", err)
		}
		if message.Type == "ack" || message.Type == "event" {
			replayed = append(replayed, message)
		}
		if message.Type == "status" && message.State == "replay_complete" {
			break
		}
	}
	if len(replayed) != 1 || replayed[0].MessageID != "event-2-command-1" || replayed[0].StreamID != second.StreamID {
		return fmt.Errorf("expected only the unseen second event, got %#v", replayed)
	}

	keyHash := sha256.Sum256([]byte("runner-basic"))
	key := "aqueduct:ws:" + hex.EncodeToString(keyHash[:])
	length, err := c.redisCommand("XLEN", key)
	if err != nil {
		return err
	}
	if length != int64(4) {
		return fmt.Errorf("expected four durable transcript entries, got %#v", length)
	}
	ttl, err := c.redisCommand("TTL", key)
	if err != nil {
		return err
	}
	ttlSeconds, ok := ttl.(int64)
	if !ok || ttlSeconds <= 0 || ttlSeconds > 3 {
		return fmt.Errorf("expected sliding stream TTL in (0,3], got %#v", ttl)
	}
	raw, err := c.redisCommand("XRANGE", key, "-", "+")
	if err != nil {
		return err
	}
	flat := strings.Join(flattenRESP(raw), " ")
	for _, expected := range []string{"direction client", "direction backend", "command-1", "ack-command-1", "event-1-command-1", "event-2-command-1"} {
		if !strings.Contains(flat, expected) {
			return fmt.Errorf("Valkey transcript missing %q: %s", expected, flat)
		}
	}
	expiresBy := time.Now().Add(5 * time.Second)
	for {
		exists, err := c.redisCommand("EXISTS", key)
		if err != nil {
			return err
		}
		if exists == int64(0) {
			break
		}
		if time.Now().After(expiresBy) {
			return fmt.Errorf("Valkey stream %s did not expire after its idle TTL", key)
		}
		time.Sleep(100 * time.Millisecond)
	}
	return nil
}

func (c *contractClient) testReconnect() error {
	conn, err := c.dialReady("runner-reconnect", "0-0", "reconnect")
	if err != nil {
		return err
	}
	defer closeSocket(conn)
	deadline := time.Now().Add(8 * time.Second)
	connected := make(map[int64]bool)
	reconnecting := false
	events := make(map[string]bool)
	for len(connected) < 2 || !reconnecting || !events["before-reconnect"] || !events["after-reconnect"] {
		message, err := readEnvelope(conn, time.Until(deadline))
		if err != nil {
			return err
		}
		if message.Type == "status" && message.State == "connected" {
			connected[message.Generation] = true
		}
		if message.Type == "status" && message.State == "reconnecting" {
			reconnecting = true
		}
		if message.Type == "event" {
			events[message.MessageID] = true
		}
	}
	return nil
}

func (c *contractClient) testPerNodeLimits() error {
	first, err := c.dialReady("runner-limit-1", "0-0", "hold")
	if err != nil {
		return err
	}
	if err := waitForStatus(first, "connected", 5*time.Second); err != nil {
		first.Close()
		return err
	}
	second, err := c.dialReady("runner-limit-2", "0-0", "hold")
	if err != nil {
		first.Close()
		return err
	}
	if err := waitForWaitingPosition(second, 1, 5*time.Second); err != nil {
		first.Close()
		second.Close()
		return err
	}
	third, err := c.dialReady("runner-limit-3", "0-0", "hold")
	if err != nil {
		first.Close()
		second.Close()
		return err
	}
	if err := waitForWaitingPosition(third, 2, 5*time.Second); err != nil {
		first.Close()
		second.Close()
		third.Close()
		return err
	}

	fourth, response, err := c.dial("runner-limit-4", "0-0", "hold")
	if fourth != nil {
		fourth.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusTooManyRequests {
		first.Close()
		second.Close()
		third.Close()
		return fmt.Errorf("fourth local client should be rejected with 429, response=%v err=%v", response, err)
	}
	if response.Header.Get("Retry-After") == "" {
		first.Close()
		second.Close()
		third.Close()
		return errors.New("429 response did not include Retry-After")
	}
	response.Body.Close()

	closeSocket(second)
	if err := waitForWaitingPosition(third, 1, 5*time.Second); err != nil {
		first.Close()
		third.Close()
		return fmt.Errorf("third client did not advance after second left the queue: %w", err)
	}

	closeSocket(first)
	if err := waitForStatus(third, "connected", 5*time.Second); err != nil {
		third.Close()
		return errors.New("third upstream did not receive the released local slot")
	}
	closeSocket(third)
	return nil
}

func waitForStatus(conn *websocket.Conn, state string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		message, err := readEnvelope(conn, time.Until(deadline))
		if err != nil {
			return err
		}
		if message.Type == "status" && message.State == state {
			return nil
		}
	}
}

func waitForWaitingPosition(conn *websocket.Conn, position int, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for {
		message, err := readEnvelope(conn, time.Until(deadline))
		if err != nil {
			return err
		}
		if message.Type == "status" && message.State == "waiting" && message.Position == position {
			return nil
		}
	}
}

func readEnvelope(conn *websocket.Conn, timeout time.Duration) (envelope, error) {
	if timeout <= 0 {
		return envelope{}, errors.New("timed out")
	}
	if err := conn.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		return envelope{}, err
	}
	var message envelope
	if err := conn.ReadJSON(&message); err != nil {
		return envelope{}, err
	}
	return message, nil
}

func closeSocket(conn *websocket.Conn) {
	if conn == nil {
		return
	}
	_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "done"), time.Now().Add(time.Second))
	_ = conn.Close()
}

func streamIDLess(left, right string) bool {
	parse := func(value string) (int64, int64, bool) {
		parts := strings.Split(value, "-")
		if len(parts) != 2 {
			return 0, 0, false
		}
		ms, err1 := strconv.ParseInt(parts[0], 10, 64)
		seq, err2 := strconv.ParseInt(parts[1], 10, 64)
		return ms, seq, err1 == nil && err2 == nil
	}
	lms, lseq, lok := parse(left)
	rms, rseq, rok := parse(right)
	return lok && rok && (lms < rms || lms == rms && lseq < rseq)
}

func (c *contractClient) redisCommand(args ...string) (any, error) {
	conn, err := net.DialTimeout("tcp", c.valkeyAddr, 3*time.Second)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(3 * time.Second))
	var request bytes.Buffer
	fmt.Fprintf(&request, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&request, "$%d\r\n%s\r\n", len(arg), arg)
	}
	if _, err := conn.Write(request.Bytes()); err != nil {
		return nil, err
	}
	return readRESP(bufio.NewReader(conn))
}

func readRESP(reader *bufio.Reader) (any, error) {
	prefix, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	line, err := reader.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(strings.TrimSuffix(line, "\n"), "\r")
	switch prefix {
	case '+':
		return line, nil
	case '-':
		return nil, errors.New(line)
	case ':':
		return strconv.ParseInt(line, 10, 64)
	case '$':
		size, err := strconv.Atoi(line)
		if err != nil || size < 0 {
			return nil, err
		}
		data := make([]byte, size+2)
		if _, err := io.ReadFull(reader, data); err != nil {
			return nil, err
		}
		return string(data[:size]), nil
	case '*':
		count, err := strconv.Atoi(line)
		if err != nil || count < 0 {
			return nil, err
		}
		values := make([]any, count)
		for i := range values {
			values[i], err = readRESP(reader)
			if err != nil {
				return nil, err
			}
		}
		return values, nil
	default:
		return nil, fmt.Errorf("unsupported RESP prefix %q", prefix)
	}
}

func flattenRESP(value any) []string {
	switch typed := value.(type) {
	case string:
		return []string{typed}
	case int64:
		return []string{strconv.FormatInt(typed, 10)}
	case []any:
		var flattened []string
		for _, child := range typed {
			flattened = append(flattened, flattenRESP(child)...)
		}
		return flattened
	default:
		return nil
	}
}
