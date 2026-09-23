package main

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

type shutdownBackendState struct {
	mu                 sync.Mutex
	slowStarted        chan struct{}
	slowStartedOnce    sync.Once
	webhookJobIDs      map[string]bool
	drainEvents        []string
	registrationStates []string
}

func newShutdownBackendState() *shutdownBackendState {
	return &shutdownBackendState{
		slowStarted:   make(chan struct{}),
		webhookJobIDs: make(map[string]bool),
	}
}

func (s *shutdownBackendState) registerRoutes(mux *http.ServeMux) {
	mux.HandleFunc("/slow-job", func(w http.ResponseWriter, _ *http.Request) {
		s.slowStartedOnce.Do(func() { close(s.slowStarted) })
		time.Sleep(2500 * time.Millisecond)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"finished":true}`))
	})
	mux.HandleFunc("/webhook", func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		if jobID, ok := payload["job_id"].(string); ok {
			s.mu.Lock()
			s.webhookJobIDs[jobID] = true
			s.mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/drain-webhook", func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		if event, ok := payload["event"].(string); ok {
			s.mu.Lock()
			s.drainEvents = append(s.drainEvents, event)
			s.mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	})
	mux.HandleFunc("/register", func(w http.ResponseWriter, r *http.Request) {
		var payload map[string]any
		_ = json.NewDecoder(r.Body).Decode(&payload)
		if state, ok := payload["state"].(string); ok {
			s.mu.Lock()
			s.registrationStates = append(s.registrationStates, state)
			s.mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	})
}

func (s *shutdownBackendState) hasWebhook(jobID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.webhookJobIDs[jobID]
}

func (s *shutdownBackendState) hasDrainEvent(event string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, candidate := range s.drainEvents {
		if candidate == event {
			return true
		}
	}
	return false
}

func (s *shutdownBackendState) hasRegistrationSequence(expected ...string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	next := 0
	for _, state := range s.registrationStates {
		if next < len(expected) && state == expected[next] {
			next++
		}
	}
	return next == len(expected)
}

func runShutdownContract(args []string) error {
	flags := flag.NewFlagSet("shutdown-test", flag.ContinueOnError)
	aquiferProcess := flags.String("aquifer-process", "/aquifer", "Aquifer binary under test")
	valkeyAddr := flags.String("valkey", "valkey:6379", "Valkey address")
	if err := flags.Parse(args); err != nil {
		return err
	}

	state := newShutdownBackendState()
	mux := newBackendMux()
	state.registerRoutes(mux)
	listener, err := net.Listen("tcp", "127.0.0.1:6060")
	if err != nil {
		return fmt.Errorf("listen for shutdown backend: %w", err)
	}
	backend := &http.Server{Handler: mux}
	backendDone := make(chan error, 1)
	go func() { backendDone <- backend.Serve(listener) }()
	defer func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = backend.Shutdown(ctx)
		<-backendDone
	}()

	var processLogs bytes.Buffer
	command := exec.Command(*aquiferProcess)
	command.Dir = "/tmp"
	command.Env = append(os.Environ(),
		"AQUIFER_ADAPTER=http",
		"PORT=8080",
		"DB_PATH=/tmp/aquifer-shutdown.db",
		"CONFIG_PATH=",
		"L8_KEY_PATH=/tmp/aquifer-shutdown.l8-key",
		"AQUIFER_VALKEY_URL=redis://"+*valkeyAddr,
		"AQUIFER_ALLOWED_URL_DOMAINS=127.0.0.1",
		"AQUIFER_WS_MAX_CLIENT_CONNECTIONS=4",
		"AQUIFER_WS_MAX_UPSTREAM_CONNECTIONS=4",
		"AQUIFER_WS_MAX_WAITING_CONNECTIONS=4",
		"AQUIFER_WS_CONNECT_RPS=50",
		"AQUIFER_WS_SLOW_START_RPS=5",
		"AQUIFER_WS_STREAM_TTL_SECONDS=60",
		"AQUIFER_WS_READ_BLOCK_MS=100",
		"AQUIFER_WS_HANDSHAKE_TIMEOUT_SECONDS=3",
		"AQUIFER_SHUTDOWN_TIMEOUT_SECONDS=10",
		"AQUIFER_SHUTDOWN_QUIESCE_MS=1500",
		"AQUIFER_WS_DRAIN_GRACE_SECONDS=2",
		"AQUIFER_REGISTRY_URL=http://127.0.0.1:6060/register",
		"AQUIFER_REGISTRY_INTERVAL_SECONDS=3600",
		"AQUIFER_DRAIN_ENABLED=true",
		"AQUIFER_DRAIN_TIMER_SECONDS=60",
		"AQUIFER_IDLE_TIMEOUT_SECONDS=60",
		"AQUIFER_DRAIN_WEBHOOK_URL=http://127.0.0.1:6060/drain-webhook",
	)
	command.Stdout = io.MultiWriter(os.Stderr, &processLogs)
	command.Stderr = io.MultiWriter(os.Stderr, &processLogs)
	if err := command.Start(); err != nil {
		return fmt.Errorf("start Aquifer: %w", err)
	}
	processDone := make(chan error, 1)
	go func() { processDone <- command.Wait() }()
	processExited := false
	defer func() {
		if !processExited {
			_ = command.Process.Kill()
			<-processDone
		}
	}()

	httpClient := &http.Client{Timeout: 2 * time.Second}
	if err := waitUntil(5*time.Second, func() bool {
		response, requestErr := httpClient.Get("http://127.0.0.1:8080/ready")
		if requestErr != nil {
			return false
		}
		defer response.Body.Close()
		return response.StatusCode == http.StatusOK
	}); err != nil {
		return fmt.Errorf("Aquifer never became ready: %w\n%s", err, processLogs.String())
	}
	if err := waitUntil(3*time.Second, func() bool {
		return state.hasRegistrationSequence("active")
	}); err != nil {
		return fmt.Errorf("active registration did not arrive: %w", err)
	}

	client := &contractClient{
		aquiferURL: "ws://127.0.0.1:8080/websocket",
		backendURL: "ws://127.0.0.1:6060/socket",
		valkeyAddr: *valkeyAddr,
		dialer: websocket.Dialer{
			HandshakeTimeout: 5 * time.Second,
			Subprotocols:     []string{subprotocol},
		},
	}
	socket, err := client.dialReady("runner-shutdown", "0-0", "hold")
	if err != nil {
		return fmt.Errorf("open shutdown WebSocket: %w", err)
	}
	defer socket.Close()
	if err := waitForStatus(socket, "connected", 5*time.Second); err != nil {
		return fmt.Errorf("wait for shutdown WebSocket: %w", err)
	}
	if err := socket.WriteJSON(envelope{Type: "command", MessageID: "shutdown-command", Payload: json.RawMessage(`{"action":"hold"}`)}); err != nil {
		return fmt.Errorf("write durable shutdown command: %w", err)
	}
	if err := waitForEnvelope(socket, 3*time.Second, func(message envelope) bool {
		return message.Type == "command_recorded" && message.MessageID == "shutdown-command" && message.StreamID != ""
	}); err != nil {
		return fmt.Errorf("command was not persisted before shutdown: %w", err)
	}

	jobPayload := map[string]any{
		"user_id":        "shutdown-user",
		"idempotent_key": "accepted-before-sigterm",
		"url":            "http://127.0.0.1:6060/slow-job",
		"method":         http.MethodPost,
		"webhook_url":    "http://127.0.0.1:6060/webhook",
	}
	jobID, err := submitShutdownJob(httpClient, jobPayload, http.StatusCreated)
	if err != nil {
		return err
	}
	select {
	case <-state.slowStarted:
	case <-time.After(3 * time.Second):
		return errors.New("accepted job did not begin before SIGTERM")
	}

	shutdownStarted := time.Now()
	if err := command.Process.Signal(syscall.SIGTERM); err != nil {
		return fmt.Errorf("send SIGTERM: %w", err)
	}
	var drainingMessage envelope
	if err := waitForEnvelope(socket, 2*time.Second, func(message envelope) bool {
		if message.Type == "status" && message.State == "server_draining" {
			drainingMessage = message
			return true
		}
		return false
	}); err != nil {
		return fmt.Errorf("WebSocket did not receive server_draining: %w", err)
	}
	if drainingMessage.RetryAfterMS != 2000 {
		return fmt.Errorf("server_draining advertised %dms grace, expected 2000ms", drainingMessage.RetryAfterMS)
	}
	if err := assertDrainingWebSocket(client); err != nil {
		return err
	}
	if err := assertDrainingHTTP(httpClient, jobPayload); err != nil {
		return err
	}
	if err := waitForServiceRestart(socket, 4*time.Second); err != nil {
		return err
	}

	select {
	case err := <-processDone:
		processExited = true
		if err != nil {
			return fmt.Errorf("Aquifer exited unsuccessfully after SIGTERM: %w\n%s", err, processLogs.String())
		}
	case <-time.After(11 * time.Second):
		return fmt.Errorf("Aquifer exceeded its shutdown deadline\n%s", processLogs.String())
	}
	elapsed := time.Since(shutdownStarted)
	if elapsed < 2*time.Second || elapsed > 11*time.Second {
		return fmt.Errorf("unexpected shutdown duration %v", elapsed)
	}
	if !state.hasWebhook(jobID) {
		return fmt.Errorf("accepted job %s exited without delivering its completion webhook", jobID)
	}
	if !state.hasDrainEvent("instance_idle") {
		return errors.New("shutdown did not deliver the final acknowledged drain batch")
	}
	if !state.hasRegistrationSequence("active", "draining", "offline") {
		return errors.New("registration did not report active -> draining -> offline")
	}
	length, err := client.redisCommand("XLEN", webSocketStreamKey("runner-shutdown"))
	if err != nil {
		return fmt.Errorf("read shutdown transcript: %w", err)
	}
	if count, ok := length.(int64); !ok || count < 1 {
		return fmt.Errorf("durable WebSocket transcript was lost during shutdown: %#v", length)
	}
	if response, requestErr := httpClient.Get("http://127.0.0.1:8080/ready"); requestErr == nil {
		response.Body.Close()
		return errors.New("Aquifer listener remained reachable after process exit")
	}

	fmt.Printf(`{"result":"PASS","checks":["sigterm","readiness","admission_rejection","accepted_job_drain","completion_webhook","websocket_handoff","websocket_admission_rejection","close_1012","registration_lifecycle","final_ledger_flush","durable_transcript","bounded_exit"],"shutdown_ms":%d}`+"\n", elapsed.Milliseconds())
	return nil
}

func assertDrainingWebSocket(client *contractClient) error {
	connection, response, err := client.dial("runner-rejected-during-drain", "0-0", "hold")
	if connection != nil {
		connection.Close()
	}
	if response != nil {
		defer response.Body.Close()
	}
	if err == nil || response == nil || response.StatusCode != http.StatusServiceUnavailable {
		return fmt.Errorf("new WebSocket was not rejected while draining: response=%v err=%v", response, err)
	}
	if response.Header.Get("X-Aqueduct-Node-State") != "draining" || response.Header.Get("Retry-After") == "" {
		return fmt.Errorf("draining WebSocket rejection omitted lifecycle headers: %v", response.Header)
	}
	return nil
}

func submitShutdownJob(client *http.Client, payload map[string]any, expectedStatus int) (string, error) {
	body, _ := json.Marshal(payload)
	response, err := client.Post("http://127.0.0.1:8080/jobs", "application/json", bytes.NewReader(body))
	if err != nil {
		return "", fmt.Errorf("submit shutdown job: %w", err)
	}
	defer response.Body.Close()
	raw, _ := io.ReadAll(response.Body)
	if response.StatusCode != expectedStatus {
		return "", fmt.Errorf("submit shutdown job status=%d body=%s", response.StatusCode, raw)
	}
	var result map[string]any
	if err := json.Unmarshal(raw, &result); err != nil {
		return "", fmt.Errorf("decode shutdown job response: %w", err)
	}
	jobID, _ := result["job_id"].(string)
	if jobID == "" {
		return "", fmt.Errorf("shutdown job response has no job_id: %s", raw)
	}
	return jobID, nil
}

func assertDrainingHTTP(client *http.Client, jobPayload map[string]any) error {
	ready, err := client.Get("http://127.0.0.1:8080/ready")
	if err != nil {
		return fmt.Errorf("read draining readiness during quiesce: %w", err)
	}
	defer ready.Body.Close()
	if ready.StatusCode != http.StatusServiceUnavailable || ready.Header.Get("X-Aqueduct-Node-State") != "draining" || ready.Header.Get("Retry-After") == "" {
		return fmt.Errorf("unexpected draining readiness: status=%d headers=%v", ready.StatusCode, ready.Header)
	}

	health, err := client.Get("http://127.0.0.1:8080/health")
	if err != nil {
		return fmt.Errorf("read health during quiesce: %w", err)
	}
	var healthPayload map[string]any
	decodeErr := json.NewDecoder(health.Body).Decode(&healthPayload)
	health.Body.Close()
	if decodeErr != nil || health.StatusCode != http.StatusOK || healthPayload["status"] != "draining" {
		return fmt.Errorf("unexpected draining liveness: status=%d body=%v err=%v", health.StatusCode, healthPayload, decodeErr)
	}

	rejected := make(map[string]any, len(jobPayload))
	for key, value := range jobPayload {
		rejected[key] = value
	}
	rejected["idempotent_key"] = "rejected-after-sigterm"
	body, _ := json.Marshal(rejected)
	response, err := client.Post("http://127.0.0.1:8080/jobs", "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("submit job during quiesce: %w", err)
	}
	defer response.Body.Close()
	if response.StatusCode != http.StatusServiceUnavailable || response.Header.Get("X-Aqueduct-Node-State") != "draining" {
		raw, _ := io.ReadAll(response.Body)
		return fmt.Errorf("new job was not rejected while draining: status=%d headers=%v body=%s", response.StatusCode, response.Header, raw)
	}
	return nil
}

func waitForServiceRestart(conn *websocket.Conn, timeout time.Duration) error {
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	for {
		_, _, err := conn.ReadMessage()
		if err == nil {
			continue
		}
		var closeErr *websocket.CloseError
		if !errors.As(err, &closeErr) || closeErr.Code != websocket.CloseServiceRestart {
			return fmt.Errorf("expected WebSocket close code %d, got %v", websocket.CloseServiceRestart, err)
		}
		return nil
	}
}

func waitForEnvelope(conn *websocket.Conn, timeout time.Duration, match func(envelope) bool) error {
	deadline := time.Now().Add(timeout)
	for {
		message, err := readEnvelope(conn, time.Until(deadline))
		if err != nil {
			return err
		}
		if match(message) {
			return nil
		}
	}
}

func waitUntil(timeout time.Duration, condition func() bool) error {
	deadline := time.Now().Add(timeout)
	for {
		if condition() {
			return nil
		}
		if time.Now().After(deadline) {
			return errors.New("timed out")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func webSocketStreamKey(sessionID string) string {
	hash := sha256Sum(sessionID)
	return "aqueduct:ws:" + hash
}

func sha256Sum(value string) string {
	sum := sha256.Sum256([]byte(value))
	return fmt.Sprintf("%x", sum[:])
}
