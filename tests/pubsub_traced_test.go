package tests

// Pubsub construction for the replication tests.
//
// Kubo builds its internal gossipsub with the library's default queue sizes
// (validation queue 32, per-peer outbound queue 32) and exposes no way to
// change them or to attach a RawTracer. Those 32-slot outbound queues
// overflow during TestReplicationMultipeer's concurrent write burst (10 peers
// publishing 5 head announcements each, plus mesh forwarding): the publisher
// silently drops whole RPCs (DropRPC) and never retries them, and when every
// redundant path for an announcement (flood-publish, mesh forwarding, the
// ~3-heartbeat IHAVE/IWANT gossip window) is lost during the congestion, the
// head is never replicated. Tracing showed hundreds of DropRPC events per
// node at queue size 32, with failures in ~2/15 runs, and zero drops or
// failures at 256.
//
// So the test node generators build the same pubsub kubo would
// (flood-publish + BasicSeqnoValidator) directly on the node's host, with
// enlarged queues and a RawTracer that reports drop counters on stdout
// whenever real loss occurs. TEST_PUBSUB_QUEUE overrides the queue size for
// future investigation (e.g. 32 reproduces the kubo-default behavior).

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"testing"

	ipfsCore "github.com/ipfs/kubo/core"
	pubsub "github.com/libp2p/go-libp2p-pubsub"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	mocknet "github.com/libp2p/go-libp2p/p2p/net/mock"
	"github.com/stretchr/testify/require"
)

const defaultTestPubsubQueueSize = 256

func testPubsubQueueSize() int {
	if v := os.Getenv("TEST_PUBSUB_QUEUE"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			return n
		}
	}
	return defaultTestPubsubQueueSize
}

// dropTracer counts pubsub events that imply silent message loss.
type dropTracer struct {
	mu            sync.Mutex
	label         string
	dropRPC       int
	rejected      map[string]int // reason -> count
	undeliverable int
	throttled     int
	grafts        int
	delivered     int
	duplicates    int
}

func newDropTracer(label string) *dropTracer {
	return &dropTracer{label: label, rejected: make(map[string]int)}
}

func (d *dropTracer) report() string {
	d.mu.Lock()
	defer d.mu.Unlock()
	return fmt.Sprintf("[%s] delivered=%d dup=%d grafts=%d dropRPC=%d undeliverable=%d throttled=%d rejected=%v",
		d.label, d.delivered, d.duplicates, d.grafts, d.dropRPC, d.undeliverable, d.throttled, d.rejected)
}

// lossy reports whether any event implying actual message loss was traced.
// RejectValidationIgnored is excluded: it is the seqno validator ignoring an
// out-of-order older message whose newer sibling was already accepted.
func (d *dropTracer) lossy() bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.dropRPC > 0 || d.undeliverable > 0 || d.throttled > 0 {
		return true
	}
	for reason := range d.rejected {
		if reason != pubsub.RejectValidationIgnored {
			return true
		}
	}
	return false
}

func (d *dropTracer) AddPeer(peer.ID, protocol.ID) {}
func (d *dropTracer) RemovePeer(peer.ID)           {}
func (d *dropTracer) Join(string)                  {}
func (d *dropTracer) Leave(string)                 {}
func (d *dropTracer) Graft(peer.ID, string)        { d.mu.Lock(); d.grafts++; d.mu.Unlock() }
func (d *dropTracer) Prune(peer.ID, string)        {}
func (d *dropTracer) ValidateMessage(*pubsub.Message) {
}
func (d *dropTracer) DeliverMessage(*pubsub.Message) { d.mu.Lock(); d.delivered++; d.mu.Unlock() }
func (d *dropTracer) RejectMessage(_ *pubsub.Message, reason string) {
	d.mu.Lock()
	d.rejected[reason]++
	d.mu.Unlock()
}
func (d *dropTracer) DuplicateMessage(*pubsub.Message) { d.mu.Lock(); d.duplicates++; d.mu.Unlock() }
func (d *dropTracer) ThrottlePeer(peer.ID)             { d.mu.Lock(); d.throttled++; d.mu.Unlock() }
func (d *dropTracer) RecvRPC(*pubsub.RPC)              {}
func (d *dropTracer) SendRPC(*pubsub.RPC, peer.ID)     {}
func (d *dropTracer) DropRPC(_ *pubsub.RPC, p peer.ID) { d.mu.Lock(); d.dropRPC++; d.mu.Unlock() }
func (d *dropTracer) UndeliverableMessage(*pubsub.Message) {
	d.mu.Lock()
	d.undeliverable++
	d.mu.Unlock()
}

// memSeqnoStore mirrors kubo's seqnoStore (repo-datastore-backed) with a plain
// map; the test repos use a fresh in-memory datastore anyway.
type memSeqnoStore struct {
	mu sync.Mutex
	m  map[peer.ID][]byte
}

func (s *memSeqnoStore) Get(_ context.Context, p peer.ID) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.m[p], nil
}

func (s *memSeqnoStore) Put(_ context.Context, p peer.ID, val []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.m[p] = val
	return nil
}

// testingTracedPubSub replaces the kubo node's internal pubsub with one we
// construct ourselves: same options kubo uses (flood-publish +
// BasicSeqnoValidator), plus a RawTracer and configurable queue sizes.
// Must be called on a node built with pubsub disabled, before the core API
// is created.
func testingTracedPubSub(ctx context.Context, t *testing.T, node *ipfsCore.IpfsNode, label string, queueSize int) *dropTracer {
	t.Helper()

	tracer := newDropTracer(label)
	ps, err := pubsub.NewGossipSub(ctx, node.PeerHost,
		pubsub.WithFloodPublish(true),
		pubsub.WithDefaultValidator(pubsub.NewBasicSeqnoValidator(&memSeqnoStore{m: make(map[peer.ID][]byte)}, slog.New(slog.DiscardHandler))),
		pubsub.WithValidateQueueSize(queueSize),
		pubsub.WithPeerOutboundQueueSize(queueSize),
		pubsub.WithRawTracer(tracer),
	)
	require.NoError(t, err)

	node.PubSub = ps

	t.Cleanup(func() {
		if tracer.lossy() {
			fmt.Println("PUBSUB-TRACE", tracer.report())
		}
	})

	return tracer
}

func testingTracedIPFSNode(ctx context.Context, t *testing.T, mn mocknet.Mocknet, label string, queueSize int) (*ipfsCore.IpfsNode, func()) {
	t.Helper()

	node, clean := testingIPFSNodeWithoutPubsub(ctx, t, mn)
	_ = testingTracedPubSub(ctx, t, node, label, queueSize)
	return node, clean
}
