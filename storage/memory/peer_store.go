package memory

import (
	"encoding/binary"
	"errors"
	"net"
	"runtime"
	"sync"
	"time"

	log "github.com/Sirupsen/logrus"

	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/storage"
)

// ErrInvalidGCInterval is returned for a GarbageCollectionInterval that is
// less than or equal to zero.
var ErrInvalidGCInterval = errors.New("invalid garbage collection interval")

// Config holds the configuration of a memory PeerStore.
type Config struct {
	GarbageCollectionInterval time.Duration `yaml:"gc_interval"`
	PeerLifetime              time.Duration `yaml:"peer_lifetime"`
	ShardCount                int           `yaml:"shard_count"`
	MaxNumWant                int           `yaml:"max_numwant"`
}

// New creates a new PeerStore backed by memory.
func New(cfg Config) (storage.PeerStore, error) {
	shardCount := 1
	if cfg.ShardCount > 0 {
		shardCount = cfg.ShardCount
	}

	if cfg.GarbageCollectionInterval <= 0 {
		return nil, ErrInvalidGCInterval
	}

	ps := &peerStore{
		shards:     make([]*peerShard, shardCount*2),
		closed:     make(chan struct{}),
		maxNumWant: cfg.MaxNumWant,
	}

	for i := 0; i < shardCount*2; i++ {
		ps.shards[i] = &peerShard{swarms: make(map[bittorrent.InfoHash]swarm)}
	}

	go func() {
		for {
			select {
			case <-ps.closed:
				return
			case <-time.After(cfg.GarbageCollectionInterval):
				before := time.Now().Add(-cfg.PeerLifetime)
				log.Debugln("memory: purging peers with no announces since", before)
				ps.collectGarbage(before)
			}
		}
	}()

	return ps, nil
}

type serializedPeer string

type peerShard struct {
	swarms map[bittorrent.InfoHash]swarm
	sync.RWMutex
}

type swarm struct {
	// map serialized peer to mtime
	seeders  map[serializedPeer]int64
	leechers map[serializedPeer]int64
}

type peerStore struct {
	shards     []*peerShard
	closed     chan struct{}
	maxNumWant int
}

var _ storage.PeerStore = &peerStore{}

func (s *peerStore) shardIndex(infoHash bittorrent.InfoHash, v6 bool) uint32 {
	// There are twice the amount of shards specified by the user, the first
	// half is dedicated to IPv4 swarms and the second half is dedicated to
	// IPv6 swarms.
	idx := binary.BigEndian.Uint32(infoHash[:4]) % (uint32(len(s.shards)) / 2)
	if v6 {
		idx += uint32(len(s.shards) / 2)
	}
	return idx
}

func newPeerKey(p bittorrent.Peer) serializedPeer {
	b := make([]byte, 20+2+len(p.IP))
	copy(b[:20], p.ID[:])
	binary.BigEndian.PutUint16(b[20:22], p.Port)
	copy(b[22:], p.IP)

	return serializedPeer(b)
}

func decodePeerKey(pk serializedPeer) bittorrent.Peer {
	return bittorrent.Peer{
		ID:   bittorrent.PeerIDFromString(string(pk[:20])),
		Port: binary.BigEndian.Uint16([]byte(pk[20:22])),
		IP:   net.IP(pk[22:]),
	}
}

func (s *peerStore) PutSeeder(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	log.Debugf("PutSeeder InfoHash:%s Peer:[ID: %s, IP: %s, Port %d]", ih.String(),
		p.ID.String(), p.IP.String(), p.Port)

	select {
	case <-s.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	pk := newPeerKey(p)

	shard := s.shards[s.shardIndex(ih, len(p.IP) == net.IPv6len)]
	shard.Lock()

	if _, ok := shard.swarms[ih]; !ok {
		shard.swarms[ih] = swarm{
			seeders:  make(map[serializedPeer]int64),
			leechers: make(map[serializedPeer]int64),
		}
	}

	shard.swarms[ih].seeders[pk] = time.Now().UnixNano()

	shard.Unlock()
	return nil
}

func (s *peerStore) DeleteSeeder(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	log.Debugf("DeleteSeeder InfoHash:%s Peer:[ID: %s, IP: %s, Port %d]", ih.String(),
		p.ID.String(), p.IP.String(), p.Port)
	select {
	case <-s.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	pk := newPeerKey(p)

	shard := s.shards[s.shardIndex(ih, len(p.IP) == net.IPv6len)]
	shard.Lock()

	if _, ok := shard.swarms[ih]; !ok {
		shard.Unlock()
		return storage.ErrResourceDoesNotExist
	}

	if _, ok := shard.swarms[ih].seeders[pk]; !ok {
		shard.Unlock()
		return storage.ErrResourceDoesNotExist
	}

	delete(shard.swarms[ih].seeders, pk)

	if len(shard.swarms[ih].seeders)|len(shard.swarms[ih].leechers) == 0 {
		delete(shard.swarms, ih)
	}

	shard.Unlock()
	return nil
}

func (s *peerStore) PutLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	log.Debugf("PutLeecher InfoHash:%s Peer:[ID: %s, IP: %s, Port %d]", ih.String(),
		p.ID.String(), p.IP.String(), p.Port)
	select {
	case <-s.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	pk := newPeerKey(p)

	shard := s.shards[s.shardIndex(ih, len(p.IP) == net.IPv6len)]
	shard.Lock()

	if _, ok := shard.swarms[ih]; !ok {
		shard.swarms[ih] = swarm{
			seeders:  make(map[serializedPeer]int64),
			leechers: make(map[serializedPeer]int64),
		}
	}

	shard.swarms[ih].leechers[pk] = time.Now().UnixNano()

	shard.Unlock()
	return nil
}

func (s *peerStore) DeleteLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	log.Debugf("DeleteLeecher InfoHash:%s Peer:[ID: %s, IP: %s, Port %d]", ih.String(),
		p.ID.String(), p.IP.String(), p.Port)
	select {
	case <-s.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	pk := newPeerKey(p)

	shard := s.shards[s.shardIndex(ih, len(p.IP) == net.IPv6len)]
	shard.Lock()

	if _, ok := shard.swarms[ih]; !ok {
		shard.Unlock()
		return storage.ErrResourceDoesNotExist
	}

	if _, ok := shard.swarms[ih].leechers[pk]; !ok {
		shard.Unlock()
		return storage.ErrResourceDoesNotExist
	}

	delete(shard.swarms[ih].leechers, pk)

	if len(shard.swarms[ih].seeders)|len(shard.swarms[ih].leechers) == 0 {
		delete(shard.swarms, ih)
	}

	shard.Unlock()
	return nil
}

func (s *peerStore) GraduateLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	log.Debugf("GraduateLeecher InfoHash:%s Peer:[ID: %s, IP: %s, Port %d]", ih.String(),
		p.ID.String(), p.IP.String(), p.Port)
	select {
	case <-s.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	pk := newPeerKey(p)

	shard := s.shards[s.shardIndex(ih, len(p.IP) == net.IPv6len)]
	shard.Lock()

	if _, ok := shard.swarms[ih]; !ok {
		shard.swarms[ih] = swarm{
			seeders:  make(map[serializedPeer]int64),
			leechers: make(map[serializedPeer]int64),
		}
	}

	delete(shard.swarms[ih].leechers, pk)

	shard.swarms[ih].seeders[pk] = time.Now().UnixNano()

	shard.Unlock()
	return nil
}

func (s *peerStore) AnnouncePeers(ih bittorrent.InfoHash, seeder bool, numWant int, announcer bittorrent.Peer) (peers []bittorrent.Peer, err error) {
	log.Debugf("AnnouncePeers InfoHash:%s, seeder: %v, numWant: %d, Peer:[ID: %s, IP: %s, Port %d]", ih.String(), seeder, numWant,
		announcer.ID.String(), announcer.IP.String(), announcer.Port)
	select {
	case <-s.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	if numWant > s.maxNumWant {
		numWant = s.maxNumWant
	}

	shard := s.shards[s.shardIndex(ih, len(announcer.IP) == net.IPv6len)]
	shard.RLock()

	if _, ok := shard.swarms[ih]; !ok {
		shard.RUnlock()
		return nil, storage.ErrResourceDoesNotExist
	}

	if seeder {
		// Append leechers as possible.
		leechers := shard.swarms[ih].leechers
		for p := range leechers {
			decodedPeer := decodePeerKey(p)
			if numWant == 0 {
				break
			}

			peers = append(peers, decodedPeer)
			numWant--
		}
	} else {
		// Append as many seeders as possible.
		seeders := shard.swarms[ih].seeders
		for p := range seeders {
			decodedPeer := decodePeerKey(p)
			if numWant == 0 {
				break
			}

			peers = append(peers, decodedPeer)
			numWant--
		}

		// Append leechers until we reach numWant.
		leechers := shard.swarms[ih].leechers
		if numWant > 0 {
			for p := range leechers {
				decodedPeer := decodePeerKey(p)
				if numWant == 0 {
					break
				}

				if decodedPeer.Equal(announcer) {
					continue
				}
				peers = append(peers, decodedPeer)
				numWant--
			}
		}
	}

	shard.RUnlock()
	return
}

func (s *peerStore) ScrapeSwarm(ih bittorrent.InfoHash, v6 bool) (resp bittorrent.Scrape) {
	select {
	case <-s.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	shard := s.shards[s.shardIndex(ih, v6)]
	shard.RLock()

	if _, ok := shard.swarms[ih]; !ok {
		shard.RUnlock()
		return
	}

	resp.Incomplete = uint32(len(shard.swarms[ih].leechers))
	resp.Complete = uint32(len(shard.swarms[ih].seeders))
	shard.RUnlock()

	return
}

// collectGarbage deletes all Peers from the PeerStore which are older than the
// cutoff time.
//
// This function must be able to execute while other methods on this interface
// are being executed in parallel.
func (s *peerStore) collectGarbage(cutoff time.Time) error {
	select {
	case <-s.closed:
		panic("attempted to interact with stopped memory store")
	default:
	}

	cutoffUnix := cutoff.UnixNano()
	for _, shard := range s.shards {
		shard.RLock()
		var infohashes []bittorrent.InfoHash
		for ih := range shard.swarms {
			infohashes = append(infohashes, ih)
		}
		shard.RUnlock()
		runtime.Gosched()

		for _, ih := range infohashes {
			shard.Lock()

			if _, stillExists := shard.swarms[ih]; !stillExists {
				shard.Unlock()
				runtime.Gosched()
				continue
			}

			for pk, mtime := range shard.swarms[ih].leechers {
				if mtime <= cutoffUnix {
					delete(shard.swarms[ih].leechers, pk)
					p := decodePeerKey(pk)
					log.Debugf("Deleting peer: [%s, %s, %d]", p.ID.String(), p.IP.String(), p.Port)
				}
			}

			for pk, mtime := range shard.swarms[ih].seeders {
				if mtime <= cutoffUnix {
					delete(shard.swarms[ih].seeders, pk)
					p := decodePeerKey(pk)
					log.Debugf("Deleting peer: [%s, %s, %d]", p.ID.String(), p.IP.String(), p.Port)
				}
			}

			if len(shard.swarms[ih].seeders)|len(shard.swarms[ih].leechers) == 0 {
				delete(shard.swarms, ih)
			}

			shard.Unlock()
			runtime.Gosched()
		}

		runtime.Gosched()
	}

	return nil
}

func (s *peerStore) Stop() <-chan error {
	toReturn := make(chan error)
	go func() {
		shards := make([]*peerShard, len(s.shards))
		for i := 0; i < len(s.shards); i++ {
			shards[i] = &peerShard{swarms: make(map[bittorrent.InfoHash]swarm)}
		}
		s.shards = shards
		close(s.closed)
		close(toReturn)
	}()
	return toReturn
}
