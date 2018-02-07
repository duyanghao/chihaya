package redis

import (
	"encoding/base64"
	"encoding/binary"
	"errors"
	"net"
	"sync"

	log "github.com/Sirupsen/logrus"

	"github.com/chihaya/chihaya/bittorrent"
	"github.com/chihaya/chihaya/storage"

	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/chihaya/chihaya/common"
	"github.com/garyburd/redigo/redis"
	"gopkg.in/redsync.v1"
)

/*Data Structure Definition Begin*/

// ErrInvalidGCInterval is returned for a GarbageCollectionInterval that is
// less than or equal to zero.
var ErrInvalidGCInterval = errors.New("invalid garbage collection interval")

// Config holds the configuration of a redis PeerStore.
type Config struct {
	GarbageCollectionInterval time.Duration `yaml:"gc_interval"`
	PeerLifetime              time.Duration `yaml:"peer_lifetime"`
	MaxNumWant                int           `yaml:"max_numwant"`
	RedisBroker               string        `yaml:"redis_broker"`
}

// RedisBackend represents a Memcache result backend
type RedisBackend struct {
	host     string
	password string
	db       int
	pool     *redis.Pool
	// If set, path to a socket file overrides hostname
	socketPath string
	redsync    *redsync.Redsync
	common.RedisConnector
}

type serializedPeer string

type peerStore struct {
	closed     chan struct{}
	maxNumWant int
	redisbk    *RedisBackend
	sync.RWMutex
}

var _ storage.PeerStore = &peerStore{}

/*Data Structure Definition End*/

// ParseRedisURL ...
func ParseRedisURL(url string) (host, password string, db int, err error) {
	// redis://pwd@host/db

	parts := strings.Split(url, "redis://")
	if parts[0] != "" {
		err = errors.New("No redis scheme found")
		return
	}
	if len(parts) != 2 {
		err = fmt.Errorf("Redis connection string should be in format redis://password@host:port/db, instead got %s", url)
		return
	}
	parts = strings.Split(parts[1], "@")
	var hostAndDB string
	if len(parts) == 2 {
		//[pwd, host/db]
		password = parts[0]
		hostAndDB = parts[1]
	} else {
		hostAndDB = parts[0]
	}
	parts = strings.Split(hostAndDB, "/")
	if len(parts) == 1 {
		//[host]
		host, db = parts[0], 0 //default redis db
	} else {
		//[host, db]
		host = parts[0]
		db, err = strconv.Atoi(parts[1])
		if err != nil {
			db, err = 0, nil //ignore err here
		}
	}
	return
}

// NewRedisBackend creates RedisBackend instance
func NewRedisBackend(host, password, socketPath string, db int) *RedisBackend {
	return &RedisBackend{
		host:       host,
		db:         db,
		password:   password,
		socketPath: socketPath,
	}
}

// open returns or creates instance of Redis connection
func (b *RedisBackend) open() redis.Conn {
	if b.pool == nil {
		b.pool = b.NewPool(b.socketPath, b.host, b.password, b.db)
	}
	if b.redsync == nil {
		var pools = []redsync.Pool{b.pool}
		b.redsync = redsync.New(pools)
	}
	return b.pool.Get()
}

// New creates a new PeerStore backed by redis.
func New(cfg Config) (storage.PeerStore, error) {
	if cfg.GarbageCollectionInterval <= 0 {
		return nil, ErrInvalidGCInterval
	}

	// creates RedisBackend instance
	h, p, db, err := ParseRedisURL(cfg.RedisBroker)
	if err != nil {
		return nil, err
	}

	ps := &peerStore{
		closed:     make(chan struct{}),
		maxNumWant: cfg.MaxNumWant,
		redisbk:    NewRedisBackend(h, p, "", db),
	}

	go func() {
		for {
			select {
			case <-ps.closed:
				return
			case <-time.After(cfg.GarbageCollectionInterval):
				before := time.Now().Add(-cfg.PeerLifetime)
				log.Debugln("redis: purging peers with no announces since", before)
				ps.collectGarbage(before)
			}
		}
	}()

	return ps, nil
}

// convert bittorrent.Peer to serializedPeer(string)
func newPeerKey(p bittorrent.Peer) serializedPeer {
	b := make([]byte, 20+2+len(p.IP))
	copy(b[:20], p.ID[:])
	binary.BigEndian.PutUint16(b[20:22], p.Port)
	copy(b[22:], p.IP)

	return serializedPeer(base64.StdEncoding.EncodeToString(b))
}

// convert serializedPeer(string) to bittorrent.Peer
func decodePeerKey(pk serializedPeer) bittorrent.Peer {
	decoded_pk, err := base64.StdEncoding.DecodeString(string(pk))
	if err != nil {
		log.Errorf("redis: base64.StdEncoding.DecodeString failure: %s", err)
	}
	pk = serializedPeer(decoded_pk)
	return bittorrent.Peer{
		ID:   bittorrent.PeerIDFromString(string(pk[:20])),
		Port: binary.BigEndian.Uint16([]byte(pk[20:22])),
		IP:   net.IP(pk[22:]),
	}
}

func (s *peerStore) PutSeeder(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	IPver := "IPv4"
	if len(p.IP) == net.IPv6len {
		IPver = "IPv6"
	}

	encoded_ih := IPver + "_S_" + base64.StdEncoding.EncodeToString([]byte(ih.String())) // key

	log.Debugf("PutSeeder InfoHash:%s Base64EncodedInfoHash:%s Peer:[ID: %s, IP: %s, Port %d]", ih.String(), encoded_ih, p.ID.String(), p.IP.String(), p.Port)

	select {
	case <-s.closed:
		panic("attempted to interact with stopped redis store")
	default:
	}

	pk := newPeerKey(p) // field

	conn := s.redisbk.open()
	defer conn.Close()

	Untime := strconv.FormatInt(time.Now().UnixNano(), 10)

	_, err := conn.Do("HSET", encoded_ih, pk, Untime)

	if err != nil {
		return err
	}

	_, err = conn.Do("HSET", IPver, encoded_ih, Untime)

	if err != nil {
		return err
	}

	return nil
}

func (s *peerStore) DeleteSeeder(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	IPver := "IPv4"
	if len(p.IP) == net.IPv6len {
		IPver = "IPv6"
	}

	encoded_ih := IPver + "_S_" + base64.StdEncoding.EncodeToString([]byte(ih.String())) // key

	log.Debugf("DeleteSeeder InfoHash:%s Base64EncodedInfoHash:%s Peer:[ID: %s, IP: %s, Port %d]", ih.String(), encoded_ih, p.ID.String(), p.IP.String(), p.Port)

	select {
	case <-s.closed:
		panic("attempted to interact with stopped redis store")
	default:
	}

	pk := newPeerKey(p)

	conn := s.redisbk.open()
	defer conn.Close()

	DelNum, err := conn.Do("HDEL", encoded_ih, pk)
	if err != nil {
		return err
	}
	if DelNum.(int64) == 0 {
		return storage.ErrResourceDoesNotExist
	}

	return nil
}

func (s *peerStore) PutLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	IPver := "IPv4"
	if len(p.IP) == net.IPv6len {
		IPver = "IPv6"
	}

	encoded_ih := IPver + "_L_" + base64.StdEncoding.EncodeToString([]byte(ih.String())) // key

	log.Debugf("PutLeecher InfoHash:%s Base64EncodedInfoHash:%s Peer:[ID: %s, IP: %s, Port %d]", ih.String(), encoded_ih, p.ID.String(), p.IP.String(), p.Port)
	select {
	case <-s.closed:
		panic("attempted to interact with stopped redis store")
	default:
	}

	pk := newPeerKey(p)

	conn := s.redisbk.open()
	defer conn.Close()

	Untime := strconv.FormatInt(time.Now().UnixNano(), 10)

	_, err := conn.Do("HSET", encoded_ih, pk, Untime)

	if err != nil {
		return err
	}

	_, err = conn.Do("HSET", IPver, encoded_ih, Untime)

	if err != nil {
		return err
	}

	return nil
}

func (s *peerStore) DeleteLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	IPver := "IPv4"
	if len(p.IP) == net.IPv6len {
		IPver = "IPv6"
	}

	encoded_ih := IPver + "_L_" + base64.StdEncoding.EncodeToString([]byte(ih.String())) // key

	log.Debugf("DeleteLeecher InfoHash:%s Base64EncodedInfoHash:%s Peer:[ID: %s, IP: %s, Port %d]", ih.String(), encoded_ih, p.ID.String(), p.IP.String(), p.Port)
	select {
	case <-s.closed:
		panic("attempted to interact with stopped redis store")
	default:
	}

	pk := newPeerKey(p)

	conn := s.redisbk.open()
	defer conn.Close()

	DelNum, err := conn.Do("HDEL", encoded_ih, pk)
	if err != nil {
		return err
	}
	if DelNum.(int64) == 0 {
		return storage.ErrResourceDoesNotExist
	}

	return nil
}

func (s *peerStore) GraduateLeecher(ih bittorrent.InfoHash, p bittorrent.Peer) error {
	IPver := "IPv4"
	if len(p.IP) == net.IPv6len {
		IPver = "IPv6"
	}

	encoded_ih := base64.StdEncoding.EncodeToString([]byte(ih.String()))
	encoded_ih_L := IPver + "_L_" + encoded_ih // key
	encoded_ih_S := IPver + "_S_" + encoded_ih // key

	log.Debugf("GraduateLeecher InfoHash:%s Base64EncodedInfoHash:%s Peer:[ID: %s, IP: %s, Port %d]", ih.String(), encoded_ih, p.ID.String(), p.IP.String(), p.Port)
	select {
	case <-s.closed:
		panic("attempted to interact with stopped redis store")
	default:
	}

	pk := newPeerKey(p)

	conn := s.redisbk.open()
	defer conn.Close()

	_, err := conn.Do("HDEL", encoded_ih_L, pk)
	if err != nil {
		return err
	}

	Untime := strconv.FormatInt(time.Now().UnixNano(), 10)

	_, err = conn.Do("HSET", encoded_ih_S, pk, Untime)

	if err != nil {
		return err
	}

	_, err = conn.Do("HSET", IPver, encoded_ih_S, Untime)

	if err != nil {
		return err
	}

	return nil
}

func (s *peerStore) AnnouncePeers(ih bittorrent.InfoHash, seeder bool, numWant int, announcer bittorrent.Peer) (peers []bittorrent.Peer, err error) {
	IPver := "IPv4"
	if len(announcer.IP) == net.IPv6len {
		IPver = "IPv6"
	}

	encoded_ih := base64.StdEncoding.EncodeToString([]byte(ih.String()))
	encoded_ih_L := IPver + "_L_" + encoded_ih // key
	encoded_ih_S := IPver + "_S_" + encoded_ih // key

	log.Debugf("AnnouncePeers InfoHash:%s, Base64EncodedInfoHash:%s, seeder: %v, numWant: %d, Peer:[ID: %s, IP: %s, Port %d]", ih.String(), encoded_ih, seeder, numWant, announcer.ID.String(), announcer.IP.String(), announcer.Port)
	select {
	case <-s.closed:
		panic("attempted to interact with stopped redis store")
	default:
	}

	if numWant > s.maxNumWant {
		numWant = s.maxNumWant
	}

	conn := s.redisbk.open()
	defer conn.Close()

	leechers, err := conn.Do("HKEYS", encoded_ih_L)
	if err != nil {
		return nil, err
	}
	con_leechers := leechers.([]interface{})

	seeders, err := conn.Do("HKEYS", encoded_ih_S)
	if err != nil {
		return nil, err
	}
	con_seeders := seeders.([]interface{})

	if len(con_leechers) == 0 && len(con_seeders) == 0 {
		return nil, storage.ErrResourceDoesNotExist
	}

	if seeder {
		for _, p := range con_leechers {
			decodedPeer := decodePeerKey(serializedPeer(p.([]byte)))
			if numWant == 0 {
				break
			}

			peers = append(peers, decodedPeer)
			numWant--
		}
	} else {
		// Append as many Seeders as possible.
		for _, p := range con_seeders {
			decodedPeer := decodePeerKey(serializedPeer(p.([]byte)))
			if numWant == 0 {
				break
			}

			peers = append(peers, decodedPeer)
			numWant--
		}

		// Append Leechers until we reach numWant.
		if numWant > 0 {
			for _, p := range con_leechers {
				decodedPeer := decodePeerKey(serializedPeer(p.([]byte)))
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

	APResult := ""
	for _, pr := range peers {
		APResult = fmt.Sprintf("%s Peer:[ID: %s, IP: %s, Port %d]", APResult, pr.ID.String(), pr.IP.String(), pr.Port)
	}
	log.Debugf("AnnouncePeers result:%s", APResult)
	return
}

func (s *peerStore) ScrapeSwarm(ih bittorrent.InfoHash, v6 bool) (resp bittorrent.Scrape) {
	IPver := "IPv4"
	if v6 {
		IPver = "IPv6"
	}

	encoded_ih := base64.StdEncoding.EncodeToString([]byte(ih.String()))
	encoded_ih_L := IPver + "_L_" + encoded_ih // key
	encoded_ih_S := IPver + "_S_" + encoded_ih // key

	select {
	case <-s.closed:
		panic("attempted to interact with stopped redis store")
	default:
	}

	conn := s.redisbk.open()
	defer conn.Close()

	leechers_len, err := conn.Do("HLEN", encoded_ih_L)
	if err != nil {
		log.Errorf("Redis HLEN %s failure: %s", encoded_ih_L, err)
		return
	}
	l_len := leechers_len.(int64)

	seeders_len, err := conn.Do("HLEN", encoded_ih_S)
	if err != nil {
		log.Errorf("Redis HLEN %s failure: %s", encoded_ih_S, err)
		return
	}
	s_len := seeders_len.(int64)

	if l_len == 0 && s_len == 0 {
		return
	}

	resp.Incomplete = uint32(l_len)
	resp.Complete = uint32(s_len)

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
		panic("attempted to interact with stopped redis store")
	default:
	}

	shards := [2]string{"IPv4", "IPv6"}

	conn := s.redisbk.open()
	defer conn.Close()

	cutoffUnix := cutoff.UnixNano()
	for _, shard := range shards {

		infohashes_list, err := conn.Do("HKEYS", shard) // key
		if err != nil {
			return err
		}
		infohashes := infohashes_list.([]interface{})

		for _, ih := range infohashes {

			ih_str := string(ih.([]byte))

			ih_list, err := conn.Do("HGETALL", ih_str) // field
			if err != nil {
				return err
			}
			con_ih := ih_list.([]interface{})

			if len(con_ih) == 0 {
				_, err := conn.Do("DEL", ih_str)
				if err != nil {
					return err
				}
				log.Debugf("Deleting Redis key: %s", ih_str)
				_, err = conn.Do("HDEL", shard, ih_str)
				if err != nil {
					return err
				}
				log.Debugf("Deleting Redis Hkey: %s and Hfield: %s", shard, ih_str)
				continue
			}

			var pk serializedPeer
			for index, ih_field := range con_ih {
				if index%2 != 0 { // value
					mtime, err := strconv.ParseInt(string(ih_field.([]byte)), 10, 64)
					if err != nil {
						return err
					}
					if mtime <= cutoffUnix {
						_, err := conn.Do("HDEL", ih_str, pk)
						if err != nil {
							return err
						}
						p := decodePeerKey(pk)
						log.Debugf("Deleting peer: [%s, %s, %d]", p.ID.String(), p.IP.String(), p.Port)
					}
				} else { // key
					pk = serializedPeer(ih_field.([]byte))
				}
			}

			ih_len, err := conn.Do("HLEN", ih_str)
			if err != nil {
				return err
			}
			if ih_len.(int64) == 0 {
				_, err := conn.Do("DEL", ih_str)
				if err != nil {
					return err
				}
				log.Debugf("Deleting Redis key: %s", ih_str)
				_, err = conn.Do("HDEL", shard, ih_str)
				if err != nil {
					return err
				}
				log.Debugf("Deleting Redis Hkey: %s and Hfield: %s", shard, ih_str)
			}
		}

	}

	return nil
}

func (s *peerStore) Stop() <-chan error {
	toReturn := make(chan error)
	go func() {
		close(s.closed)
		close(toReturn)
	}()
	return toReturn
}
