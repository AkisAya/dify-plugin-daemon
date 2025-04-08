package cache

import (
	"context"
	"crypto/tls"
	"time"

	"github.com/langgenius/dify-plugin-daemon/internal/utils/log"
	"github.com/langgenius/dify-plugin-daemon/internal/utils/parser"
	"github.com/redis/go-redis/v9"
)

// RedisCache implements the Cache interface using Redis
type RedisCache struct {
	client *redis.Client
	ctx    context.Context
}

// RedisTxPipe is an adapter that implements the TxPipe interface with redis.Pipeliner
type RedisTxPipe struct {
	pipeliner redis.Pipeliner
	ctx       context.Context
}

// Store implements TxPipe.Store for redis.Pipeliner
func (rtp *RedisTxPipe) Store(key string, value any, expire time.Duration) error {
	if _, ok := value.(string); !ok {
		var err error
		value, err = parser.MarshalCBOR(value)
		if err != nil {
			return err
		}
	}

	return rtp.pipeliner.Set(rtp.ctx, serialKey(key), value, expire).Err()
}

// SetNX implements TxPipe.SetNX for redis.Pipeliner
func (rtp *RedisTxPipe) SetNX(key string, value any, expire time.Duration) (bool, error) {
	if _, ok := value.(string); !ok {
		var err error
		value, err = parser.MarshalCBOR(value)
		if err != nil {
			return false, err
		}
	}

	cmd := rtp.pipeliner.SetNX(rtp.ctx, serialKey(key), value, expire)
	return cmd.Val(), cmd.Err()
}

var _ Cache = (*RedisCache)(nil)   // Ensure RedisCache implements Cache interface
var _ TxPipe = (*RedisTxPipe)(nil) // Ensure RedisTxPipe implements TxPipe interface

func getRedisOptions(addr, password string, useSsl bool, db int) *redis.Options {
	opts := &redis.Options{
		Addr:     addr,
		Password: password,
		DB:       db,
	}
	if useSsl {
		opts.TLSConfig = &tls.Config{}
	}
	return opts
}

func InitRedisCache(addr, password string, useSsl bool, db int) (Cache, error) {
	opts := getRedisOptions(addr, password, useSsl, db)
	redisClient := redis.NewClient(opts)

	ctx := context.Background()
	if _, err := redisClient.Ping(ctx).Result(); err != nil {
		return nil, err
	}

	c := &RedisCache{
		client: redisClient,
		ctx:    ctx,
	}

	return c, nil
}

func (rc *RedisCache) Close() error {
	if rc.client == nil {
		return ErrDBNotInit
	}

	return rc.client.Close()
}

func (rc *RedisCache) Store(key string, value any, expire time.Duration) error {
	return rc.store(serialKey(key), value, expire)
}

func (rc *RedisCache) store(key string, value any, expire time.Duration) error {
	if rc.client == nil {
		return ErrDBNotInit
	}

	if _, ok := value.(string); !ok {
		var err error
		value, err = parser.MarshalCBOR(value)
		if err != nil {
			return err
		}
	}

	return rc.client.Set(rc.ctx, key, value, expire).Err()
}

func (rc *RedisCache) Get(key string) ([]byte, error) {
	return rc.get(serialKey(key))
}

func (rc *RedisCache) get(key string) ([]byte, error) {
	if rc.client == nil {
		return nil, ErrDBNotInit
	}

	val, err := rc.client.Get(rc.ctx, key).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrNotFound
		}
		return nil, err
	}

	if len(val) == 0 {
		return nil, ErrNotFound
	}

	// Return the raw bytes, let the caller decide how to unmarshal
	return val, nil
}

func (rc *RedisCache) GetString(key string) (string, error) {
	if rc.client == nil {
		return "", ErrDBNotInit
	}

	v, err := rc.client.Get(rc.ctx, serialKey(key)).Result()
	if err != nil {
		if err == redis.Nil {
			return "", ErrNotFound
		}
		return "", err
	}

	return v, err
}

func (rc *RedisCache) Del(key string) error {
	return rc.del(serialKey(key))
}

func (rc *RedisCache) del(key string) error {
	if rc.client == nil {
		return ErrDBNotInit
	}

	_, err := rc.client.Del(rc.ctx, key).Result()
	if err != nil {
		if err == redis.Nil {
			return ErrNotFound
		}
		return err
	}

	return nil
}

func (rc *RedisCache) Exist(key string) (int64, error) {
	if rc.client == nil {
		return 0, ErrDBNotInit
	}

	return rc.client.Exists(rc.ctx, serialKey(key)).Result()
}

func (rc *RedisCache) Increase(key string) (int64, error) {
	if rc.client == nil {
		return 0, ErrDBNotInit
	}

	num, err := rc.client.Incr(rc.ctx, serialKey(key)).Result()
	if err != nil {
		if err == redis.Nil {
			return 0, ErrNotFound
		}
		return 0, err
	}

	return num, nil
}

func (rc *RedisCache) Decrease(key string) (int64, error) {
	if rc.client == nil {
		return 0, ErrDBNotInit
	}

	return rc.client.Decr(rc.ctx, serialKey(key)).Result()
}

func (rc *RedisCache) SetExpire(key string, expire time.Duration) error {
	if rc.client == nil {
		return ErrDBNotInit
	}

	return rc.client.Expire(rc.ctx, serialKey(key), expire).Err()
}

func (rc *RedisCache) SetMapField(key string, v map[string]any) error {
	if rc.client == nil {
		return ErrDBNotInit
	}

	return rc.client.HMSet(rc.ctx, serialKey(key), v).Err()
}

func (rc *RedisCache) SetMapOneField(key string, field string, value any) error {
	if rc.client == nil {
		return ErrDBNotInit
	}

	if _, ok := value.(string); !ok {
		value = parser.MarshalJson(value)
	}

	return rc.client.HSet(rc.ctx, serialKey(key), field, value).Err()
}

func (rc *RedisCache) GetMapField(key string, field string) ([]byte, error) {
	if rc.client == nil {
		return nil, ErrDBNotInit
	}

	val, err := rc.client.HGet(rc.ctx, serialKey(key), field).Bytes()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return val, nil
}

func (rc *RedisCache) GetMapFieldString(key string, field string) (string, error) {
	if rc.client == nil {
		return "", ErrDBNotInit
	}

	val, err := rc.client.HGet(rc.ctx, serialKey(key), field).Result()
	if err != nil {
		if err == redis.Nil {
			return "", ErrNotFound
		}
		return "", err
	}

	return val, nil
}

func (rc *RedisCache) DelMapField(key string, field string) error {
	if rc.client == nil {
		return ErrDBNotInit
	}

	return rc.client.HDel(rc.ctx, serialKey(key), field).Err()
}

func (rc *RedisCache) GetMap(key string) (map[string]string, error) {
	if rc.client == nil {
		return nil, ErrDBNotInit
	}

	val, err := rc.client.HGetAll(rc.ctx, serialKey(key)).Result()
	if err != nil {
		if err == redis.Nil {
			return nil, ErrNotFound
		}
		return nil, err
	}

	return val, nil
}

func (rc *RedisCache) ScanKeys(match string) ([]string, error) {
	if rc.client == nil {
		return nil, ErrDBNotInit
	}

	result := make([]string, 0)

	if err := rc.ScanKeysAsync(match, func(keys []string) error {
		result = append(result, keys...)
		return nil
	}); err != nil {
		return nil, err
	}

	return result, nil
}

func (rc *RedisCache) ScanKeysAsync(match string, fn func([]string) error) error {
	if rc.client == nil {
		return ErrDBNotInit
	}

	cursor := uint64(0)

	for {
		keys, newCursor, err := rc.client.Scan(rc.ctx, cursor, match, 32).Result()
		if err != nil {
			return err
		}

		if err := fn(keys); err != nil {
			return err
		}

		if newCursor == 0 {
			break
		}

		cursor = newCursor
	}

	return nil
}

func (rc *RedisCache) ScanMap(key string, match string) (map[string]string, error) {
	if rc.client == nil {
		return nil, ErrDBNotInit
	}

	result := make(map[string]string)

	if err := rc.ScanMapAsync(key, match, func(m map[string]string) error {
		for k, v := range m {
			result[k] = v
		}
		return nil
	}); err != nil {
		return nil, err
	}

	return result, nil
}

func (rc *RedisCache) ScanMapAsync(key string, match string, fn func(map[string]string) error) error {
	if rc.client == nil {
		return ErrDBNotInit
	}

	cursor := uint64(0)

	for {
		kvs, newCursor, err := rc.client.
			HScan(rc.ctx, serialKey(key), cursor, match, 32).
			Result()

		if err != nil {
			return err
		}

		result := make(map[string]string)
		for i := 0; i < len(kvs); i += 2 {
			result[kvs[i]] = kvs[i+1]
		}

		if err := fn(result); err != nil {
			return err
		}

		if newCursor == 0 {
			break
		}

		cursor = newCursor
	}

	return nil
}

func (rc *RedisCache) SetNX(key string, value any, expire time.Duration) (bool, error) {
	if rc.client == nil {
		return false, ErrDBNotInit
	}

	// marshal the value if not a string
	if _, ok := value.(string); !ok {
		var err error
		value, err = parser.MarshalCBOR(value)
		if err != nil {
			return false, err
		}
	}

	return rc.client.SetNX(rc.ctx, serialKey(key), value, expire).Result()
}

// Lock key, expire time takes responsibility for expiration time
// try_lock_timeout takes responsibility for the timeout of trying to lock
func (rc *RedisCache) Lock(key string, expire time.Duration, tryLockTimeout time.Duration) error {
	if rc.client == nil {
		return ErrDBNotInit
	}

	const LOCK_DURATION = 20 * time.Millisecond

	ticker := time.NewTicker(LOCK_DURATION)
	defer ticker.Stop()

	for range ticker.C {
		if _, err := rc.client.SetNX(rc.ctx, serialKey(key), "1", expire).Result(); err == nil {
			return nil
		}

		tryLockTimeout -= LOCK_DURATION
		if tryLockTimeout <= 0 {
			return ErrLockTimeout
		}
	}

	return nil
}

func (rc *RedisCache) Unlock(key string) error {
	if rc.client == nil {
		return ErrDBNotInit
	}

	return rc.client.Del(rc.ctx, serialKey(key)).Err()
}

func (rc *RedisCache) Expire(key string, expire time.Duration) (bool, error) {
	if rc.client == nil {
		return false, ErrDBNotInit
	}

	return rc.client.Expire(rc.ctx, serialKey(key), expire).Result()
}

func (rc *RedisCache) Transaction(fn func(txp TxPipe) error) error {
	if rc.client == nil {
		return ErrDBNotInit
	}

	return rc.client.Watch(rc.ctx, func(tx *redis.Tx) error {
		_, err := tx.TxPipelined(rc.ctx, func(p redis.Pipeliner) error {
			txPipe := &RedisTxPipe{
				pipeliner: p,
				ctx:       rc.ctx,
			}
			return fn(txPipe)
		})
		if err == redis.Nil {
			return nil
		}
		return err
	})
}

func (rc *RedisCache) Publish(channel string, message any) error {
	if rc.client == nil {
		return ErrDBNotInit
	}

	if _, ok := message.(string); !ok {
		message = parser.MarshalJson(message)
	}

	return rc.client.Publish(rc.ctx, channel, message).Err()
}

func (rc *RedisCache) Subscribe(channel string) (<-chan []byte, func()) {
	pubsub := rc.client.Subscribe(rc.ctx, channel)
	ch := make(chan []byte)
	connectionEstablished := make(chan bool)

	go func() {
		defer close(ch)
		defer close(connectionEstablished)

		alive := true
		for alive {
			iface, err := pubsub.Receive(context.Background())
			if err != nil {
				log.Error("failed to receive message from redis: %s, will retry in 1 second", err.Error())
				time.Sleep(1 * time.Second)
				continue
			}
			switch data := iface.(type) {
			case *redis.Subscription:
				connectionEstablished <- true
			case *redis.Message:
				// Send the raw payload bytes
				ch <- []byte(data.Payload)
			case *redis.Pong:
			default:
				alive = false
			}
		}
	}()

	// wait for the connection to be established
	<-connectionEstablished

	return ch, func() {
		pubsub.Close()
	}
}
