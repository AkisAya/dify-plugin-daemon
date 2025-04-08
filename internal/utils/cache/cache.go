package cache

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/langgenius/dify-plugin-daemon/internal/types/app"
)

var (
	ErrDBNotInit   = errors.New("cache client not init")
	ErrNotFound    = errors.New("key not found")
	ErrLockTimeout = errors.New("lock timeout")

	ErrCmdNotSupport = errors.New("unsupported command")
)

// Cache defines the interface for cache operations
type Cache interface {
	Close() error

	Store(key string, value any, expire time.Duration) error

	store(key string, value any, expire time.Duration) error

	Get(key string) ([]byte, error)

	get(key string) ([]byte, error)

	GetString(key string) (string, error)

	Del(key string) error

	del(key string) error

	Exist(key string) (int64, error)

	Increase(key string) (int64, error)

	Decrease(key string) (int64, error)

	SetExpire(key string, expire time.Duration) error

	SetMapField(key string, v map[string]any) error
	SetMapOneField(key string, field string, value any) error
	GetMapField(key string, field string) ([]byte, error)
	GetMapFieldString(key string, field string) (string, error)
	DelMapField(key string, field string) error
	GetMap(key string) (map[string]string, error)

	ScanKeys(match string) ([]string, error)
	ScanKeysAsync(match string, fn func([]string) error) error
	ScanMap(key string, match string) (map[string]string, error)
	ScanMapAsync(key string, match string, fn func(map[string]string) error) error

	SetNX(key string, value any, expire time.Duration) (bool, error)

	Lock(key string, expire time.Duration, tryLockTimeout time.Duration) error
	Unlock(key string) error
	Expire(key string, expire time.Duration) (bool, error)

	Transaction(fn func(txp TxPipe) error) error

	Publish(channel string, message any) error
	Subscribe(channel string) (<-chan []byte, func())
}

type TxPipe interface {
	Store(key string, value any, expire time.Duration) error
	SetNX(key string, value any, expire time.Duration) (bool, error)
}

var cacheInstance Cache

// InitCache initializes a cache provider
func InitCache(conf *app.Config) error {
	var err error
	switch conf.CacheProvider {
	case "redis":
		if conf.RedisHost == "" || conf.RedisPort == 0 {
			return errors.New("redis host or port is empty")
		}
		addr := fmt.Sprintf("%s:%d", conf.RedisHost, conf.RedisPort)
		cacheInstance, err = InitRedisCache(addr, conf.RedisPass, conf.RedisUseSsl, conf.RedisDB)
		if err != nil {
			return err
		}
	default:
		return errors.New("unsupported cache provider")
	}
	return nil
}

// GetCacheInstance returns the current cache instance
func GetCacheInstance() Cache {
	return cacheInstance
}

// SetCacheInstance sets the current cache instance (useful for testing with mocks)
func SetCacheInstance(cache Cache) {
	cacheInstance = cache
}

func serialKey(keys ...string) string {
	return strings.Join(append(
		[]string{"plugin_daemon"},
		keys...,
	), ":")
}

func InitRedisClient(addr, password string, useSsl bool, db int) error {
	var err error
	cacheInstance, err = InitRedisCache(addr, password, useSsl, db)
	return err
}
