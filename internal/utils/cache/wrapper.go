package cache

import (
	"time"

	"github.com/langgenius/dify-plugin-daemon/internal/utils/parser"
	"github.com/redis/go-redis/v9"
)

// These wrapper functions maintain backward compatibility with existing code
// that uses the previous global functions.

// Close the cache client
func Close() error {
	if cacheInstance == nil {
		return ErrDBNotInit
	}
	return cacheInstance.Close()
}

func Store(key string, value any, expire time.Duration, context ...redis.Cmdable) error {
	return store(serialKey(key), value, expire)
}

func store(key string, value any, expire time.Duration, context ...redis.Cmdable) error {
	if cacheInstance == nil {
		return ErrDBNotInit
	}
	return cacheInstance.store(key, value, expire)
}

func Get[T any](key string, context ...redis.Cmdable) (*T, error) {
	return get[T](serialKey(key))
}

func get[T any](key string, context ...redis.Cmdable) (*T, error) {
	if cacheInstance == nil {
		return nil, ErrDBNotInit
	}

	// Get raw value as bytes from the cache
	bytes, err := cacheInstance.get(key)
	if err != nil {
		return nil, err
	}

	// Unmarshal bytes to the requested type
	result, err := parser.UnmarshalCBOR[T](bytes)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func GetString(key string, context ...redis.Cmdable) (string, error) {
	if cacheInstance == nil {
		return "", ErrDBNotInit
	}
	// Ignore the context parameter
	return cacheInstance.GetString(key)
}

func Del(key string, context ...redis.Cmdable) error {
	return del(serialKey(key))
}

func del(key string, context ...redis.Cmdable) error {
	if cacheInstance == nil {
		return ErrDBNotInit
	}
	// Ignore the context parameter
	return cacheInstance.del(key)
}

func Exist(key string, context ...redis.Cmdable) (int64, error) {
	if cacheInstance == nil {
		return 0, ErrDBNotInit
	}
	// Ignore the context parameter
	return cacheInstance.Exist(key)
}

func Increase(key string, context ...redis.Cmdable) (int64, error) {
	if cacheInstance == nil {
		return 0, ErrDBNotInit
	}
	// Ignore the context parameter
	return cacheInstance.Increase(key)
}

func Decrease(key string, context ...redis.Cmdable) (int64, error) {
	if cacheInstance == nil {
		return 0, ErrDBNotInit
	}
	// Ignore the context parameter
	return cacheInstance.Decrease(key)
}

func SetExpire(key string, time time.Duration, context ...redis.Cmdable) error {
	if cacheInstance == nil {
		return ErrDBNotInit
	}
	// Ignore the context parameter
	return cacheInstance.SetExpire(key, time)
}

func SetMapField(key string, v map[string]any, context ...redis.Cmdable) error {
	if cacheInstance == nil {
		return ErrDBNotInit
	}
	// Ignore the context parameter
	return cacheInstance.SetMapField(key, v)
}

func SetMapOneField(key string, field string, value any, context ...redis.Cmdable) error {
	if cacheInstance == nil {
		return ErrDBNotInit
	}
	// Ignore the context parameter
	return cacheInstance.SetMapOneField(key, field, value)
}

func GetMapField[T any](key string, field string, context ...redis.Cmdable) (*T, error) {
	if cacheInstance == nil {
		return nil, ErrDBNotInit
	}

	// Get bytes from cache
	bytes, err := cacheInstance.GetMapField(key, field)
	if err != nil {
		return nil, err
	}

	// Convert bytes to string for JSON unmarshaling
	valStr := string(bytes)

	// Deserialize to the requested type
	result, err := parser.UnmarshalJson[T](valStr)
	if err != nil {
		return nil, err
	}

	return &result, nil
}

func GetMapFieldString(key string, field string, context ...redis.Cmdable) (string, error) {
	if cacheInstance == nil {
		return "", ErrDBNotInit
	}
	// Ignore the context parameter
	return cacheInstance.GetMapFieldString(key, field)
}

func DelMapField(key string, field string, context ...redis.Cmdable) error {
	if cacheInstance == nil {
		return ErrDBNotInit
	}
	// Ignore the context parameter
	return cacheInstance.DelMapField(key, field)
}

func GetMap[V any](key string, context ...redis.Cmdable) (map[string]V, error) {
	if cacheInstance == nil {
		return nil, ErrDBNotInit
	}

	// Get all map fields as strings
	rawMap, err := cacheInstance.GetMap(key)
	if err != nil {
		return nil, err
	}

	// Transform each value to the desired type
	result := make(map[string]V)
	for k, v := range rawMap {
		// Unmarshal to the desired type
		value, err := parser.UnmarshalJson[V](v)
		if err != nil {
			continue
		}

		result[k] = value
	}

	return result, nil
}

func ScanKeys(match string, context ...redis.Cmdable) ([]string, error) {
	return nil, ErrCmdNotSupport

	// if cacheInstance == nil {
	// 	return nil, ErrDBNotInit
	// }
	// // Ignore the context parameter
	// return cacheInstance.ScanKeys(match)
}

func ScanKeysAsync(match string, fn func([]string) error, context ...redis.Cmdable) error {
	return ErrCmdNotSupport
	// if cacheInstance == nil {
	// 	return ErrDBNotInit
	// }
	// // Ignore the context parameter
	// return cacheInstance.ScanKeysAsync(match, fn)
}

func ScanMap[V any](key string, match string, context ...redis.Cmdable) (map[string]V, error) {
	if cacheInstance == nil {
		return nil, ErrDBNotInit
	}

	// Get scan results as map[string]string
	rawMap, err := cacheInstance.ScanMap(key, match)
	if err != nil {
		return nil, err
	}

	// Transform each value to the desired type
	result := make(map[string]V)
	for k, v := range rawMap {
		// Unmarshal to the desired type
		value, err := parser.UnmarshalJson[V](v)
		if err != nil {
			continue
		}

		result[k] = value
	}

	return result, nil
}

func ScanMapAsync[V any](key string, match string, fn func(map[string]V) error, context ...redis.Cmdable) error {
	if cacheInstance == nil {
		return ErrDBNotInit
	}

	// Create a wrapper function that converts types
	wrapper := func(rawMap map[string]string) error {
		// Transform each value to the desired type
		result := make(map[string]V)
		for k, v := range rawMap {
			// Unmarshal to the desired type
			value, err := parser.UnmarshalJson[V](v)
			if err != nil {
				continue
			}

			result[k] = value
		}

		return fn(result)
	}

	// Call the non-generic version with the wrapper
	return cacheInstance.ScanMapAsync(key, match, wrapper)
}

func SetNX[T any](key string, value T, expire time.Duration, context ...redis.Cmdable) (bool, error) {
	if cacheInstance == nil {
		return false, ErrDBNotInit
	}

	// Marshal the value
	bytes, err := parser.MarshalCBOR(value)
	if err != nil {
		return false, err
	}

	// Ignore the context parameter
	return cacheInstance.SetNX(key, bytes, expire)
}

// Lock key with expiration time
func Lock(key string, expire time.Duration, tryLockTimeout time.Duration, context ...redis.Cmdable) error {
	if cacheInstance == nil {
		return ErrDBNotInit
	}
	// Ignore the context parameter
	return cacheInstance.Lock(key, expire, tryLockTimeout)
}

func Unlock(key string, context ...redis.Cmdable) error {
	if cacheInstance == nil {
		return ErrDBNotInit
	}
	// Ignore the context parameter
	return cacheInstance.Unlock(key)
}

func Expire(key string, time time.Duration, context ...redis.Cmdable) (bool, error) {
	if cacheInstance == nil {
		return false, ErrDBNotInit
	}
	// Ignore the context parameter
	return cacheInstance.Expire(key, time)
}

func Transaction(fn func(txp TxPipe) error) error {
	if cacheInstance == nil {
		return ErrDBNotInit
	}

	return cacheInstance.Transaction(fn)
}

func Publish(channel string, message any, context ...redis.Cmdable) error {
	if cacheInstance == nil {
		return ErrDBNotInit
	}
	// Ignore the context parameter
	return cacheInstance.Publish(channel, message)
}

func Subscribe[T any](channel string) (<-chan T, func()) {
	if cacheInstance == nil {
		ch := make(chan T)
		close(ch)
		return ch, func() {}
	}

	// Get bytes channel from cache
	bytesCh, cleanup := cacheInstance.Subscribe(channel)

	// Create typed channel
	typedCh := make(chan T)

	// Start a goroutine to convert bytes to typed values
	go func() {
		defer close(typedCh)

		for bytes := range bytesCh {
			// Unmarshal to the desired type
			value, err := parser.UnmarshalJson[T](string(bytes))
			if err != nil {
				continue
			}
			typedCh <- value
		}
	}()

	return typedCh, cleanup
}
