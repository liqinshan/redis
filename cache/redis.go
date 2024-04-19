package cache

import (
	"context"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"time"
)

// RedisClient 定义接口，client和cluster实现这些接口，从而提供统一的方法能力
type RedisClient interface {
	// key相关
	// 基本上把go-redis的string_command.go中定义的方法照抄一遍，几个常用的即可
	// go-redis版本: 9.5.1
	Del(ctx context.Context, keys ...string) (int64, error)
	Exists(ctx context.Context, keys ...string) (int64, error)
	Expire(ctx context.Context, key string, expiration time.Duration) (bool, error)

	// string相关
	Get(ctx context.Context, key string) (string, error)
	GetRange(ctx context.Context, key string, start, end int64) (string, error)
	MGet(ctx context.Context, keys ...string) ([]any, error)
	StrLen(ctx context.Context, key string) (int64, error)

	Set(ctx context.Context, key string, value any, expiration time.Duration) (string, error)
	SetNX(ctx context.Context, key string, value any, expiration time.Duration) (bool, error)
	SetEX(ctx context.Context, key string, value any, expiration time.Duration) (string, error)
	MSet(ctx context.Context, values ...any) (string, error)
	MSetNX(ctx context.Context, values ...any) (bool, error)

	// list相关
	LPush(ctx context.Context, key string, values ...any) (int64, error)
	LPushX(ctx context.Context, key string, values ...any) (int64, error)
	RPush(ctx context.Context, key string, values ...any) (int64, error)
	RPushX(ctx context.Context, key string, values ...any) (int64, error)

	BLPop(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error)
	BRPop(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error)

	// 一些封装的魔法方法，方便使用
	GetOrSet(ctx context.Context, key string, f RedisFunc, expiration time.Duration) (any, error)
}

type RedisFunc func(ctx context.Context) (value any, err error)

// NewClientFactory 通用方法
func NewClientFactory(redisConfig any) (RedisClient, error) {
	switch client := redisConfig.(type) {
	case *redis.Client:
		return &StandardClientAdapter{client: client}, nil
	case *redis.ClusterClient:
		return &ClusterClientAdapter{clusterClient: client}, nil
	default:
		return nil, errors.New("redis模式错误")
	}
}
