package redis

import (
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"golang.org/x/net/context"
	"time"
)

// ClusterClientAdapter 集群模式client
type ClusterClientAdapter struct {
	clusterClient *redis.ClusterClient
}

func (c *ClusterClientAdapter) Del(ctx context.Context, keys ...string) (int64, error) {
	return c.clusterClient.Del(ctx, keys...).Result()
}

func (c *ClusterClientAdapter) Exists(ctx context.Context, keys ...string) (int64, error) {
	return c.clusterClient.Exists(ctx, keys...).Result()
}

func (c *ClusterClientAdapter) Expire(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	return c.clusterClient.Expire(ctx, key, expiration).Result()
}

func (c *ClusterClientAdapter) Get(ctx context.Context, key string) (string, error) {
	return c.clusterClient.Get(ctx, key).Result()
}

func (c *ClusterClientAdapter) GetRange(ctx context.Context, key string, start, end int64) (string, error) {
	return c.clusterClient.GetRange(ctx, key, start, end).Result()
}

func (c *ClusterClientAdapter) MGet(ctx context.Context, keys ...string) ([]any, error) {
	return c.clusterClient.MGet(ctx, keys...).Result()
}

func (c *ClusterClientAdapter) StrLen(ctx context.Context, key string) (int64, error) {
	return c.clusterClient.StrLen(ctx, key).Result()
}

func (c *ClusterClientAdapter) Set(ctx context.Context, key string, value any, expiration time.Duration) (string, error) {
	return c.clusterClient.Set(ctx, key, value, expiration).Result()
}

func (c *ClusterClientAdapter) SetNX(ctx context.Context, key string, value any, expiration time.Duration) (bool, error) {
	return c.clusterClient.SetNX(ctx, key, value, expiration).Result()
}

func (c *ClusterClientAdapter) SetEX(ctx context.Context, key string, value any, expiration time.Duration) (string, error) {
	return c.clusterClient.SetEx(ctx, key, value, expiration).Result()
}

func (c *ClusterClientAdapter) MSet(ctx context.Context, values ...any) (string, error) {
	return c.clusterClient.MSet(ctx, values...).Result()

}

func (c *ClusterClientAdapter) MSetNX(ctx context.Context, values ...any) (bool, error) {
	return c.clusterClient.MSetNX(ctx, values...).Result()
}

func (c *ClusterClientAdapter) LPush(ctx context.Context, key string, values ...any) (int64, error) {
	return c.clusterClient.LPush(ctx, key, values...).Result()
}

func (c *ClusterClientAdapter) LPushX(ctx context.Context, key string, values ...any) (int64, error) {
	return c.clusterClient.LPushX(ctx, key, values...).Result()
}

func (c *ClusterClientAdapter) RPush(ctx context.Context, key string, values ...any) (int64, error) {
	return c.clusterClient.RPush(ctx, key, values...).Result()
}

func (c *ClusterClientAdapter) RPushX(ctx context.Context, key string, values ...any) (int64, error) {
	return c.clusterClient.RPushX(ctx, key, values...).Result()
}

func (c *ClusterClientAdapter) BLPop(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	return c.clusterClient.BLPop(ctx, timeout, keys...).Result()
}

func (c *ClusterClientAdapter) BRPop(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	return c.clusterClient.BRPop(ctx, timeout, keys...).Result()
}

func (c *ClusterClientAdapter) GetOrSet(ctx context.Context, key string, f RedisFunc, expiration time.Duration) (any, error) {
	v, err := c.clusterClient.Get(ctx, key).Result()

	if err == redis.Nil {
		// 注意不同函数的处理逻辑不同，查询结果为空，有的会返回错误，有的返回正常，这里默认为空则返回错误
		value, valueErr := f(ctx)

		if valueErr != nil {
			return nil, errors.WithStack(valueErr)
		}

		_, err = c.clusterClient.Set(ctx, key, value, expiration).Result()
		return value, errors.WithMessage(err, "redis set失败")
	}

	if err != nil {
		return nil, errors.WithMessage(err, "redis查询错误")
	}
	return v, nil
}
