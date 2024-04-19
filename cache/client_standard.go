package cache

import (
	"context"
	"github.com/pkg/errors"
	"github.com/redis/go-redis/v9"
	"time"
)

// StandardClientAdapter client
type StandardClientAdapter struct {
	client *redis.Client
}

func (c *StandardClientAdapter) Del(ctx context.Context, keys ...string) (int64, error) {
	return c.client.Del(ctx, keys...).Result()
}

func (c *StandardClientAdapter) Exists(ctx context.Context, keys ...string) (int64, error) {
	return c.client.Exists(ctx, keys...).Result()
}

func (c *StandardClientAdapter) Expire(ctx context.Context, key string, expiration time.Duration) (bool, error) {
	return c.client.Expire(ctx, key, expiration).Result()
}

func (c *StandardClientAdapter) Get(ctx context.Context, key string) (string, error) {
	return c.client.Get(ctx, key).Result()
}

func (c *StandardClientAdapter) GetRange(ctx context.Context, key string, start, end int64) (string, error) {
	return c.client.GetRange(ctx, key, start, end).Result()
}

func (c *StandardClientAdapter) MGet(ctx context.Context, keys ...string) ([]any, error) {
	return c.client.MGet(ctx, keys...).Result()
}

func (c *StandardClientAdapter) StrLen(ctx context.Context, key string) (int64, error) {
	return c.client.StrLen(ctx, key).Result()
}

func (c *StandardClientAdapter) Set(ctx context.Context, key string, value any, expiration time.Duration) (string, error) {
	return c.client.Set(ctx, key, value, expiration).Result()
}

func (c *StandardClientAdapter) SetNX(ctx context.Context, key string, value any, expiration time.Duration) (bool, error) {
	return c.client.SetNX(ctx, key, value, expiration).Result()
}

func (c *StandardClientAdapter) SetEX(ctx context.Context, key string, value any, expiration time.Duration) (string, error) {
	return c.client.SetEx(ctx, key, value, expiration).Result()
}

func (c *StandardClientAdapter) MSet(ctx context.Context, values ...any) (string, error) {
	return c.client.MSet(ctx, values...).Result()

}

func (c *StandardClientAdapter) MSetNX(ctx context.Context, values ...any) (bool, error) {
	return c.client.MSetNX(ctx, values...).Result()
}

func (c *StandardClientAdapter) LPush(ctx context.Context, key string, values ...any) (int64, error) {
	return c.client.LPush(ctx, key, values...).Result()
}

func (c *StandardClientAdapter) LPushX(ctx context.Context, key string, values ...any) (int64, error) {
	return c.client.LPushX(ctx, key, values...).Result()
}

func (c *StandardClientAdapter) RPush(ctx context.Context, key string, values ...any) (int64, error) {
	return c.client.RPush(ctx, key, values...).Result()
}

func (c *StandardClientAdapter) RPushX(ctx context.Context, key string, values ...any) (int64, error) {
	return c.client.RPushX(ctx, key, values...).Result()
}

func (c *StandardClientAdapter) BLPop(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	return c.client.BLPop(ctx, timeout, keys...).Result()
}

func (c *StandardClientAdapter) BRPop(ctx context.Context, timeout time.Duration, keys ...string) ([]string, error) {
	return c.client.BRPop(ctx, timeout, keys...).Result()
}

func (c *StandardClientAdapter) GetOrSet(ctx context.Context, key string, f RedisFunc, expiration time.Duration) (any, error) {
	v, err := c.client.Get(ctx, key).Result()

	if err == redis.Nil {
		// 注意不同函数的处理逻辑不同，查询结果为空，有的会返回错误，有的返回正常，这里默认为空则返回错误
		value, valueErr := f(ctx)

		if valueErr != nil {
			return nil, errors.WithStack(valueErr)
		}

		_, err = c.client.Set(ctx, key, value, expiration).Result()
		return value, errors.WithMessage(err, "redis set失败")
	}

	if err != nil {
		return nil, errors.WithMessage(err, "redis查询错误")
	}
	return v, nil
}
