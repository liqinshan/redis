package redis_test

import (
	"context"
	"fmt"
	"github.com/liqinshan/redis/cache"
	"github.com/redis/go-redis/v9"
	"strings"
)

var (
	Ctx   = context.Background()
	Cache cache.RedisClient
)

var (
	UserPrefix = ""
)

func InitRedis(cluster bool) {
	// 集群模式下，Addr有多个值，以逗号分开
	addr := ""
	pass := ""
	db := 0

	var err error

	if cluster {
		rdb := redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:    strings.Split(addr, ","),
			Password: pass,
		})

		pingErr := rdb.ForEachShard(Ctx, func(ctx context.Context, shard *redis.Client) error {
			return shard.Ping(ctx).Err()
		})
		if pingErr != nil {
			panic(pingErr)
		}

		Cache, err = cache.NewClientFactory(rdb)
	} else {
		rdb := redis.NewClient(&redis.Options{
			Addr:     addr,
			Password: pass,
			DB:       db,
		})

		_, pingErr := rdb.Ping(Ctx).Result()
		if pingErr != nil {
			panic(pingErr)
		}

		Cache, err = cache.NewClientFactory(rdb)
	}

	if err != nil {
		panic(err)
	}
}

func ExampleClient() {
	InitRedis(true)

	data, err := Cache.Get(Ctx, "aaa")
	if err != nil {
		fmt.Println("err", err)
	} else {
		fmt.Println(data)
	}

}
