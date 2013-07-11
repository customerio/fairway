package fairway

import (
	"github.com/garyburd/redigo/redis"
	"time"
)

type QueueDefinition struct {
	name    string
	channel string
}

type Config struct {
	Namespace string
	Facet     func(message *Msg) string
	queues    []*QueueDefinition
	redisPool *redis.Pool
}

func (c *Config) AddQueue(name, channel string) {
	c.queues = append(c.queues, &QueueDefinition{name, channel})
}

func (c *Config) scripts() *scripts {
	return newScripts(c)
}

func NewConfig(server string, poolSize int) *Config {
	return &Config{
		"fairway",
		func(message *Msg) string { return "default" },
		[]*QueueDefinition{},
		&redis.Pool{
			MaxIdle:     poolSize,
			MaxActive:   poolSize,
			IdleTimeout: 240 * time.Second,
			Dial: func() (redis.Conn, error) {
				c, err := redis.Dial("tcp", server)
				if err != nil {
					return nil, err
				}
				return c, err
			},
			TestOnBorrow: func(c redis.Conn, t time.Time) error {
				_, err := c.Do("PING")
				return err
			},
		},
	}
}
