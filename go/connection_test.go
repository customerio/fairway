package fairway

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func ConnectionSpec(c gospec.Context) {
	config := NewConfig("localhost:6379", "15", 2)
	config.AddQueue("myqueue", ".*")
	conn := NewConnection(config)

	c.Specify("NewConnection", func() {
		c.Specify("registers any queues defined in configuration", func() {
			c.Expect(len(conn.Queues()), Equals, 1)
			config.AddQueue("myqueue2", ".*")
			conn.RegisterQueues()
			c.Expect(len(conn.Queues()), Equals, 2)
		})

		c.Specify("stores registered queues in redis", func() {
			r := config.redisPool.Get()
			defer r.Close()

			values, _ := redis.Strings(r.Do("hgetall", "fairway:registered_queues"))

			expected := []string{"myqueue", ".*"}

			for i, str := range values {
				c.Expect(str, Equals, expected[i])
			}
		})
	})

	c.Specify("Queues", func() {
		c.Specify("returns a Queue for every currently registered queue", func() {
			c.Expect(*conn.Queues()[0], Equals, *NewQueue(conn, "myqueue"))
		})
	})

	c.Specify("Deliver", func() {
		c.Specify("adds message to the facet for the queue", func() {
			r := config.redisPool.Get()
			defer r.Close()

			count, _ := redis.Int(r.Do("llen", "fairway:myqueue:default"))
			c.Expect(count, Equals, 0)

			msg, _ := NewMsg(map[string]string{"name": "mymessage"})

			conn.Deliver(msg)

			count, _ = redis.Int(r.Do("llen", "fairway:myqueue:default"))
			c.Expect(count, Equals, 1)

			value, _ := redis.String(r.Do("lindex", "fairway:myqueue:default", 0))
			c.Expect(value, Equals, msg.json())
		})

		c.Specify("adds facets to the list of active facets", func() {
			r := config.redisPool.Get()
			defer r.Close()

			facets, _ := redis.Strings(r.Do("smembers", "fairway:myqueue:active_facets"))
			c.Expect(len(facets), Equals, 0)

			msg, _ := NewMsg(map[string]string{})

			conn.Deliver(msg)

			facets, _ = redis.Strings(r.Do("smembers", "fairway:myqueue:active_facets"))
			c.Expect(len(facets), Equals, 1)
			c.Expect(facets[0], Equals, "default")
		})

		c.Specify("pushes facet onto the facet queue", func() {
			r := config.redisPool.Get()
			defer r.Close()

			count, _ := redis.Int(r.Do("llen", "fairway:myqueue:facet_queue"))
			c.Expect(count, Equals, 0)

			msg, _ := NewMsg(map[string]string{})

			conn.Deliver(msg)

			count, _ = redis.Int(r.Do("llen", "fairway:myqueue:facet_queue"))
			c.Expect(count, Equals, 1)

			value, _ := redis.String(r.Do("lindex", "fairway:myqueue:facet_queue", 0))
			c.Expect(value, Equals, "default")
		})

		c.Specify("doesn't push facet if already active", func() {
			r := config.redisPool.Get()
			defer r.Close()

			r.Do("sadd", "fairway:myqueue:active_facets", "default")

			msg, _ := NewMsg(map[string]string{})

			conn.Deliver(msg)

			count, _ := redis.Int(r.Do("llen", "fairway:myqueue:facet_queue"))
			c.Expect(count, Equals, 0)
		})

		c.Specify("returns nil if delivery succeeds", func() {
			msg, _ := NewMsg(map[string]string{})
			err := conn.Deliver(msg)
			c.Expect(err, IsNil)
		})

		c.Specify("returns error if delivery fails", func() {
			config := NewConfig("localhost:9999", "15", 2)
			conn := NewConnection(config)

			msg, _ := NewMsg(map[string]string{})
			err := conn.Deliver(msg)
			c.Expect(err.Error(), Equals, "dial tcp 127.0.0.1:9999: connection refused")
		})
	})
}
