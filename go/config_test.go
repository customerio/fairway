package fairway

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

func ConfigSpec(c gospec.Context) {
	// Load test instance of redis on port 6400
	config := NewConfig("localhost:6400", 10)

	c.Specify("NewConfig", func() {
		c.Specify("namespace is fairway", func() {
			c.Expect(config.Namespace, Equals, "fairway")
		})

		c.Specify("sets the facet to always return 'default'", func() {
			c.Expect(config.Facet(NewMsg(make([]string, 0))), Equals, "default")
		})

		c.Specify("doesn't have any defined queues", func() {
			c.Expect(len(config.queues), Equals, 0)
		})
	})

	c.Specify("sets redis pool size", func() {
		c.Expect(config.redisPool.MaxIdle, Equals, 10)
		c.Expect(config.redisPool.MaxActive, Equals, 10)
		config = NewConfig("localhost:6400", 20)
		c.Expect(config.redisPool.MaxIdle, Equals, 20)
		c.Expect(config.redisPool.MaxActive, Equals, 20)
	})

	c.Specify("can specify custom namespace", func() {
		config.Namespace = "mynamespace"
		c.Expect(config.Namespace, Equals, "mynamespace")
	})

	c.Specify("can specify custom facet", func() {
		config.Facet = func(message *Msg) string {
			return "myfacet"
		}
		c.Expect(config.Facet(NewMsg(make([]string, 0))), Equals, "myfacet")
	})

	c.Specify("can define a queue", func() {
		config.AddQueue("myqueue", "default")
		c.Expect(len(config.queues), Equals, 1)
		c.Expect(*config.queues[0], Equals, QueueDefinition{"myqueue", "default"})
	})
}
