package fairway

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

func ConfigSpec(c gospec.Context) {
	config := NewConfig("localhost:6379", "15", 10)

	c.Specify("NewConfig", func() {
		c.Specify("namespace is fairway", func() {
			c.Expect(config.Namespace, Equals, "fairway")
		})

		c.Specify("sets the facet to always return 'default'", func() {
			msg, _ := NewMsg(make([]string, 0))
			c.Expect(config.Facet(msg), Equals, "default")
		})

		c.Specify("doesn't have any defined queues", func() {
			c.Expect(len(config.queues), Equals, 0)
		})
	})

	c.Specify("sets redis pool size", func() {
		c.Expect(config.Pool.MaxIdle, Equals, 10)
		c.Expect(config.Pool.MaxActive, Equals, 10)
		config = NewConfig("localhost:6379", "15", 20)
		c.Expect(config.Pool.MaxIdle, Equals, 20)
		c.Expect(config.Pool.MaxActive, Equals, 20)
	})

	c.Specify("can specify custom namespace", func() {
		config.Namespace = "mynamespace"
		c.Expect(config.Namespace, Equals, "mynamespace")
	})

	c.Specify("can specify custom facet", func() {
		config.Facet = func(message *Msg) string {
			return "myfacet"
		}
		msg, _ := NewMsg(make([]string, 0))
		c.Expect(config.Facet(msg), Equals, "myfacet")
	})

	c.Specify("can define a queue", func() {
		config.AddQueue("myqueue", "default")
		c.Expect(len(config.queues), Equals, 1)
		c.Expect(*config.queues[0], Equals, QueueDefinition{"myqueue", "default"})
	})
}
