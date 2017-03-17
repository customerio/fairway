package fairway

import (
	"fmt"

	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/customerio/redigo/redis"
)

func ChanneledConnectionSpec(c gospec.Context) {
	config := NewConfig("localhost:6379", "15", 2)
	config.AddQueue("myqueue", "typea")
	config.AddQueue("myqueue2", "typeb")

	conn := NewChanneledConnection(config, func(message *Msg) string {
		channel, _ := message.Get("type").String()
		return fmt.Sprint("channel:type", channel, ":channel")
	})

	c.Specify("Deliver", func() {
		c.Specify("only queues up message for matching queues", func() {
			r := config.Pool.Get()
			defer r.Close()

			count, _ := redis.Int(r.Do("llen", "fairway:myqueue:default"))
			c.Expect(count, Equals, 0)
			count, _ = redis.Int(r.Do("llen", "fairway:myqueue2:default"))
			c.Expect(count, Equals, 0)

			msg, _ := NewMsg(map[string]interface{}{"type": "a"})

			conn.Deliver(msg)

			count, _ = redis.Int(r.Do("llen", "fairway:myqueue:default"))
			c.Expect(count, Equals, 1)
			count, _ = redis.Int(r.Do("llen", "fairway:myqueue2:default"))
			c.Expect(count, Equals, 0)
		})
	})

	c.Specify("DeliverBytes", func() {
		c.Specify("only queues up message for matching queues", func() {
			r := config.Pool.Get()
			defer r.Close()

			count, _ := redis.Int(r.Do("llen", "fairway:myqueue:default"))
			c.Expect(count, Equals, 0)
			count, _ = redis.Int(r.Do("llen", "fairway:myqueue2:default"))
			c.Expect(count, Equals, 0)

			msg, _ := NewMsg(map[string]interface{}{"type": "a"})

			conn.DeliverBytes("channel:typea:channel", "default", msg.Original)

			count, _ = redis.Int(r.Do("llen", "fairway:myqueue:default"))
			c.Expect(count, Equals, 1)
			count, _ = redis.Int(r.Do("llen", "fairway:myqueue2:default"))
			c.Expect(count, Equals, 0)
		})
	})
}
