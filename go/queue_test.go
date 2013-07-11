package fairway

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/garyburd/redigo/redis"
)

func QueueSpec(c gospec.Context) {
	// Load test instance of redis on port 6400
	config := NewConfig("localhost:6400", 2)
	config.AddQueue("myqueue", ".*")
	conn := NewConnection(config)
	queue := NewQueue(conn, "myqueue")

	c.Specify("NewQueue", func() {
	})

	c.Specify("Pull", func() {
		c.Specify("pulls a message off the queue using FIFO", func() {
			msg1 := NewMsg(map[string]string{"name": "mymessage1"})
			msg2 := NewMsg(map[string]string{"name": "mymessage2"})

			conn.Deliver(msg1)
			conn.Deliver(msg2)

			queueName, message := queue.Pull()
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg1.json())

			queueName, message = queue.Pull()
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg2.json())
		})

		c.Specify("pulls from facets of the queue in round-robin", func() {
			config.Facet = func(msg *Msg) string {
				str, _ := msg.Get("facet").String()
				return str
			}

			msg1 := NewMsg(map[string]string{"facet": "1", "name": "mymessage1"})
			msg2 := NewMsg(map[string]string{"facet": "1", "name": "mymessage2"})
			msg3 := NewMsg(map[string]string{"facet": "2", "name": "mymessage3"})

			conn.Deliver(msg1)
			conn.Deliver(msg2)
			conn.Deliver(msg3)

			_, message := queue.Pull()
			c.Expect(message.json(), Equals, msg1.json())
			_, message = queue.Pull()
			c.Expect(message.json(), Equals, msg3.json())
			_, message = queue.Pull()
			c.Expect(message.json(), Equals, msg2.json())
		})

		c.Specify("removes facet from active list if it becomes empty", func() {
			r := config.redisPool.Get()
			defer r.Close()

			conn.Deliver(NewMsg(map[string]string{}))

			count, _ := redis.Int(r.Do("scard", "fairway:myqueue:active_facets"))
			c.Expect(count, Equals, 1)

			queue.Pull()

			count, _ = redis.Int(r.Do("scard", "fairway:myqueue:active_facets"))
			c.Expect(count, Equals, 0)
		})

		c.Specify("returns nil if there are no messages to receive", func() {
			conn.Deliver(NewMsg(map[string]string{}))

			queueName, message := queue.Pull()
			c.Expect(queueName, Equals, "myqueue")
			queueName, message = queue.Pull()
			c.Expect(queueName, Equals, "")
			c.Expect(message, IsNil)
		})
	})
}
