package fairway

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/customerio/redigo/redis"

	"time"
)

func QueueSpec(c gospec.Context) {
	config := NewConfig("localhost:6379", "15", 2)
	config.AddQueue("myqueue", ".*")
	conn := NewConnection(config)
	queue := NewQueue(conn, "myqueue")

	c.Specify("NewQueue", func() {
	})

	c.Specify("Pull", func() {
		c.Specify("pulls a message off the queue using FIFO", func() {
			msg1, _ := NewMsg(map[string]string{"name": "mymessage1"})
			msg2, _ := NewMsg(map[string]string{"name": "mymessage2"})

			conn.Deliver(msg1)
			conn.Deliver(msg2)

			queueName, message := queue.Pull(time.Now().Unix())
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg1.json())

			queueName, message = queue.Pull(time.Now().Unix())
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg2.json())
		})

		c.Specify("places pulled message on inflight sorted set until acknowledged", func() {
			msg1, _ := NewMsg(map[string]string{"name": "mymessage1"})

			conn.Deliver(msg1)

			c.Expect(len(queue.Inflight()), Equals, 0)

			queueName, message := queue.Pull(time.Now().Unix())
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg1.json())

			c.Expect(len(queue.Inflight()), Equals, 1)
			c.Expect(queue.Inflight()[0], Equals, msg1.json())

			queue.Ack(msg1)

			c.Expect(len(queue.Inflight()), Equals, 0)
		})

		c.Specify("pulls from inflight message set if messages are unacknowledged", func() {
			msg1, _ := NewMsg(map[string]string{"name": "mymessage1"})
			msg2, _ := NewMsg(map[string]string{"name": "mymessage2"})

			conn.Deliver(msg1)
			conn.Deliver(msg2)

			queueName, message := queue.Pull(time.Now().Add(-10 * time.Minute).Unix())
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg1.json())

			queueName, message = queue.Pull(time.Now().Unix())
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg1.json())

			queueName, message = queue.Pull(time.Now().Unix())
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg2.json())
		})

		c.Specify("doesn't place pulled message on inflight sorted set if 0 timestamp", func() {
			msg1, _ := NewMsg(map[string]string{"name": "mymessage1"})

			conn.Deliver(msg1)

			c.Expect(len(queue.Inflight()), Equals, 0)

			queueName, message := queue.Pull(0)
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg1.json())

			c.Expect(len(queue.Inflight()), Equals, 0)
		})

		c.Specify("doesn't pull from inflight message set if timestamp is 0", func() {
			msg1, _ := NewMsg(map[string]string{"name": "mymessage1"})
			msg2, _ := NewMsg(map[string]string{"name": "mymessage2"})

			conn.Deliver(msg1)
			conn.Deliver(msg2)

			queueName, message := queue.Pull(0)
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg1.json())

			queueName, message = queue.Pull(time.Now().Unix())
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg2.json())
		})

		c.Specify("pulls from facets of the queue in round-robin", func() {
			config.Facet = func(msg *Msg) string {
				str, _ := msg.Get("facet").String()
				return str
			}

			msg1, _ := NewMsg(map[string]string{"facet": "1", "name": "mymessage1"})
			msg2, _ := NewMsg(map[string]string{"facet": "1", "name": "mymessage2"})
			msg3, _ := NewMsg(map[string]string{"facet": "2", "name": "mymessage3"})

			conn.Deliver(msg1)
			conn.Deliver(msg2)
			conn.Deliver(msg3)

			_, message := queue.Pull(time.Now().Unix())
			c.Expect(message.json(), Equals, msg1.json())
			_, message = queue.Pull(time.Now().Unix())
			c.Expect(message.json(), Equals, msg3.json())
			_, message = queue.Pull(time.Now().Unix())
			c.Expect(message.json(), Equals, msg2.json())
		})

		c.Specify("removes facet from active list if it becomes empty", func() {
			r := config.Pool.Get()
			defer r.Close()

			msg, _ := NewMsg(map[string]string{})
			conn.Deliver(msg)

			count, _ := redis.Int(r.Do("scard", "fairway:myqueue:active_facets"))
			c.Expect(count, Equals, 1)

			queue.Pull(time.Now().Unix())

			count, _ = redis.Int(r.Do("scard", "fairway:myqueue:active_facets"))
			c.Expect(count, Equals, 0)
		})

		c.Specify("returns nil if there are no messages to receive", func() {
			msg, _ := NewMsg(map[string]string{})
			conn.Deliver(msg)

			queueName, message := queue.Pull(time.Now().Unix())
			c.Expect(queueName, Equals, "myqueue")
			queueName, message = queue.Pull(time.Now().Unix())
			c.Expect(queueName, Equals, "")
			c.Expect(message, IsNil)
		})
	})
}
