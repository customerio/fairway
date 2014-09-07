package fairway

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
	"github.com/customerio/redigo/redis"
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

			queueName, message := queue.Pull(-1)
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg1.json())

			queueName, message = queue.Pull(-1)
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg2.json())
		})

		c.Specify("places pulled message on inflight sorted set until acknowledged", func() {
			msg1, _ := NewMsg(map[string]string{"name": "mymessage1"})

			conn.Deliver(msg1)

			c.Expect(len(queue.Inflight()), Equals, 0)

			queueName, message := queue.Pull(100)
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

			queueName, message := queue.Pull(0)
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg1.json())

			queueName, message = queue.Pull(10)
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg1.json())

			queueName, message = queue.Pull(10)
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg2.json())
		})

		c.Specify("allows puller to ping to keep message inflight", func() {
			msg1, _ := NewMsg(map[string]string{"name": "mymessage1"})
			msg2, _ := NewMsg(map[string]string{"name": "mymessage2"})

			conn.Deliver(msg1)
			conn.Deliver(msg2)

			queueName, message := queue.Pull(0)
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg1.json())

			// Extends time before message is resent
			queue.Ping(msg1, 10)

			queueName, message = queue.Pull(10)
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg2.json())

			// Sets time for message to resend to now
			queue.Ping(msg1, 0)

			queueName, message = queue.Pull(10)
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg1.json())
		})

		c.Specify("set limits messages inflight", func() {
			limit, err := queue.InflightLimit()

			c.Expect(limit, Equals, 0)
			c.Expect(err, IsNil)

			queue.SetInflightLimit(1)

			limit, err = queue.InflightLimit()

			c.Expect(limit, Equals, 1)
			c.Expect(err, IsNil)
		})

		c.Specify("limits messages inflight", func() {
			r := config.Pool.Get()
			defer r.Close()

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

			queue.SetInflightLimit(1)

			_, message := queue.Pull(2)
			c.Expect(message.json(), Equals, msg1.json())

			count, _ := redis.Int(r.Do("get", "fairway:myqueue:1:inflight"))
			c.Expect(count, Equals, 1)

			_, message = queue.Pull(2)
			c.Expect(message.json(), Equals, msg3.json())

			count, _ = redis.Int(r.Do("get", "fairway:myqueue:1:inflight"))
			c.Expect(count, Equals, 1)

			count, _ = redis.Int(r.Do("get", "fairway:myqueue:2:inflight"))
			c.Expect(count, Equals, 1)

			_, message = queue.Pull(2)
			c.Expect(message, IsNil)
			_, message = queue.Pull(2)
			c.Expect(message, IsNil)

			count, _ = redis.Int(r.Do("get", "fairway:myqueue:1:inflight"))
			c.Expect(count, Equals, 1)

			count, _ = redis.Int(r.Do("get", "fairway:myqueue:2:inflight"))
			c.Expect(count, Equals, 1)

			queue.Ack(msg1)

			count, err := redis.Int(r.Do("get", "fairway:myqueue:1:inflight"))
			c.Expect(count, Equals, 0)
			c.Expect(err.Error(), Equals, "redigo: nil returned")

			count, _ = redis.Int(r.Do("get", "fairway:myqueue:2:inflight"))
			c.Expect(count, Equals, 1)

			_, message = queue.Pull(2)
			c.Expect(message.json(), Equals, msg2.json())

			count, _ = redis.Int(r.Do("get", "fairway:myqueue:1:inflight"))
			c.Expect(count, Equals, 1)

			count, _ = redis.Int(r.Do("get", "fairway:myqueue:2:inflight"))
			c.Expect(count, Equals, 1)
		})

		c.Specify("if inflight limit is 0, no limit", func() {
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

			queue.SetInflightLimit(0)

			_, message := queue.Pull(2)
			c.Expect(message.json(), Equals, msg1.json())
			_, message = queue.Pull(2)
			c.Expect(message.json(), Equals, msg3.json())
			_, message = queue.Pull(2)
			c.Expect(message.json(), Equals, msg2.json())
		})

		c.Specify("doesn't place pulled message on inflight sorted set if inflight is disabled", func() {
			msg1, _ := NewMsg(map[string]string{"name": "mymessage1"})

			conn.Deliver(msg1)

			c.Expect(len(queue.Inflight()), Equals, 0)

			queueName, message := queue.Pull(-1)
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg1.json())

			c.Expect(len(queue.Inflight()), Equals, 0)
		})

		c.Specify("doesn't pull from inflight message set if inflight is disabled", func() {
			msg1, _ := NewMsg(map[string]string{"name": "mymessage1"})
			msg2, _ := NewMsg(map[string]string{"name": "mymessage2"})

			conn.Deliver(msg1)
			conn.Deliver(msg2)

			queueName, message := queue.Pull(-1)
			c.Expect(queueName, Equals, "myqueue")
			c.Expect(message.json(), Equals, msg1.json())

			queueName, message = queue.Pull(-1)
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

			_, message := queue.Pull(-1)
			c.Expect(message.json(), Equals, msg1.json())
			_, message = queue.Pull(-1)
			c.Expect(message.json(), Equals, msg3.json())
			_, message = queue.Pull(-1)
			c.Expect(message.json(), Equals, msg2.json())
		})

		c.Specify("removes facet from active list if it becomes empty", func() {
			r := config.Pool.Get()
			defer r.Close()

			msg, _ := NewMsg(map[string]string{})
			conn.Deliver(msg)

			count, _ := redis.Int(r.Do("scard", "fairway:myqueue:active_facets"))
			c.Expect(count, Equals, 1)

			queue.Pull(-1)

			count, _ = redis.Int(r.Do("scard", "fairway:myqueue:active_facets"))
			c.Expect(count, Equals, 0)
		})

		c.Specify("returns nil if there are no messages to receive", func() {
			msg, _ := NewMsg(map[string]string{})
			conn.Deliver(msg)

			queueName, message := queue.Pull(-1)
			c.Expect(queueName, Equals, "myqueue")
			queueName, message = queue.Pull(-1)
			c.Expect(queueName, Equals, "")
			c.Expect(message, IsNil)
		})
	})
}
