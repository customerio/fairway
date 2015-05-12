package fairway

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

func FacetSpec(c gospec.Context) {
	config := NewConfig("localhost:6379", "15", 2)
	config.AddQueue("myqueue", ".*")
	conn := NewConnection(config)
	queue := NewQueue(conn, "myqueue")

	c.Specify("Length", func() {
		c.Specify("returns number of messages queues for a given facet", func() {
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

			l, _ := NewFacet(queue, "1").Length()
			c.Expect(l, Equals, 2)
			l, _ = NewFacet(queue, "2").Length()
			c.Expect(l, Equals, 1)
			l, _ = NewFacet(queue, "3").Length()
			c.Expect(l, Equals, 0)
		})
	})
}
