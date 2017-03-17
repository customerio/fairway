package fairway

import (
	"github.com/customerio/gospec"
	. "github.com/customerio/gospec"
)

func MsgSpec(c gospec.Context) {
	c.Specify("NewMsg", func() {
		c.Specify("returns a new message with body as the content", func() {
			msg, _ := NewMsg(map[string]interface{}{"hello": "world"})
			c.Expect(msg.json(), Equals, "{\"hello\":\"world\"}")
		})

		c.Specify("returns err if couldn't convert object", func() {
			msg, err := NewMsg(func() {})
			c.Expect(msg, IsNil)
			c.Expect(err, Not(IsNil))
		})
	})

	c.Specify("NewMsgFromString", func() {
		c.Specify("returns a new message with string as the content", func() {
			msg, _ := NewMsgFromString("{\"hello\":\"world\"}")
			c.Expect(msg.json(), Equals, "{\"hello\":\"world\"}")
		})

		c.Specify("returns err if couldn't convert string", func() {
			msg, err := NewMsgFromString("not json")
			c.Expect(msg, IsNil)
			c.Expect(err, Not(IsNil))
		})
	})
}
