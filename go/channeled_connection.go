package fairway

type channeledConn struct {
	*conn
	channel func(message *Msg) string
}

func (c *channeledConn) Channel(msg *Msg) string {
	return c.channel(msg)
}

func (c *channeledConn) Deliver(msg *Msg) error {
	channel := c.Channel(msg)
	facet := c.config.Facet(msg)
	return c.scripts.deliver(channel, facet, msg)
}

func NewChanneledConnection(config *Config, channelFunc func(message *Msg) string) Connection {
	return &channeledConn{
		NewConnection(config).(*conn),
		channelFunc,
	}
}
