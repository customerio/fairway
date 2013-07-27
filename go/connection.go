package fairway

type Connection interface {
	RegisterQueues()
	Queues() []*Queue
	Channel(*Msg) string
	Deliver(*Msg) error
	Configuration() *Config
}

type conn struct {
	config  *Config
	scripts *scripts
}

func (c *conn) RegisterQueues() {
	for _, definition := range c.config.queues {
		c.scripts.registerQueue(definition)
	}
}

func (c *conn) Queues() []*Queue {
	registered, _ := c.scripts.registeredQueues()
	queues := make([]*Queue, len(registered))

	for i, queue := range registered {
		queues[i] = NewQueue(c, queue)
	}

	return queues
}

func (c *conn) Channel(msg *Msg) string {
	return "default"
}

func (c *conn) Deliver(msg *Msg) error {
	channel := c.Channel(msg)
	facet := c.config.Facet(msg)
	return c.scripts.deliver(channel, facet, msg)
}

func (c *conn) Configuration() *Config {
	return c.config
}

func NewConnection(config *Config) Connection {
	c := &conn{
		config,
		config.scripts(),
	}
	c.RegisterQueues()
	return c
}
