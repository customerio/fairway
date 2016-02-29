package fairway

type Queue struct {
	conn Connection
	name string
}

func NewQueue(conn Connection, name string) *Queue {
	return &Queue{conn, name}
}

func (q *Queue) Name() string {
	return q.name
}

func (q *Queue) Length() (int, error) {
	return q.conn.Configuration().scripts().length(q.name)
}

func (q *Queue) Pull(resendTimeframe int) (string, *Msg) {
	name, msgs := q.conn.Configuration().scripts().pull(q.name, 1, resendTimeframe)

	var m *Msg

	if len(msgs) > 0 {
		m = msgs[0]
	}

	return name, m
}

func (q *Queue) PullN(n, resendTimeframe int) (string, []*Msg) {
	return q.conn.Configuration().scripts().pull(q.name, n, resendTimeframe)
}

func (q *Queue) Inflight() []string {
	return q.conn.Configuration().scripts().inflight(q.name)
}

func (q *Queue) InflightLimit() (int, error) {
	return q.conn.Configuration().scripts().inflightLimit(q.name)
}

func (q *Queue) SetInflightLimit(limit int) error {
	return q.conn.Configuration().scripts().setInflightLimit(q.name, limit)
}

func (q *Queue) Ping(message *Msg, resendTimeframe int) error {
	return q.conn.Configuration().scripts().ping(q.name, message, resendTimeframe)
}

func (q *Queue) Ack(message *Msg) error {
	facet := q.conn.Configuration().Facet(message)
	return q.conn.Configuration().scripts().ack(q.name, facet, message)
}
