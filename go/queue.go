package fairway

type Queue struct {
	conn Connection
	name string
}

func NewQueue(conn Connection, name string) *Queue {
	return &Queue{conn, name}
}

func (q *Queue) Pull(resendTimeframe int) (string, *Msg) {
	return q.conn.Configuration().scripts().pull(q.name, resendTimeframe)
}

func (q *Queue) Inflight() []string {
	return q.conn.Configuration().scripts().inflight(q.name)
}

func (q *Queue) Ack(message *Msg) error {
	return q.conn.Configuration().scripts().ack(q.name, message)
}
