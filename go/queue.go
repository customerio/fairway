package fairway

type Queue struct {
	conn Connection
	name string
}

func NewQueue(conn Connection, name string) *Queue {
	return &Queue{conn, name}
}

func (q *Queue) Pull() (string, *Msg) {
	return q.conn.Configuration().scripts().pull(q.name)
}
