package fairway

type Queue struct {
	conn Connection
	name string
}

func NewQueue(conn Connection, name string) *Queue {
	return &Queue{conn, name}
}

func (q *Queue) ActiveFacets() ([]*Facet, error) {
	var facets []*Facet
	names, err := q.conn.Configuration().scripts().activeFacets(q.name)
	if err != nil {
		return nil, err
	}
	for _, f := range names {
		facets = append(facets, NewFacet(q, f))
	}
	return facets, nil
}

func (q *Queue) Name() string {
	return q.name
}

func (q *Queue) Length() (int, error) {
	return q.conn.Configuration().scripts().length(q.name)
}

func (q *Queue) Pull(resendTimeframe int) (string, *Msg) {
	return q.conn.Configuration().scripts().pull(q.name, resendTimeframe)
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
