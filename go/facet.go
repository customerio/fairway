package fairway

type Facet struct {
	queue *Queue
	name  string
}

func NewFacet(queue *Queue, name string) *Facet {
	return &Facet{queue, name}
}

func (f *Facet) Name() string {
	return f.name
}

func (f *Facet) Length() (int, error) {
	return f.queue.conn.Configuration().scripts().facetLength(f.queue.name, f.name)
}
