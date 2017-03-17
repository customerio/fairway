package fairway

import (
	"encoding/json"

	simplejson "github.com/bitly/go-simplejson"
)

// Msg is immutable. Don't change it or you'll regret it.
type Msg struct {
	Original []byte
	*simplejson.Json
}

func NewMsg(body interface{}) (*Msg, error) {
	bytes, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	simplej := simplejson.New()
	simplej.SetPath(nil, body)

	return &Msg{bytes, simplej}, nil
}

func NewMsgFromBytes(body []byte) (*Msg, error) {
	simplej, err := simplejson.NewJson(body)
	if err != nil {
		return nil, err
	}

	return &Msg{body, simplej}, nil
}

func NewMsgFromString(s string) (*Msg, error) {
	return NewMsgFromBytes([]byte(s))
}

// The fairway message is immutable, it mustn't be changed. These functions
// are present for testing purposes.
func (m *Msg) Set(key string, val interface{}) {
	panic("cannot mutate")
}

func (j *Msg) SetPath(branch []string, val interface{}) {
	panic("cannot mutate")
}

func (j *Msg) Del(key string) {
	panic("cannot mutate")
}

func (j *Msg) json() string {
	return string(j.Original)
}
