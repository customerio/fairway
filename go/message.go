package fairway

import (
	"encoding/json"
	"github.com/bitly/go-simplejson"
)

type Msg struct {
	*simplejson.Json
}

func NewMsg(body interface{}) (*Msg, error) {
	bytes, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}

	simplej, err := simplejson.NewJson(bytes)
	if err != nil {
		return nil, err
	}

	return &Msg{simplej}, nil
}

func NewMsgFromString(body string) (*Msg, error) {
	simplej, err := simplejson.NewJson([]byte(body))
	if err != nil {
		return nil, err
	}

	return &Msg{simplej}, nil
}

func (m *Msg) json() string {
	j, _ := m.MarshalJSON()
	return string(j)
}
