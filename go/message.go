package fairway

import (
	"encoding/json"
	"fmt"
	"github.com/bitly/go-simplejson"
)

type Msg struct {
	*simplejson.Json
}

func NewMsg(body interface{}) *Msg {
	bytes, _ := json.Marshal(body)
	simplej, _ := simplejson.NewJson(bytes)
	return &Msg{simplej}
}

func NewMsgFromString(body string) *Msg {
	simplej, _ := simplejson.NewJson([]byte(body))
	return &Msg{simplej}
}
func (m *Msg) json() string {
	j, err := m.MarshalJSON()
	if err != nil {
		fmt.Println(err)
	}
	return string(j)
}
