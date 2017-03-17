package fairway

import (
	"github.com/customerio/redigo/redis"

	"fmt"
	"time"
)

type scripts struct {
	config *Config

	deliverScript  *redis.Script
	pullScript     *redis.Script
	inflightScript *redis.Script
	pingScript     *redis.Script
	ackScript      *redis.Script
	priorityScript *redis.Script
}

func newScripts(config *Config) *scripts {
	return &scripts{
		config:         config,
		deliverScript:  redis.NewScript(1, FairwayDeliver()),
		pullScript:     redis.NewScript(4, FairwayPull()),
		inflightScript: redis.NewScript(1, FairwayInflight()),
		pingScript:     redis.NewScript(3, FairwayPing()),
		ackScript:      redis.NewScript(1, FairwayAck()),
		priorityScript: redis.NewScript(1, FairwayPriority()),
	}
}

func (s *scripts) namespace() string {
	namespace := s.config.Namespace

	if len(namespace) > 0 {
		namespace = fmt.Sprint(namespace, ":")
	}

	return namespace
}

func (s *scripts) registeredQueuesKey() string {
	return fmt.Sprint(s.namespace(), "registered_queues")
}

func (s *scripts) registerQueue(queue *QueueDefinition) {
	conn := s.config.Pool.Get()
	defer conn.Close()

	_, err := redis.Bool(conn.Do("hset", s.registeredQueuesKey(), queue.name, queue.channel))

	if err != nil {
		panic(err)
	}
}

func (s *scripts) registeredQueues() ([]string, error) {
	conn := s.config.Pool.Get()
	defer conn.Close()
	return redis.Strings(conn.Do("hkeys", s.registeredQueuesKey()))
}

func (s *scripts) deliver(channel, facet string, msg *Msg) error {
	conn := s.config.Pool.Get()
	defer conn.Close()

	_, err := s.deliverScript.Do(conn, s.namespace(), channel, facet, msg.Original)

	return err
}

func (s *scripts) deliverBytes(channel, facet string, msg []byte) error {
	conn := s.config.Pool.Get()
	defer conn.Close()

	_, err := s.deliverScript.Do(conn, s.namespace(), channel, facet, string(msg))

	return err
}

func (s *scripts) length(queue string) (int, error) {
	conn := s.config.Pool.Get()
	defer conn.Close()
	return redis.Int(conn.Do("get", s.namespace()+queue+":length"))
}

func (s *scripts) pull(queueName string, n, wait int) (string, []*Msg) {
	conn := s.config.Pool.Get()
	defer conn.Close()

	if n <= 0 {
		n = 1
	}

	r, err := s.pullScript.Do(conn, s.namespace(), int(time.Now().Unix()), n, wait, queueName)
	if err != nil || r == nil {
		return "", nil
	}

	result := r.([]interface{})

	queue := string(result[0].([]byte))

	messages := make([]*Msg, 0, len(result[1].([]interface{})))

	for _, m := range result[1].([]interface{}) {
		if m != nil {
			msg, _ := NewMsgFromBytes(m.([]byte))
			messages = append(messages, msg)
		}
	}

	return queue, messages
}

func (s *scripts) inflight(queueName string) []string {
	conn := s.config.Pool.Get()
	defer conn.Close()

	result, err := redis.Strings(s.inflightScript.Do(conn, s.namespace(), queueName))

	if err != nil {
		return []string{}
	}

	return result
}

func (s *scripts) inflightLimit(queue string) (limit int, err error) {
	conn := s.config.Pool.Get()
	defer conn.Close()

	limit, err = redis.Int(conn.Do("get", s.namespace()+queue+":limit"))

	if err != nil && err.Error() == "redigo: nil returned" {
		return 0, nil
	}

	return
}

func (s *scripts) setInflightLimit(queue string, limit int) (err error) {
	conn := s.config.Pool.Get()
	defer conn.Close()

	_, err = conn.Do("set", s.namespace()+queue+":limit", limit)

	return
}

func (s *scripts) ping(queueName string, message *Msg, wait int) error {
	conn := s.config.Pool.Get()
	defer conn.Close()

	_, err := redis.Strings(s.pingScript.Do(conn, s.namespace(), int(time.Now().Unix()),
		wait, queueName, message.Original))

	return err
}

func (s *scripts) ack(queueName string, facet string, message *Msg) error {
	conn := s.config.Pool.Get()
	defer conn.Close()

	_, err := redis.Strings(s.ackScript.Do(conn, s.namespace(), queueName, facet, message.Original))

	return err
}

func (s *scripts) setPriority(queueName string, facet string, priority int) error {
	conn := s.config.Pool.Get()
	defer conn.Close()

	_, err := s.priorityScript.Do(conn, s.namespace(), queueName, facet, priority)

	return err
}
