package fairway

import (
	"fmt"
	"github.com/garyburd/redigo/redis"
	"io/ioutil"
)

type scripts struct {
	config *Config
	data   map[string]*redis.Script
}

func newScripts(config *Config) *scripts {
	return &scripts{config, make(map[string]*redis.Script)}
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
	conn := s.config.redisPool.Get()
	defer conn.Close()

	conn.Do("hset", s.registeredQueuesKey(), queue.name, queue.channel)
}

func (s *scripts) registeredQueues() []string {
	conn := s.config.redisPool.Get()
	defer conn.Close()

	result, _ := redis.Strings(conn.Do("hkeys", s.registeredQueuesKey()))

	return result
}

func (s *scripts) deliver(channel, facet string, msg *Msg) {
	conn := s.config.redisPool.Get()
	defer conn.Close()

	script := s.findScript("fairway_deliver", 1)

	script.Do(conn, s.namespace(), channel, facet, msg.json())
}

func (s *scripts) pull(queueName string) (string, *Msg) {
	conn := s.config.redisPool.Get()
	defer conn.Close()

	script := s.findScript("fairway_pull", 1)

	result, err := redis.Strings(script.Do(conn, s.namespace(), queueName))

	if err != nil {
		return "", nil
	}

	queue := result[0]
	message := NewMsgFromString(result[1])

	return queue, message
}

func (s *scripts) findScript(name string, keyCount int) *redis.Script {
	if s.data[name] == nil {
		script, _ := ioutil.ReadFile(fmt.Sprint("../redis/", name, ".lua"))
		s.data[name] = redis.NewScript(keyCount, string(script))
	}

	return s.data[name]
}
