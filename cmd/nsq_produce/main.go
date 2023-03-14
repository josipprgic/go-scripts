package main

import (
	"encoding/json"
	"flag"
	"github.com/minus5/svckit/env"
	"github.com/minus5/svckit/log"
	"github.com/minus5/svckit/metric"
	"github.com/minus5/svckit/metric/statsd"
	"github.com/minus5/svckit/nsq"
	"os"
	"sync"
	"time"
)

type InputMessage struct {
	Topic string                 `json:"topic"`
	Data  map[string]interface{} `json:"data"`
}

var topic string

const (
	workingDir = "./input"
)

func init() {
	flag.StringVar(&topic, "topic", "", "nsq topic name")
	flag.Parse()
}

func main() {
	log.Info("starting")
	defer log.Info("stopped")

	statsd.MustDial(statsd.MetricPrefix(env.AppName()))
	defer statsd.Close()
	metric.Counter("service.start")
	defer metric.Counter("service.exit")

	wg := sync.WaitGroup{}
	loop := func() {
		for true {
			var entries []os.DirEntry
			var err error
			if entries, err = os.ReadDir(workingDir); err != nil {
				log.Fatalf("error while reading dir: ./input ", err)
				return
			}

			m := map[string][]map[string]interface{}{}
			for _, entry := range entries {
				bytes, err := os.ReadFile(workingDir + "/" + entry.Name())
				if err != nil {
					log.Errorf("couldn't read file: "+workingDir+"/"+entry.Name(), err)
					continue
				}

				if len(bytes) > 0 && bytes[0] == '[' {
					var in []InputMessage
					json.Unmarshal(bytes, &in)
					if len(in) > 0 {
						for _, one := range in {
							if m[one.Topic] == nil {
								m[one.Topic] = []map[string]interface{}{}
							}

							m[one.Topic] = append(m[one.Topic], one.Data)
						}
					}
				} else {
					in := InputMessage{}
					err = json.Unmarshal(bytes, &in)
					if err != nil || in.Topic == "" || in.Data == nil {
						log.Errorf("input has nil values: topic and data must be specified")
					}
					if m[in.Topic] == nil {
						m[in.Topic] = []map[string]interface{}{}
					}

					m[in.Topic] = append(m[in.Topic], in.Data)
				}

				if err := os.Remove(workingDir + "/" + entry.Name()); err != nil {
					log.Errorf("failed to remove file after producing: %s", entry.Name())
				}
			}

			producers := map[string]*nsq.Producer{}
			for key := range m {
				producer, err := nsq.NewProducer(key)
				if err != nil {
					log.Errorf("couldn't open producer to topic: "+key, err)
					continue
				}

				producers[key] = producer
			}

			for key := range m {
				p := producers[key]
				for _, v := range m[key] {
					msg, err := json.Marshal(v)
					if err != nil {
						log.Errorf("failed to marshal message: ", err)
					}
					err = p.Publish(msg)
					if err != nil {
						log.Errorf("failed to publish message: ", err)
					}
				}
			}

			time.Sleep(2 * time.Second)
		}

		wg.Done()
	}

	wg.Add(1)
	go loop()
	wg.Wait()
}
