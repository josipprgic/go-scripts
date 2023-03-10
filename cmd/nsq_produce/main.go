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
	"strings"
	"sync"
)

type input_message struct {
	topic string
	data  map[string]interface{}
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
		wg.Add(1)
		for true {
			var entries []os.DirEntry
			var err error
			if entries, err = os.ReadDir(workingDir); err != nil {
				log.Fatalf("error while reading dir: ./input ", err)
				return
			}

			var m map[string][]map[string]interface{}
			for _, entry := range entries {
				if strings.HasSuffix(entry.Name(), ".json") {
					s := strings.TrimSuffix(entry.Name(), ".json")
					topic = s
				}

				bytes, err := os.ReadFile(workingDir + "/" + entry.Name())
				if err != nil {
					log.Errorf("couldn't read file: "+workingDir+"/"+entry.Name(), err)
					continue
				}

				if len(bytes) > 0 && bytes[0] == '[' {
					var in []input_message
					json.Unmarshal(bytes, &in)
					if len(in) > 0 {
						for _, one := range in {
							if m[one.topic] == nil {
								m[one.topic] = []map[string]interface{}{}
							}

							m[one.topic] = append(m[one.topic], one.data)
						}
					}
				} else {
					var in input_message
					json.Unmarshal(bytes, &in)
					if m[in.topic] == nil {
						m[in.topic] = []map[string]interface{}{}
					}

					m[in.topic] = append(m[in.topic], in.data)
				}
			}

			var producers map[string]*nsq.Producer
			for key := range m {
				producer, err := nsq.NewProducer(topic)
				if err != nil {
					log.Errorf("couldn't open producer to topic: "+topic, err)
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
					p.Publish(msg)
				}
			}
		}
		wg.Done()
	}

	go loop()
	wg.Wait()
}
