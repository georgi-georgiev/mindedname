package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"sync"
	"time"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/georgi-georgiev/vow/pkg/config"
	"github.com/georgi-georgiev/vow/pkg/helpers"
	"github.com/georgi-georgiev/vow/pkg/models"
	"github.com/georgi-georgiev/vow/pkg/syncutil"
	"github.com/gocql/gocql"
)

var (
	kafkaConsumer *kafka.Consumer
	cluster       *gocql.ClusterConfig
	session       *gocql.Session
)

func main() {
	ctx, cancelCtx := context.WithCancel(context.Background())

	conf, err := config.NewConfig("config.yml")
	if err != nil {
		panic(err)
	}

	kafkaConsumer = helpers.InitKafkaConsumer(conf)
	cluster, session = helpers.InitCassandra(conf)

	mux := http.NewServeMux()
	mux.HandleFunc("/hello", getHelloTwo)

	server := &http.Server{
		Addr:    ":7474",
		Handler: mux,
		BaseContext: func(l net.Listener) context.Context {
			ctx = context.WithValue(ctx, "customerServer", l.Addr().String())
			return ctx
		},
	}

	go func() {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("customer server closed\n")
		} else if err != nil {
			fmt.Printf("error listening for customer server: %s\n", err)
		}
		cancelCtx()
	}()

	var wgShutdown sync.WaitGroup
	wgShutdown.Add(1)

	err = kafkaConsumer.Subscribe("order", nil)
	if err != nil {
		panic(err)
	}

	go consumeOrders(100, &wgShutdown)

	<-ctx.Done()
}

func consumeOrders(timeout int, wgShutdown *sync.WaitGroup) {
	defer wgShutdown.Done()
	for {
		if syncutil.SignaledServiceShutdown() {
			kafkaConsumer.Close()
			return
		}

		ev := kafkaConsumer.Poll(timeout)
		if ev == nil {
			continue
		}

		switch e := ev.(type) {
		case *kafka.Message:
			var err error
			paused := false
			for {
				err = consumerOrder(e)
				if err != nil {
					fmt.Println(err.Error())
				}

				if err != nil {
					if !paused {
						if err := kafkaConsumer.Pause([]kafka.TopicPartition{e.TopicPartition}); err != nil {
							kafkaConsumer.Close()
							panic(err)
						}
						paused = true
					}

					time.Sleep(time.Millisecond * 1000)

					if syncutil.SignaledServiceShutdown() {
						kafkaConsumer.Close()
						return
					}
					// dummy poll to keep the consumer alive
					if ev := kafkaConsumer.Poll(timeout); ev != nil {
						switch e := ev.(type) {
						case kafka.Error:
							if e.IsFatal() {
								panic(e)
							}
						}
					}
				} else {
					if paused {
						if err := kafkaConsumer.Resume([]kafka.TopicPartition{e.TopicPartition}); err != nil {
							kafkaConsumer.Close()
							panic(err)
						}
					}
					break
				}
			}
			if _, err := kafkaConsumer.CommitMessage(e); err != nil {
				panic(err)
			}
		case kafka.Error:
			if e.IsFatal() {
				panic(e)
			}
		}
	}
}

func consumerOrder(msg *kafka.Message) error {
	var order models.Order
	err := json.Unmarshal(msg.Value, &order)
	if err != nil {
		return err
	}

	customer := models.Customer{
		Name: order.Client,
	}

	err = session.Query("INSERT INTO test.customers (name) VALUES (?);", customer.Name).Exec()
	if err != nil {
		return err
	}

	requestURL := fmt.Sprintf("http://payment:7575/payment")

	client := http.Client{}

	payment := models.Payment{
		Amount: int(order.Price * 100),
	}

	paymentBytes, err := json.Marshal(&payment)
	if err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(context.TODO(), http.MethodPost, requestURL, bytes.NewBuffer(paymentBytes))
	if err != nil {
		return err
	}
	resp, err := client.Do(req)
	if err != nil {
		return err
	}

	defer resp.Body.Close()

	return nil
}

func getHelloTwo(w http.ResponseWriter, r *http.Request) {
}
