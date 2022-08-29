package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httputil"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/georgi-georgiev/vow/pkg/config"
	"github.com/georgi-georgiev/vow/pkg/helpers"
	"github.com/georgi-georgiev/vow/pkg/models"
	"github.com/georgi-georgiev/vow/pkg/plunder"
	"github.com/jackc/pgx/v4"
	"github.com/jackc/pgx/v4/pgxpool"
)

var (
	kafkaProducer *kafka.Producer
	pgxPool       *pgxpool.Pool
)

func main() {

	ctx, cancelCtx := context.WithCancel(context.Background())

	conf, err := config.NewConfig("config.yml")
	if err != nil {
		panic(err)
	}

	kafkaProducer = helpers.InitKafkaProducer(conf)
	pgxPool = helpers.InitPostgres(conf)

	mux := http.NewServeMux()
	mux.Handle("/order", plunder.AdaptFunc(orderHandler, plunder.Notify))

	server := &http.Server{
		Addr:    ":7373",
		Handler: mux,
	}

	pgxPool.Config().ConnConfig.Logger = &plunder.PlunderPGXLogger{}

	plunder.Init(conf)
	url := "postgres://" + conf.Postgres.Username + ":" + conf.Postgres.Password + "@" + conf.Postgres.Host + "/" + conf.Postgres.Dbname + "?sslmode=disable"
	//go plunder.Cdc(url)
	go plunder.Listen(url)

	go func() {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("order server closed\n")
		} else if err != nil {
			fmt.Printf("error listening for order server: %s\n", err)
		}
		cancelCtx()
	}()

	<-ctx.Done()
}

func orderHandler(w http.ResponseWriter, r *http.Request) {
	fmt.Println("order handler")
	ctx := r.Context()

	if r.Method != http.MethodPost {
		msg := fmt.Sprintf("%s is not supported", r.Method)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	fmt.Println("before decode")

	var order models.Order

	b, _ := httputil.DumpRequest(r, true)
	fmt.Println(string(b))

	err := json.NewDecoder(r.Body).Decode(&order)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	fmt.Println("before insert")

	tx, err := pgxPool.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	defer func() {
		if err != nil {
			tx.Rollback(ctx)
		} else {
			tx.Commit(ctx)
		}
	}()
	ct, err := tx.Exec(ctx, "insert into orders values($1, $2, $3)", order.Product, order.Price, order.Client)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	tx.Query(ctx, "select txid_current()")

	fmt.Println("commandtag", ct.String())

	fmt.Println("after insert")

	orderBody, err := json.Marshal(&order)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	topic := "order"
	kafkaProducer.Produce(&kafka.Message{
		TopicPartition: kafka.TopicPartition{Topic: &topic, Partition: kafka.PartitionAny},
		Value:          orderBody,
	}, nil)

	w.Write([]byte("order created"))
}
