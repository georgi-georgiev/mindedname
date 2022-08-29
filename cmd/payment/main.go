package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"net/http"
	"time"

	"github.com/georgi-georgiev/vow/pkg/config"
	"github.com/georgi-georgiev/vow/pkg/helpers"
	"github.com/georgi-georgiev/vow/pkg/models"
	"github.com/go-redis/redis/v8"
)

var (
	rdb *redis.Client
)

func main() {
	ctx, cancelCtx := context.WithCancel(context.Background())

	conf, err := config.NewConfig("config.yml")
	if err != nil {
		panic(err)
	}

	rdb = helpers.InitRedis(conf)

	mux := http.NewServeMux()
	mux.HandleFunc("/payment", paymentHandler)

	server := &http.Server{
		Addr:    ":7575",
		Handler: mux,
		BaseContext: func(l net.Listener) context.Context {
			ctx = context.WithValue(ctx, "paymentServer", l.Addr().String())
			return ctx
		},
	}

	go func() {
		err := server.ListenAndServe()
		if errors.Is(err, http.ErrServerClosed) {
			fmt.Printf("payment server closed\n")
		} else if err != nil {
			fmt.Printf("error listening for payment server: %s\n", err)
		}
		cancelCtx()
	}()

	<-ctx.Done()
}

func paymentHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()

	if r.Method != http.MethodPost {
		msg := fmt.Sprintf("%s is not supported", r.Method)
		http.Error(w, msg, http.StatusBadRequest)
		return
	}

	var payment models.Payment

	err := json.NewDecoder(r.Body).Decode(&payment)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	rdb.Set(ctx, "payment", payment.Amount, 1*time.Hour)
}
