package plunder

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"net/http"
	"os"
	"time"

	"github.com/georgi-georgiev/vow/pkg/config"
	"github.com/lib/pq"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

var (
	mongoDB *mongo.Database
)

type Adapter func(http.Handler) http.HandlerFunc

func AdaptHandler(h http.Handler, adapters ...Adapter) http.Handler {
	for _, adapter := range adapters {
		h = adapter(h)
	}
	return h
}

func AdaptFunc(hf http.HandlerFunc, adapters ...Adapter) http.HandlerFunc {
	h := http.HandlerFunc(hf)
	for _, adapter := range adapters {
		h = http.HandlerFunc(adapter(h))
	}
	return h
}

func Init(conf *config.Config) {
	auth := options.Client().SetAuth(options.Credential{
		Username: conf.Mongo.Username,
		Password: conf.Mongo.Password,
	})
	url := options.Client().ApplyURI(conf.Mongo.Url)
	client, err := mongo.Connect(context.Background(), auth, url)
	if err != nil {
		panic(err)
	}

	err = client.Ping(context.Background(), nil)
	if err != nil {
		panic(err)
	}

	mongoDB = client.Database(conf.Mongo.Dbname)
}

type Entity struct {
	Key    string
	Server string
	Data   string
	Type   string
}

func Notify(next http.Handler) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Println("Notify middleware")
		ctx := r.Context()

		key := r.Header.Get("plunder")

		ctx = context.WithValue(ctx, "plunder", key)

		r = r.WithContext(ctx)

		requestBytes, err := ioutil.ReadAll(r.Body)
		if err != nil {
			panic(err)
		}
		r.Body.Close()

		r.Body = ioutil.NopCloser(bytes.NewReader(requestBytes))

		data := string(requestBytes)

		err = InsertEntity(key, data, "request")
		if err != nil {
			fmt.Println("Error inserting entity", err)
		}

		next.ServeHTTP(w, r)
	}
}

func InsertEntity(key string, data string, entityType string) error {
	coll := mongoDB.Collection("plunder")

	name, err := os.Hostname()
	if err != nil {
		return err
	}

	entity := Entity{
		Key:    key,
		Server: name,
		Data:   data,
		Type:   entityType,
	}

	_, err = coll.InsertOne(context.Background(), &entity)
	if err != nil {
		return err
	}

	return nil
}

func Listen(conninfo string) {
	_, err := sql.Open("postgres", conninfo)
	if err != nil {
		panic(err)
	}

	reportProblem := func(ev pq.ListenerEventType, err error) {
		if err != nil {
			fmt.Println(err.Error())
		}
	}

	listener := pq.NewListener(conninfo, 10*time.Second, time.Minute, reportProblem)
	err = listener.Listen("events")
	if err != nil {
		panic(err)
	}

	fmt.Println("Start monitoring PostgreSQL...")
	for {
		waitForNotification(listener)
	}
}

func waitForNotification(l *pq.Listener) {
	for {
		select {
		case n := <-l.Notify:
			fmt.Println("Received data from channel [", n.Channel, "] :")
			// Prepare notification payload for pretty print
			var prettyJSON bytes.Buffer
			err := json.Indent(&prettyJSON, []byte(n.Extra), "", "\t")
			if err != nil {
				fmt.Println("Error processing JSON: ", err)
				return
			}

			data := string(prettyJSON.Bytes())

			err = InsertEntity("no key", data, "db")
			if err != nil {
				fmt.Println("Error inserting entity: ", err)
				return
			}

			fmt.Println("entity inserted")
			return
		case <-time.After(90 * time.Second):
			fmt.Println("Received no events for 90 seconds, checking connection")
			go func() {
				l.Ping()
			}()
			return
		}
	}
}
