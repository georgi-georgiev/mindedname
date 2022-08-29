package helpers

import (
	"context"
	"fmt"

	"github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/georgi-georgiev/vow/pkg/config"
	"github.com/georgi-georgiev/vow/pkg/plunder"
	"github.com/go-redis/redis/v8"
	"github.com/gocql/gocql"
	"github.com/jackc/pgx/v4/pgxpool"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func InitCassandra(conf *config.Config) (*gocql.ClusterConfig, *gocql.Session) {
	cluster := gocql.NewCluster(conf.Cassandra.Hosts)
	cluster.Port = conf.Cassandra.Port
	cluster.Keyspace = conf.Cassandra.Keyspace
	cluster.Authenticator = gocql.PasswordAuthenticator{
		Username: conf.Cassandra.User,
		Password: conf.Cassandra.Pass,
	}

	consitency, err := gocql.ParseConsistencyWrapper(conf.Cassandra.Consistency)
	if err != nil {
		cluster.Consistency = gocql.Quorum
	} else {
		cluster.Consistency = consitency
	}

	session, err := cluster.CreateSession()
	if err != nil {
		panic(err)
	}

	return cluster, session
}

func InitKafkaProducer(conf *config.Config) *kafka.Producer {
	p, err := kafka.NewProducer(&kafka.ConfigMap{"bootstrap.servers": conf.Kafka.Broker})
	if err != nil {
		panic(err)
	}
	fmt.Println("Init kafka producer")
	return p
}

func InitKafkaConsumer(conf *config.Config) *kafka.Consumer {
	c, err := kafka.NewConsumer(&kafka.ConfigMap{"bootstrap.servers": conf.Kafka.Broker, "group.id": "emv_tests"})
	if err != nil {
		panic(err)
	}
	return c
}

func InitPostgres(conf *config.Config) *pgxpool.Pool {
	url := "postgres://" + conf.Postgres.Username + ":" + conf.Postgres.Password + "@" + conf.Postgres.Host + "/" + conf.Postgres.Dbname
	config, err := pgxpool.ParseConfig(url)
	if err != nil {
		panic(err)
	}
	config.AfterConnect = plunder.AfterConnect
	config.AfterRelease = plunder.AfterRelease
	config.ConnConfig.Logger = &plunder.PlunderPGXLogger{}
	dbpool, err := pgxpool.ConnectConfig(context.Background(), config)
	if err != nil {
		panic(err)
	}

	return dbpool
}

func InitMongo(conf *config.Config) *mongo.Database {
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

	return client.Database(conf.Mongo.Dbname)
}

func InitRedis(conf *config.Config) *redis.Client {
	return redis.NewClient(&redis.Options{
		Addr:     conf.Redis.Host,
		Password: conf.Redis.Pass,
		DB:       0,
	})
}
