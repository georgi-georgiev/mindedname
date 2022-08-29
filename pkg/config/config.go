package config

import (
	"github.com/rotisserie/eris"
	"github.com/spf13/viper"
)

type Config struct {
	Kafka     KafkaConfiguration
	Postgres  PostgresConfiguration
	Cassandra CassandraConfiguration
	Mongo     MongoConfiguration
	Apis      ApisConfiguration
	Topic     TopicsConfiguration
	Databases DatabasesConfiguration
	Redis     RedisConfigration
}

type KafkaConfiguration struct {
	Broker string
}

type PostgresConfiguration struct {
	Driver   string
	Username string
	Password string
	Host     string
	Port     string
	Dbname   string
}

type CassandraConfiguration struct {
	Hosts       string
	Port        int
	Keyspace    string
	User        string
	Pass        string
	Consistency string
}

type MongoConfiguration struct {
	Url      string
	Username string
	Password string
	Dbname   string
}

type RedisConfigration struct {
	Host string
	Pass string
}

type ApisConfiguration struct {
}

type TopicsConfiguration struct {
}

type DatabasesConfiguration struct {
}

func NewConfig(path string) (*Config, error) {
	var config *Config

	viper.SetConfigFile(path)
	viper.SetConfigType("yaml")

	if err := viper.ReadInConfig(); err != nil {
		return nil, eris.Wrap(err, "cannot read config file")
	}

	err := viper.Unmarshal(&config)
	if err != nil {
		return nil, eris.Wrap(err, "cannot unmarshal config")
	}

	return config, nil
}
