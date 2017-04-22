package models

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

// TODO: initialize config from Server
var globalConfig *Config

// SetGlobalConfig set global config
func SetGlobalConfig(cfg *Config) {
	globalConfig = cfg
}

// Config is connect to datastore parameters
type Config struct {
	Datastore *DatastoreConfig `yaml:"datastore"`
}

type DatastoreConfig struct {
	Redis *RedisConfig `yaml:"redis"`
	MySQL *MySQLConfig `yaml:"mysql"`
}

type RedisConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	DB       int    `yaml:"db"`
	Password string `yaml:"password"`
}

type MySQLConfig struct {
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

// LoadConfigFromFile read config file and create config object
func LoadConfigFromFile(path string) (*Config, error) {
	d, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var config Config
	if err := yaml.Unmarshal(d, &config); err != nil {
		return nil, err
	}
	return &config, nil
}
