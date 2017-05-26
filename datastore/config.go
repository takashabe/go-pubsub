package datastore

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

var GlobalConfig *Config

// SetGlobalConfig set global config
func SetGlobalConfig(cfg *Config) {
	GlobalConfig = cfg
}

// Config is specific datastore config, written under "datastore"
type Config struct {
	Redis *RedisConfig `yaml:"redis"`
	MySQL *MySQLConfig `yaml:"mysql"`
}

type RedisConfig struct {
	Addr     string `yaml:"addr"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	DB       int    `yaml:"db"`
	Password string `yaml:"password"`
}

type MySQLConfig struct {
	Addr     string `yaml:"addr"`
	Host     string `yaml:"host"`
	Port     int    `yaml:"port"`
	User     string `yaml:"user"`
	Password string `yaml:"password"`
}

// TODO: move to server package
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
