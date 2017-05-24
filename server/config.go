package server

import (
	"io/ioutil"

	yaml "gopkg.in/yaml.v2"
)

type Cofig struct {
	Datastore *datastore.config `yaml:"datastore"`
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
