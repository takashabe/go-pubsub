package datastore

// TODO: abort global variables
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
