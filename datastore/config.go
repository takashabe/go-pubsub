package datastore

var globalConfig *Config

// SetGlobalConfig set global config
func SetGlobalConfig(cfg *Config) {
	globalConfig = cfg
}

// Config is specific datastore config, written under "datastore"
type Config struct {
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
