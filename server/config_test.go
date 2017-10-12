package server

import (
	"reflect"
	"testing"

	"github.com/takashabe/go-pubsub/datastore"
)

func TestLoadConfig(t *testing.T) {
	cases := []struct {
		inputPath    string
		expectConfig *Config
		expectErr    error
	}{
		{
			"testdata/valid_redis.yaml",
			&Config{
				&datastore.Config{
					Redis: &datastore.RedisConfig{
						Addr: "localhost:6379",
						DB:   0,
					},
					MySQL: nil,
				},
			},
			nil,
		},
		{
			"testdata/unknown_param.yaml",
			&Config{
				&datastore.Config{
					Redis: nil,
					MySQL: &datastore.MySQLConfig{
						Addr: "localhost:3306",
					},
				},
			},
			nil,
		},
		{
			"testdata/empty_param.yaml",
			&Config{&datastore.Config{}},
			nil,
		},
	}
	for i, c := range cases {
		got, err := LoadConfigFromFile(c.inputPath)
		if err != c.expectErr {
			t.Errorf("#%d: want %v, got %v", i, c.expectErr, err)
		}
		if !reflect.DeepEqual(got, c.expectConfig) {
			t.Errorf("#%d: want %#v, got %#v", i, c.expectConfig, got)
		}
	}
}
