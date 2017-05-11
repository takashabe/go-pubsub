package models

import (
	"reflect"
	"testing"
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
				&DatastoreConfig{
					Redis: &RedisConfig{
						Host: "localhost",
						Port: 6379,
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
				&DatastoreConfig{
					Redis: nil,
					MySQL: &MySQLConfig{
						Host: "localhost",
						Port: 3306,
					},
				},
			},
			nil,
		},
		{
			"testdata/empty_param.yaml",
			&Config{&DatastoreConfig{}},
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
