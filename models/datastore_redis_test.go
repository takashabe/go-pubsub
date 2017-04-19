package models

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"

	"github.com/garyburd/redigo/redis"
)

func dummyRedis(t *testing.T) *Redis {
	r, err := NewRedis(&Config{
		Endpoint: "localhost:6379",
	})
	if err != nil {
		t.Fatalf("failed to connect redis, got err %v", err)
	}
	return r
}

func TestRedisSetAndGet(t *testing.T) {
	type dummyType struct {
		Name string
		Age  int
	}

	cases := []struct {
		key   interface{}
		value interface{}
	}{
		{
			"a",
			dummyType{Name: "test", Age: 1},
		},
	}
	for i, c := range cases {
		// set
		var encode bytes.Buffer
		if err := json.NewEncoder(&encode).Encode(c.value); err != nil {
			t.Fatalf("failed to encode json, got err %v", err)
		}
		client := dummyRedis(t)
		if err := client.Set(c.key, &encode); err != nil {
			t.Fatalf("#%d: failed to set, key=%v, value=%v, got err %v", i, c.key, c.value, err)
		}

		// get
		v, err := redis.Bytes(client.Get(c.key), nil)
		if err != nil {
			t.Fatalf("#%d: failed to get, key=%v, got err %v", i, c.key, err)
		}
		var buf dummyType
		if err := json.NewDecoder(bytes.NewReader(v)).Decode(&buf); err != nil {
			t.Fatalf("failed to decode json, got err %v", err)
		}
		if !reflect.DeepEqual(c.value, buf) {
			t.Errorf("#%d: get value want %v, got %v", i, c.value, buf)
		}
	}
}
