package models

import (
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

func _TestRedisSetAndGet(t *testing.T) {
	cases := []struct {
		key   interface{}
		value interface{}
	}{
		{
			"a",
			Message{ID: "a"},
		},
	}
	for i, c := range cases {
		// set
		encode, err := EncodeGob(c.value)
		if err != nil {
			t.Fatalf("#%d: failed to encode data, got err %v", i, err)
		}
		client := dummyRedis(t)
		if err := client.Set(c.key, encode); err != nil {
			t.Fatalf("#%d: failed to set, key=%v, value=%v, got err %v", i, c.key, c.value, err)
		}

		// get
		data, err := redis.Bytes(client.Get(c.key), nil)
		if err != nil {
			t.Fatalf("#%d: failed to get, key=%v, got err %v", i, c.key, err)
		}
		m, err := DecodeGobMessage(data)
		if err != nil {
			t.Fatalf("#%d: failed to decode data, got err %v", i, err)
		}
		if !reflect.DeepEqual(c.value, m) {
			t.Errorf("#%d: get value want %v, got %v", i, c.value, m)
		}
	}
}
