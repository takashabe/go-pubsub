package models

import (
	"database/sql"
	"reflect"
	"testing"

	"github.com/pkg/errors"
	"github.com/takashabe/go-fixture"
	_ "github.com/takashabe/go-fixture/mysql"
)

func dummyMySQL(t *testing.T) *MySQL {
	c, err := NewMySQL(&Config{&DatastoreConfig{
		MySQL: &MySQLConfig{
			User:     "mq",
			Password: "",
			Host:     "localhost",
			Port:     3306,
		}},
	})
	if err != nil {
		t.Fatalf("failed to connect mysql, got err %v", err)
	}
	return c
}

func TestMySQLSetAndGet(t *testing.T) {
	cases := []struct {
		key   interface{}
		value interface{}
	}{
		{
			"a",
			&Message{ID: "a"},
		},
	}
	for i, c := range cases {
		// set
		encode, err := EncodeGob(c.value)
		if err != nil {
			t.Fatalf("#%d: failed to encode data, got err %v", i, err)
		}
		client := dummyMySQL(t)
		clearTable(t, client.conn)
		if err := client.Set(c.key, encode); err != nil {
			t.Fatalf("#%d: failed to set, key=%v, value=%v, got err %v", i, c.key, c.value, err)
		}

		// get
		raw, err := client.Get(c.key)
		if err != nil {
			t.Fatalf("#%d: failed to get, key=%v, got err %v", i, c.key, err)
		}
		data, ok := raw.([]byte)
		if !ok {
			t.Fatalf("#%d: failed to convert []byte, got err %v", i, err)
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

func TestMySQLDelete(t *testing.T) {
	cases := []struct {
		key       interface{}
		value     interface{}
		expectErr error
	}{
		{
			"a",
			&Message{ID: "a"},
			sql.ErrNoRows,
		},
	}
	for i, c := range cases {
		// set
		encode, err := EncodeGob(c.value)
		if err != nil {
			t.Fatalf("#%d: failed to encode data, got err %v", i, err)
		}
		client := dummyMySQL(t)
		clearTable(t, client.conn)
		if err := client.Set(c.key, encode); err != nil {
			t.Fatalf("#%d: failed to set, key=%v, value=%v, got err %v", i, c.key, c.value, err)
		}

		// delete
		if err := client.Delete(c.key); err != nil {
			t.Fatalf("#%d: failed to delete entry, got err %v", i, err)
		}

		// get
		_, err = client.Get(c.key)
		if errors.Cause(err) != c.expectErr {
			t.Errorf("#%d: ", i, c.key, err)
		}
	}
}

func TestMySQLDump(t *testing.T) {
	type kv struct {
		id    interface{}
		value interface{}
	}
	cases := []struct {
		inputEntries []kv
		expect       map[interface{}]interface{}
	}{
		{
			[]kv{
				{id: "a", value: &Message{ID: "a"}},
				{id: "b", value: &Message{ID: "b"}},
			},
			map[interface{}]interface{}{
				"a": &Message{ID: "a"},
				"b": &Message{ID: "b"},
			},
		},
	}
	for i, c := range cases {
		client := dummyMySQL(t)
		clearTable(t, client.conn)

		// set
		for _, e := range c.inputEntries {
			encode, err := EncodeGob(e.value)
			if err != nil {
				t.Fatalf("#%d: failed to encode data, got err %v", i, err)
			}
			if err := client.Set(e.id, encode); err != nil {
				t.Fatalf("#%d: failed to set, key=%v, value=%v, got err %v", i, e.id, e.value, err)
			}
		}

		// dump
		dump, err := client.Dump()
		if err != nil {
			t.Fatalf("#%d: failed to dump, goo err %v", i, err)
		}

		replaces := make(map[interface{}]interface{}, len(c.inputEntries))
		for k, v := range dump {
			m, err := DecodeGobMessage(v.([]byte))
			if err != nil {
				t.Fatalf("#%d: failed to decode data, got err %v", i, err)
			}
			replaces[k] = m
		}
		if !reflect.DeepEqual(c.expect, replaces) {
			t.Errorf("#%d: get value want %v, got %v", i, c.expect, replaces)
		}
	}
}
