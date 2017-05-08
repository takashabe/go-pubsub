package models

import (
	"reflect"
	"testing"

	"github.com/takashabe/go-fixture"
	_ "github.com/takashabe/go-fixture/mysql"
)

func dummyMySQL(t *testing.T) *MySQL {
	c, err := NewMySQL(&Config{&DatastoreConfig{
		MySQL: &MySQLConfig{
			User:     "root",
			Password: "",
			Host:     "localhost",
			Port:     3306,
		}},
	})
	if err != nil {
		t.Fatalf("failed to connect mysql, got err %v", err)
	}

	f := fixture.NewFixture(c.conn, "mysql")
	if err := f.LoadSQL("testdata/empty_table.sql"); err != nil {
		t.Fatalf("failed to execute fixture, got err %v", err)
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
