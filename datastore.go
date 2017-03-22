package queue

import (
	"fmt"
	"io/ioutil"
)

type Datastore interface {
	Set(key string, value Message) error
	Get(key string) (Message, error)
}

// Load backend datastore from cnofiguration json file.
func LoadDatastore(path string) (Datastore, error) {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, err
	}

	// TODO: loading json file and create datastore object
	fmt.Println(string(data))

	return nil, nil
}
