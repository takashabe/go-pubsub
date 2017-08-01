package datastore

import (
	"bytes"
	"encoding/gob"
)

type dummy struct {
	ID string
}

func decodeDummy(e []byte) (*dummy, error) {
	var res *dummy
	buf := bytes.NewReader(e)
	if err := gob.NewDecoder(buf).Decode(&res); err != nil {
		return nil, err
	}
	return res, nil
}
