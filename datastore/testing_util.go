package datastore

import (
	"bytes"
	"encoding/gob"
)

type Dummy struct {
	ID string
}

func DecodeDummy(e []byte) (*Dummy, error) {
	var res *Dummy
	buf := bytes.NewReader(e)
	if err := gob.NewDecoder(buf).Decode(&res); err != nil {
		return nil, err
	}
	return res, nil
}
