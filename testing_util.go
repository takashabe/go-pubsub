package queue

type testDatastore struct{}

func newTestDatastore() *testDatastore {
	return &testDatastore{}
}

func (d *testDatastore) Set(key string, value Message) error {
	return nil
}

func (d *testDatastore) Get(key string) (Message, error) {
	return Message{}, nil
}
