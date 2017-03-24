package queue

import (
	"reflect"
	"testing"
)

func dummyAcks(ids ...string) *acks {
	a := &acks{
		list: make(map[string]bool),
	}
	for _, id := range ids {
		a.add(id)
	}
	return a
}

func dummyMessageList(ms ...*Message) *MessageList {
	list := &MessageList{
		list: make([]*Message, 0),
	}
	for _, m := range ms {
		list.Append(m)
	}
	return list
}

func TestAck(t *testing.T) {
	cases := []struct {
		baseMessageList *MessageList
		ackIDs          map[string]string // SubscriptionID: MessageID
		expectAcks      *MessageList
	}{
		{
			dummyMessageList(
				&Message{
					ID:   "foo",
					acks: dummyAcks("a", "b", "c"),
				},
				&Message{
					ID:   "bar",
					acks: dummyAcks("a", "b", "c"),
				},
			),
			map[string]string{
				"a": "foo",
				"b": "bar",
			},
			dummyMessageList(
				&Message{
					ID: "foo",
					acks: &acks{
						list: map[string]bool{"a": true, "b": false, "c": false},
					},
				},
				&Message{
					ID: "bar",
					acks: &acks{
						list: map[string]bool{"a": false, "b": true, "c": false},
					},
				},
			),
		},
	}
	for i, c := range cases {
		for subID, messID := range c.ackIDs {
			c.baseMessageList.Ack(subID, messID)
		}
		if !reflect.DeepEqual(c.baseMessageList, c.expectAcks) {
			t.Errorf("#%d: want %v, got %v", i, c.expectAcks, c.baseMessageList)
		}
	}
}
