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

func dummyMessage(id string) *Message {
	return &Message{
		ID: id,
	}
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
				"z": "foo",
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

func TestGetRange(t *testing.T) {
	cases := []struct {
		baseList   *MessageList
		inputSizes []int
		expectIDs  [][]string
		expectList *MessageList
		expectErr  []error
	}{
		{
			dummyMessageList(
				dummyMessage("a"),
				dummyMessage("b"),
				dummyMessage("c"),
				dummyMessage("d"),
			),
			[]int{2, 1},
			[][]string{
				[]string{"a", "b"},
				[]string{"c"},
			},
			dummyMessageList(
				dummyMessage("d"),
			),
			[]error{nil, nil},
		},
		{
			dummyMessageList(
				dummyMessage("a"),
			),
			[]int{2, 1},
			[][]string{
				[]string{"a"},
				[]string{},
			},
			dummyMessageList(),
			[]error{
				nil,
				ErrEmptyMessage,
			},
		},
	}
	for i, c := range cases {
		// call GetRange many times
		for i2, size := range c.inputSizes {
			got, err := c.baseList.GetRange(size)
			if !isExistMessageID(got, c.expectIDs[i2]) {
				t.Errorf("#%d-%#d: want %v, got %v", i, i2, c.expectIDs[i2], got)
			}
			if err != c.expectErr[i2] {
				t.Errorf("#%d: want %v, got %v", i, c.expectErr, err)
			}
		}
		// check the remaining slice
		if !reflect.DeepEqual(c.baseList, c.expectList) {
			t.Errorf("#%d: want %v, got %v", i, c.expectList, c.baseList)
		}
	}
}

func isExistMessageID(src []*Message, subID []string) bool {
	srcMap := make(map[string]bool)
	for _, m := range src {
		srcMap[m.ID] = true
	}

	for _, id := range subID {
		if _, ok := srcMap[id]; !ok {
			// not found ID
			return false
		}
	}
	return true
}

func TestMessageAppendAndGetRange(t *testing.T) {
	list := &MessageList{
		list: make([]*Message, 0),
	}

	// get over len size
	list.Append(dummyMessage("a"))
	list.Append(dummyMessage("b"))
	got, err := list.GetRange(3)
	if err != nil {
		t.Errorf("want no error, got %v", err)
	}
	want := []string{"a", "b"}
	if !isExistMessageID(got, want) {
		t.Errorf("want %v, got %v", want, got)
	}

	// get after append
	list.Append(dummyMessage("a"))
	list.Append(dummyMessage("c"))
	got, err = list.GetRange(3)
	if err != nil {
		t.Errorf("want no error, got %v", err)
	}
	want = []string{"a", "c"}
	if !isExistMessageID(got, want) {
		t.Errorf("want %v, got %v", want, got)
	}
}
