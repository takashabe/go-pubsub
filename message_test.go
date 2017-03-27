package queue

import (
	"reflect"
	"testing"
)

func TestModifyState(t *testing.T) {
	// Warning: not cleanup topic and subscriptions each testcase
	// States flow: wait > deliver > ack
	helper.setupGlobalAndSetTopics(t, "topA")
	TopA, err := GetTopic("topA")
	if err != nil {
		t.Fatalf(err.Error())
	}
	subA, _ := NewSubscription("subA", "topA", 100, "localhost", nil)
	subB, _ := NewSubscription("subB", "topA", 100, "localhost", nil)
	TopA.AddSubscription(*subA)
	TopA.AddSubscription(*subB)
	baseMsg := NewMessage("foo", *TopA, []byte("test"), nil, TopA.subscriptions)

	cases := []struct {
		inputSubID   []string
		inputFn      func(msg *Message, id string)
		expectStates map[string]messageState
	}{
		{
			[]string{"subA"},
			func(msg *Message, id string) { msg.Deliver(id) },
			map[string]messageState{
				"subA": stateDeliver,
				"subB": stateWait,
			},
		},
		{
			[]string{"subA", "subB"},
			func(msg *Message, id string) { msg.Ack(id) },
			map[string]messageState{
				"subA": stateAck,
				"subB": stateWait,
			},
		},
	}
	for i, c := range cases {
		for _, id := range c.inputSubID {
			c.inputFn(baseMsg, id)
		}
		got := baseMsg.States.list
		if !reflect.DeepEqual(got, c.expectStates) {
			t.Errorf("#%d: want %v, got %v", i, c.expectStates, got)
		}
	}
}

func _TestGetRange(t *testing.T) {
	cases := []struct {
		baseList   *MessageList
		inputSizes []int
		expectIDs  [][]string
		expectList *MessageList
		expectErr  []error
	}{
		{
			helper.dummyMessageList(t,
				helper.dummyMessage(t, "a"),
				helper.dummyMessage(t, "b"),
				helper.dummyMessage(t, "c"),
				helper.dummyMessage(t, "d"),
			),
			[]int{2, 1},
			[][]string{
				[]string{"a", "b"},
				[]string{"c"},
			},
			helper.dummyMessageList(t,
				helper.dummyMessage(t, "d"),
			),
			[]error{nil, nil},
		},
		{
			helper.dummyMessageList(t,
				helper.dummyMessage(t, "a"),
			),
			[]int{2, 1},
			[][]string{
				[]string{"a"},
				[]string{},
			},
			helper.dummyMessageList(t),
			[]error{
				nil,
				ErrEmptyMessage,
			},
		},
	}
	for i, c := range cases {
		// call GetRange many times
		// TODO: impl
		// for i2, size := range c.inputSizes {
		//   got, err := c.baseList.GetRange("", size)
		//   if !isExistMessageID(got, c.expectIDs[i2]) {
		//     t.Errorf("#%d-%#d: want %v, got %v", i, i2, c.expectIDs[i2], got)
		//   }
		//   if err != c.expectErr[i2] {
		//     t.Errorf("#%d: want %v, got %v", i, c.expectErr, err)
		//   }
		// }
		// check the remaining slice
		if !reflect.DeepEqual(c.baseList, c.expectList) {
			t.Errorf("#%d: want %v, got %v", i, c.expectList, c.baseList)
		}
	}
}

// TODO: impl
func _TestMessageAppendAndGetRange(t *testing.T) {
	// list := &MessageList{
	//   list: make([]*Message, 0),
	// }
	//
	// // get over len size
	// list.Append(helper.dummyMessage(t, "a"))
	// list.Append(helper.dummyMessage(t, "b"))
	// got, err := list.GetRange("", 3)
	// if err != nil {
	//   t.Errorf("want no error, got %v", err)
	// }
	// want := []string{"a", "b"}
	// if !isExistMessageID(got, want) {
	//   t.Errorf("want %v, got %v", want, got)
	// }
	//
	// // get after append
	// list.Append(helper.dummyMessage(t, "a"))
	// list.Append(helper.dummyMessage(t, "c"))
	// got, err = list.GetRange("", 3)
	// if err != nil {
	//   t.Errorf("want no error, got %v", err)
	// }
	// want = []string{"a", "c"}
	// if !isExistMessageID(got, want) {
	//   t.Errorf("want %v, got %v", want, got)
	// }
}
