package main

import "testing"

func TestPartitionConversationGroupsAvoidsSingleUserTail(t *testing.T) {
	users := make([]*loadUser, 77)
	for i := range users {
		users[i] = &loadUser{UserID: string(rune('a' + (i % 26)))}
	}

	groups := partitionConversationGroups(users, 4)
	if len(groups) != 19 {
		t.Fatalf("expected 19 groups, got %d", len(groups))
	}
	if len(groups[len(groups)-1]) != 5 {
		t.Fatalf("expected final group size 5, got %d", len(groups[len(groups)-1]))
	}
	for i, group := range groups[:len(groups)-1] {
		if len(group) != 4 {
			t.Fatalf("expected group %d size 4, got %d", i, len(group))
		}
	}
}
