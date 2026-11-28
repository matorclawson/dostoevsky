package dostoevsky

import (
	"cmp"
	"slices"
	"strings"
)

// Go iterates a map in a random order. Whatever walks a map and writes what it finds takes its keys from here, so that
// equal states are written as equal bytes: partitions by topic, then number; groups by name.

func comparePartitions(a, b Partition) int {
	if c := strings.Compare(a.Topic, b.Topic); c != 0 {
		return c
	}
	return cmp.Compare(a.Number, b.Number)
}

func partitionsInOrder[V any](m map[Partition]V) []Partition {
	keys := make([]Partition, 0, len(m))
	for p := range m {
		keys = append(keys, p)
	}
	slices.SortFunc(keys, comparePartitions)
	return keys
}

func groupsInOrder[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for g := range m {
		keys = append(keys, g)
	}
	slices.Sort(keys)
	return keys
}
