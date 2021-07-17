package queries

import "strings"

func inputTables(q QueryExperiment,tables ...string) []string{
	inputs := make([]string,0)

	inputs = append(inputs,strings.Join([]string{q.GetPrefix(),"lineitem","*"},"/"))

	return inputs
}
