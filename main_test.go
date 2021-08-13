package main

import (
	"github.com/tawalaya/corral_plus_tpch/queries"
	"testing"
)

func test(t *testing.T, job queries.QueryType) {
	t.Logf("running TPCH_Q%02d", job)

	conf := runConfig{
		Query:      job,
		Backend:    "local",
		Experiment: "1",
		Endpoint:   "test",
		Undeploy:   false,
		Randomize:  false,
		Validation: false,
		Cache:      "local",
	}

	Run(conf)

	conf.Randomize = true
	Run(conf)

}

func TestQ1(t *testing.T) {
	test(t, queries.TPCH_Q1)
}

func TestQ2(t *testing.T) {
	test(t, queries.TPCH_Q2)
}

func TestQ6(t *testing.T) {
	test(t, queries.TPCH_Q6)
}

func TestQ14(t *testing.T) {
	test(t, queries.TPCH_Q14)
}

func TestQ15(t *testing.T) {
	test(t, queries.TPCH_Q15)
}

func TestQ17(t *testing.T) {
	test(t, queries.TPCH_Q17)
}

func TestQ18(t *testing.T) {
	test(t, queries.TPCH_Q18)
}
