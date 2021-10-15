package main

import (
	"github.com/spf13/viper"
	"github.com/tawalaya/corral_plus_tpch/queries"
	"testing"
)

var conf = runConfig{
	Backend:    "local",
	Experiment: "1",
	Endpoint:   "test",
	Undeploy:   false,
	Randomize:  false,
	Validation: false,
	Cache:      "redis",
}

func test(t *testing.T, job queries.QueryType) {
	t.Logf("running TPCH_Q%02d", job)

	conf.Query = job
	viper.Set("cleanup", true)
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

//func TestQ17Special(t *testing.T) {
//	conf.Cache = "local"
//	conf.Query = queries.TPCH_Q17
//	viper.Set("cleanup",true)
//	query, options := setup(conf)
//	query.(*queries.Q17).Brand = 41
//	query.(*queries.Q17).Container = "DRUM PACK"
//	err := GenerateRunnableFile(conf, query)
//	if err != nil {
//		t.Fatalf("failed to generate runnable file %+v", err)
//	}
//	//create driver
//	_ = Execute(conf, query, options)
//}
