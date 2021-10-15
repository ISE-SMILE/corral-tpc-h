package main

import (
	"fmt"
	"github.com/spf13/viper"
	"github.com/tawalaya/corral_plus_tpch/queries"
	"os"
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

func testWithMinio(t *testing.T, job queries.QueryType) {
	t.Logf("running TPCH_Q%02d", job)
	//XXX dont forget to use the right enviroment variables ;)
	viper.Set("minioHost", "tpch:9000")
	viper.Set("cleanup", true)
	conf.Query = job
	conf.Endpoint = "minio://tpch"
	Run(conf)

}

var test_queries = [...]queries.QueryType{
	queries.TPCH_Q1,
	queries.TPCH_Q2,
	queries.TPCH_Q6,
	queries.TPCH_Q14,
	queries.TPCH_Q15,
	queries.TPCH_Q17,
	queries.TPCH_Q18,
}

func TestLocal(t *testing.T) {
	for _, q := range test_queries {
		t.Run(fmt.Sprintf("Q%d", q), func(t *testing.T) {
			test(t, q)
		})
	}
}

func TestLocalRemoteMinio(t *testing.T) {
	_, user := os.LookupEnv("MINIO_USER")
	_, key := os.LookupEnv("MINIO_KEY")
	if !user || !key {
		t.Fatal("missing minio credentials in env to run!")
	}

	for _, q := range test_queries {
		t.Run(fmt.Sprintf("Q%d", q), func(t *testing.T) {
			testWithMinio(t, q)
		})
	}
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
