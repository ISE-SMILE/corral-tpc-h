package main

import (
	"github.com/ISE-SMILE/corral"
	"github.com/spf13/viper"
	"github.com/tawalaya/corral_plus_tpch/queries"
	"log"
	"os"
	"path/filepath"
)

func init(){

}

func main(){


	//TODO:

	var query queries.Query

	//TODO: make selectable
	query = &queries.Q1{}

	//TODO: make tuneable
	query.SetExperiment("1")
	query.SetEndpoint("test")

	viper.Set("logName",query.Name())

	options := query.Configure()

	//TODO: Toggle Cache Backend
	options = append(options,
		corral.WithRedisBackedCache(),
		corral.WithInputs(query.Inputs()...),
	)

	//create driver
	driver := corral.NewMultiStageDriver(query.Create(), options...)

	if err := query.Check(driver); err != nil {
		log.Fatalf("query %s check failed", query.Name())
	}

	driver.Main()

	//catch activationslog and move ...
	var results = driver.GetFinalOutputs()
	temp, err := os.MkdirTemp("", "results")
	if err != nil {
		log.Fatalf("query %s failed to download results %+v",query.Name(),err)
	}
	err = driver.DownloadAndRemove(results, temp)
	if err != nil {
		log.Fatalf("query %s failed to download results %+v",query.Name(),err)
	}
	log.Printf("downloaded final resuts at %s",temp)


	files,err := filepath.Glob(filepath.Join(temp,"*"))
	if err != nil {
		log.Fatalf("query %s failed find downloaded files %+v",query.Name(),err)
	}
	//download results and validate ...
	success, err := query.Validate(files)
	if err != nil {
		log.Fatalf("query %s result is invald %+v",query.Name(),err)
	}
	if !success {
		log.Fatalf("query %s result did not match expectations",query.Name())
	}




}
