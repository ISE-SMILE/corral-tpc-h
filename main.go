package main

import (
	"encoding/json"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"text/template"
	"time"

	"github.com/ISE-SMILE/corral"
	log "github.com/sirupsen/logrus"
	"github.com/spf13/viper"
	"github.com/tawalaya/corral_plus_tpch/queries"

	"github.com/go-git/go-git/v5"
)

var (
	build string = "Debug"
	seed  int64
)

func init() {
	seed = time.Now().UnixNano()
	rand.Seed(seed)
	log.Infof("using seed %x", seed)
}

type runConfig struct {
	Query      queries.QueryType `json:"query,omitempty"`
	Backend    string            `json:"backend,omitempty"`
	Experiment string            `json:"experiment,omitempty"`
	Endpoint   string            `json:"endpoint,omitempty"`

	Undeploy   bool   `json:"undeploy,omitempty"`
	Randomize  bool   `json:"randomize,omitempty"`
	Validation bool   `json:"validation,omitempty"`
	Debug      bool   `json:"debug,omitempty"`
	Cache      string `json:"cache,omitempty"`

	CorralConfig map[string]interface{} `json:"-"`
}

var runConfigKeys = [...]string{"query", "backend", "experiment", "endpoint", "undeploy", "randomize", "validation", "cache", "debug"}

func (c runConfig) ShortName() string {
	if c.Cache == "" {
		return fmt.Sprintf("%.2s_%.2X_%X_%.8s", c.Backend, "local", seed, build)
	} else {
		return fmt.Sprintf("%.2s_%.2X_%X_%.8s", c.Backend, c.Cache, seed, build)
	}
}

func (c runConfig) isLocal() bool {
	return c.Backend == "local" || c.Backend == ""
}

func (c runConfig) SetupCache(options []corral.Option) []corral.Option {

	switch c.Backend {
	case "lambda":
		viper.Set("redisDeploymentType", 2)
	case "whisk":
		viper.Set("redisDeploymentType", 1)
	case "local":
		fallthrough
	default:
		viper.Set("redisDeploymentType", 0)

	}

	switch c.Cache {
	case "local":
		return append(options, corral.WithLocalMemoryCache())
	case "redis":
		return append(options, corral.WithRedisBackedCache())

	}
	return options
}

//loadConfig tries to a.) find the config file and loads it
func loadConfig() runConfig {

	var conf runConfig = runConfig{
		Query:      queries.TPCH_Q2,
		Backend:    "",
		Experiment: "1",
		Endpoint:   "test",
		Undeploy:   false,
		Randomize:  false,
		Validation: false,
		Cache:      "",
	}

	//hack to check args without
	arguments := os.Args[1:]
	var confFile *string = nil
	for i := 0; i < len(arguments); i++ {
		key := strings.TrimSpace(strings.ToLower(arguments[i]))
		if key == "-config" {
			if i+1 < len(arguments) {
				confFile = &arguments[i+1]
				//hack to remove the config flag
				os.Args = append(arguments[:i], arguments[i+2:]...)

				break
			}
		}
	}

	if confFile != nil {
		f, err := os.Open(*confFile)
		if err != nil {
			log.Warn("could not open config file, using default")
			return conf
		}
		data, err := io.ReadAll(f)
		if err != nil {
			log.Warn("could not read config file, using default")
			return conf
		}
		err = json.Unmarshal(data, &conf)
		if err != nil {
			log.Warn("could not parse config file, using default")
			return conf
		}
		err = json.Unmarshal(data, &conf)
		if err != nil {
			log.Warn("could not parse config file, using default")
			return conf
		}
		//campture other config data to paas to corral directly
		err = json.Unmarshal(data, &conf.CorralConfig)
		if err != nil {
			log.Warn("could not parse config file, using default")
			return conf
		}
	} else {
		log.Warn("no config defined, using default")
	}

	//remove all keys that are part of the core conf...
	for _, k := range runConfigKeys {
		delete(conf.CorralConfig, k)
	}

	return conf
}

func EnsureCleanBuild() error {
	repo, err := git.PlainOpen(".")
	if err != nil {
		return err
	}

	tree, err := repo.Worktree()
	if err != nil {
		return err
	}

	status, err := tree.Status()
	if err != nil {
		return err
	}

	if !status.IsClean() {
		return fmt.Errorf("unclean version can't ensure repoducability")
	}

	ref, err := repo.Head()
	if err != nil {
		return err
	}

	build = ref.Hash().String()
	return nil
}

func main() {
	if corral.RunningOnCloudPlatfrom() {
		RunOnCloud()
	} else {
		Run(loadConfig())
	}
}

//RunOnCloud bypasses some of the local setup for quicker exectuion on the provider side
func RunOnCloud() {
	if config == nil || parameter == nil {
		panic("expected config to be set!")
	}

	query, options := setup(*config)

	err := query.Read(parameter)
	if err != nil {
		panic(err)
	}
	Execute(*config, query, options)
}

//Execute builds the corral driver and runs it
func Execute(config runConfig, query queries.Query, options []corral.Option) *corral.Driver {
	driver := corral.NewSequentialMultiStageDriver(query.Create(), options...)
	if config.Backend != "" && config.Backend != "local" {
		driver.WithBackend(&config.Backend)
	}
	driver.Execute()

	return driver
}

//Run is the main driver and setup logic
func Run(c runConfig) {

	query, options := setup(c)

	err := EnsureCleanBuild()
	if err != nil && !viper.GetBool("debug") {
		panic(err)
	}

	err = GenerateRunnableFile(c, query)
	if err != nil {
		log.Fatalf("failed to generate runnable file %+v", err)
	}
	//create driver
	driver := Execute(c, query, options)

	if c.Validation {
		//catch activationslog and move ...
		var results = driver.GetFinalOutputs()
		temp, err := os.MkdirTemp("", "results")
		if err != nil {
			log.Fatalf("query %s failed to create tempory results folder for %+v", query.Name(), err)
		}
		err = driver.DownloadAndRemove(results, temp)
		if err != nil {
			log.Fatalf("query %s failed to download results %+v", query.Name(), err)
		}
		log.Printf("downloaded final resuts at %s", temp)

		files, err := filepath.Glob(filepath.Join(temp, "*"))
		if err != nil {
			log.Fatalf("query %s failed find downloaded files %+v", query.Name(), err)
		}
		//download results and validate ...
		success, err := query.Validate(files)
		if err != nil {
			log.Fatalf("query %s result is invald %+v", query.Name(), err)
		}
		if !success {
			log.Fatalf("query %s result did not match expectations", query.Name())
		}
	}

	if c.Undeploy && c.Backend != "" && c.Backend != "local" {
		err := driver.Undeploy(&c.Backend)
		if err != nil {
			log.Errorf("failed to undeploy %+v", err)
		}
	}
}

func setup(c runConfig) (queries.Query, []corral.Option) {
	query := queries.New(c.Query)
	if query == nil {
		panic(fmt.Errorf("could not create query from config %+v", c))
	}
	query.SetExperiment(c.Experiment)
	query.SetEndpoint(c.Endpoint)

	if c.Randomize {
		query.Randomize()
	}

	for k, v := range c.CorralConfig {
		viper.Set(k, v)
	}

	viper.Set("logDir", "runs")
	viper.Set("logName", fmt.Sprintf("%s_%s", c.ShortName(), query.Name()))

	options := query.Configure()

	//check if we need to rewrite working locations
	if !(strings.HasPrefix(c.Endpoint, "s3") || strings.HasPrefix(c.Endpoint, "minio")) {
		wd, err := os.MkdirTemp("", "corral")
		if err == nil {
			options = append(options, corral.WithWorkingLocation(wd))
		}
	} else {
		options = append(options, corral.WithWorkingLocation(fmt.Sprintf("%s/%s", c.Endpoint, "output")))
	}

	//check if we want to run on a cloud platform
	if !c.isLocal() {
		panic("need to implement this for cloud run mode!")
	}

	options = c.SetupCache(options)
	return query, options
}

//we need all this to be able to provide experiment settings at compile time...
var config *runConfig
var parameter map[string]string

type config_template struct {
	Query      int
	Backend    string
	Experiment string
	Endpoint   string
	Cache      string
	Params     map[string]string
}

const runnableTemplate = `package main

import "github.com/tawalaya/corral_plus_tpch/queries"

//file is generated do not modify manually 
func init(){
	config = &runConfig{
		Query:      queries.TPCH_Q{{.Query}},
		Backend:    "{{.Backend}}",
		Experiment: "{{.Experiment}}",
		Endpoint:   "{{.Endpoint}}",
		Undeploy:   false,
		Randomize:  false,
		Cache:      "{{.Cache}}",
	}
	
	parameter = map[string]string{
		{{ range $key, $value := .Params }}
			"{{ $key }}":"{{$value}}",
		{{ end }}
	}
}
`

//GenerateRunnableFile converts the selected config into a static go-file, since corral will compile an executable to run on the cloud
func GenerateRunnableFile(c runConfig, query queries.Query) error {
	t := config_template{
		Query:      int(c.Query) + 1, //offset by 1
		Backend:    c.Backend,
		Experiment: c.Experiment,
		Endpoint:   c.Endpoint,
		Cache:      c.Cache,
		Params:     query.Serialize(),
	}
	temp := template.New("config")
	temp, err := temp.Parse(runnableTemplate)
	if err != nil {
		return err
	}

	f, err := os.OpenFile("runnable.go", os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0664)
	if err != nil {
		return err
	}
	defer f.Close()

	err = temp.Execute(f, t)
	if err != nil {
		return err
	}
	return nil
}
