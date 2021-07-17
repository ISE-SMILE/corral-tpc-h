package queries

import (
	"fmt"
	"github.com/ISE-SMILE/corral"
	"strings"
)

type Query interface {
	QueryExperiment

	//Name unique query name, based on configured inputs
	Name() string

	//Inputs generates the list of query inputs given a FileSystem
	Inputs() []string

	//Check if the inputs are ready
	Check(driver *corral.Driver) error

	//Create a corral job for this query
	Create() []*corral.Job

	//Configure  the query based on the inputs
	Configure() []corral.Option

	//Validate  the query results based on a list of outputs
	Validate([]string) (bool,error)

}

type QueryExperiment interface {
	SetPrefix(string)
	GetPrefix() string

	SetEndpoint(string)
	SetExperiment(string)
}

type Experiment struct {
	prefix string
	endpoint string
	experiment string
}

func (e *Experiment) SetPrefix(s string) {
	e.prefix = s
	i := strings.LastIndex(s,"/")

	e.experiment = s[i:]
	e.endpoint = s[:i]
}

func (e *Experiment) GetPrefix() string {
	if e.prefix == "" {
		if e.endpoint != "" {
			e.prefix = fmt.Sprintf("%s/%s", e.endpoint, e.experiment)
		} else {
			e.prefix = e.experiment
		}
	}
	return e.prefix
}

func (e *Experiment)SetEndpoint(s string) {
	e.endpoint = s
}

func (e *Experiment) SetExperiment(s string) {
	e.experiment = s
}


func (e *Experiment) ShortName() string {
	if e.endpoint != "" {
		var etype string
		if strings.IndexByte(e.endpoint,byte(':')) > 0{
			etype = strings.Split(e.endpoint,":")[0]
		} else {
			etype = "local"
		}

		return fmt.Sprintf("%s_%s",etype,e.experiment)
	} else {
		return fmt.Sprintf("%s_%s","local",e.experiment)
	}
}