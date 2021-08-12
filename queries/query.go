package queries

import (
	"fmt"
	"strings"

	"github.com/ISE-SMILE/corral"
)

type QueryType int

const (
	TPCH_Q1 QueryType = iota
	TPCH_Q2
	TPCH_Q3
	TPCH_Q4
	TPCH_Q5
	TPCH_Q6
	TPCH_Q7
	TPCH_Q8
	TPCH_Q9
	TPCH_Q10
	TPCH_Q11
	TPCH_Q12
	TPCH_Q13
	TPCH_Q14
	TPCH_Q15
	TPCH_Q16
	TPCH_Q17
	TPCH_Q18
	TPCH_Q19
	TPCH_Q20
	TPCH_Q21
	TPCH_Q22
)

//New Returns a default Query for a given type, randomize the tunables if for a full-scale experiment
func New(q QueryType) Query {
	var query Query
	switch q {
	case TPCH_Q1:
		query = &Q1{}
	case TPCH_Q2:
		query = &Q2{}
	case TPCH_Q6:
		query = &Q6{}
	case TPCH_Q14:
		query = &Q14{}
	case TPCH_Q15:
		query = &Q15{}
	case TPCH_Q17:
		query = &Q17{}
	case TPCH_Q18:
		query = &Q18{}
	default:
		return nil
	}
	query.Default()
	return query
}

type Query interface {
	QueryExperiment
	Serializable

	//Name unique query name, based on configured inputs
	Name() string

	//Check if the inputs are ready
	Check(driver *corral.Driver) error

	//Create a corral job for this query
	Create() []*corral.Job

	//Configure  the query based on the inputs
	Configure() []corral.Option

	//Validate  the query results based on a list of outputs
	Validate([]string) (bool, error)

	//Default sets the default parameters for this query
	Default()

	//Randomze sets random parameters for this query
	Randomize()
}

type Serializable interface {
	Serialize() map[string]string
	Read(map[string]string) error
}

type QueryExperiment interface {
	SetPrefix(string)
	GetPrefix() string

	SetEndpoint(string)
	SetExperiment(string)
}

type Experiment struct {
	prefix     string
	endpoint   string
	experiment string
}

func (e *Experiment) SetPrefix(s string) {
	e.prefix = s
	i := strings.LastIndex(s, "/")

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

func (e *Experiment) SetEndpoint(s string) {
	e.endpoint = s
}

func (e *Experiment) SetExperiment(s string) {
	e.experiment = s
}

func (e *Experiment) ShortName() string {
	if e.endpoint != "" {
		var etype string
		if strings.IndexByte(e.endpoint, byte(':')) > 0 {
			etype = strings.Split(e.endpoint, ":")[0]
		} else {
			etype = "local"
		}

		return fmt.Sprintf("%s_%s", etype, e.experiment)
	} else {
		return fmt.Sprintf("%s_%s", "local", e.experiment)
	}
}
