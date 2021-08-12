package queries

import (
	"fmt"
	"github.com/ISE-SMILE/corral"
	"math/rand"
	"strconv"
	"time"
)

type Q14 struct {
	Experiment
	Year int
	date time.Time
}

func (q *Q14) Name() string {
	return fmt.Sprintf("%s_tcph_q14_y%.2d", q.ShortName(), q.Year)
}

func (q *Q14) Check(driver *corral.Driver) error {
	panic("implement me")
}

func (q *Q14) Create() []*corral.Job {
	panic("implement me")
}

func (q *Q14) Configure() []corral.Option {
	return []corral.Option{
		corral.WithInputs(inputTables(q, "lineitem", "part")...),
	}
}

func (q *Q14) Validate(strings []string) (bool, error) {
	panic("implement me")
}

func (q *Q14) Serialize() map[string]string {
	m := make(map[string]string)
	m["year"] = fmt.Sprintf("%d", q.Year)
	return m
}

func (q *Q14) Read(m map[string]string) error {
	year, err := strconv.ParseInt(m["year"], 10, 32)
	q.Year = int(year)
	q.configure()
	return err
}

func (q *Q14) Default() {
	q.Year = 1995
	q.configure()
}

func (q *Q14) Randomize() {
	q.Year = 1993 + rand.Intn(4)
	q.configure()
}

func (q *Q14) configure() {
	date, _ := time.Parse("2006-01-02", fmt.Sprintf("%d-12-01", q.Year))
	q.date = date
}
