package queries

import (
	"fmt"
	"github.com/ISE-SMILE/corral"
	"math/rand"
	"strconv"
	"time"
)

type Q15 struct {
	Experiment
	Year  int
	Month int
	date  time.Time
}

func (q *Q15) Name() string {
	return fmt.Sprintf("%s_tcph_q15_%d_%d", q.ShortName(), q.Year, q.Month)
}

func (q *Q15) Check(driver *corral.Driver) error {
	panic("implement me")
}

func (q *Q15) Create() []*corral.Job {
	panic("implement me")
}

func (q *Q15) Configure() []corral.Option {
	return []corral.Option{
		corral.WithInputs(inputTables(q, "lineitem", "supplier")...),
	}
}

func (q *Q15) Validate(strings []string) (bool, error) {
	panic("implement me")
}
func (q *Q15) Serialize() map[string]string {
	m := make(map[string]string)
	m["year"] = fmt.Sprintf("%d", q.Year)
	m["month"] = fmt.Sprintf("%d", q.Month)
	return m
}

func (q *Q15) Read(m map[string]string) error {
	year, err := strconv.ParseInt(m["year"], 10, 32)
	if err != nil {
		return err
	}
	q.Year = int(year)

	month, err := strconv.ParseInt(m["month"], 10, 32)
	q.Month = int(month)

	q.configure()
	return err
}

func (q *Q15) Default() {
	q.Year = 1996
	q.Month = 1
	q.configure()
}

func (q *Q15) Randomize() {
	q.Year = 1993 + rand.Intn(4)

	if q.Year < 1997 {
		q.Month = 1 + rand.Intn(11)
	} else {
		q.Month = 1 + rand.Intn(9)
	}

	q.configure()
}

func (q *Q15) configure() {
	date, _ := time.Parse("2006-01-02", fmt.Sprintf("%d-%02d-01", q.Year, q.Month))
	q.date = date
}
