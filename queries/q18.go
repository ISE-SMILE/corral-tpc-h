package queries

import (
	"fmt"
	"github.com/ISE-SMILE/corral"
	"math/rand"
	"strconv"
)

type Q18 struct {
	Experiment
	QUANTITY int
}

func (q *Q18) Name() string {
	return fmt.Sprintf("%s_tcph_q18_d%d", q.ShortName(), q.QUANTITY)
}

func (q *Q18) Check(driver *corral.Driver) error {
	panic("implement me")
}

func (q *Q18) Create() []*corral.Job {
	panic("implement me")
}

func (q *Q18) Configure() []corral.Option {
	return []corral.Option{
		corral.WithInputs(inputTables(q, "customer", "orders", "lineitem")...),
	}
}

func (q *Q18) Validate(strings []string) (bool, error) {
	panic("implement me")
}

func (q *Q18) Serialize() map[string]string {
	m := make(map[string]string)
	m["QUANTITY"] = fmt.Sprintf("%d", q.QUANTITY)
	return m
}

func (q *Q18) Read(m map[string]string) error {
	qnt, err := strconv.ParseInt(m["QUANTITY"], 10, 32)

	q.QUANTITY = int(qnt)

	return err
}

func (q *Q18) Default() {
	q.QUANTITY = 300
}

func (q *Q18) Randomize() {
	q.QUANTITY = 312 + rand.Intn(3)
}
