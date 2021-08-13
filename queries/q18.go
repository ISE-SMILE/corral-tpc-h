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

func (q *Q18) Configure() []corral.Option {
	return []corral.Option{
		corral.WithMultiStageInputs([][]string{
			inputTables(q, "orders", "lineitem"),
			inputTables(q, "customer"),
		}),

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

func (q *Q18) Create() []*corral.Job {
	orderJoin := &Join{
		Query: q,
		left:  LineItem(),
		right: Order(),
		on:    [2]int{int(L_ORDERKEY), int(O_ORDERKEY)},
		filter: [2]Projection{
			func(table *GenericTable) []int {
				return []int{
					int(L_QUANTITY),
				}
			},
			func(table *GenericTable) []int {
				return []int{
					int(O_CUSTKEY), int(O_ORDERKEY), int(O_ORDERDATE), int(O_TOTALPRICE),
				}
			},
		},
		customReduce: func(join *Join, key string, left, right []string, emitter corral.Emitter) {

			sum := 0.0
			for _, quant := range left {
				sum += Float(quant).(float64)
			}

			if sum > float64(q.QUANTITY) {
				for _, line := range right {
					emitter.Emit(key, concat(line, fmt.Sprintf("%f", sum)))
				}
			}

		},
	}

	customerJoin := &Join{
		Query: q,
		left:  &GenericTable{},
		right: Customer(),
		on:    [2]int{0, int(C_CUSTKEY)},
	}

	return []*corral.Job{
		corral.NewJob(orderJoin, orderJoin),
		corral.NewJob(customerJoin, customerJoin),
		//TODO: sort
	}
}
