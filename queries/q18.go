package queries

import (
	"fmt"
	"github.com/ISE-SMILE/corral"
	"math/rand"
	"strconv"
	"time"
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
			[]string{},
		}),
		corral.WithSplitSize(64 * 1024 * 1024),
		corral.WithMapBinSize(256 * 1024 * 1024),
		corral.WithReduceBinSize(256 * 1024 * 1024),
		//corral.WithInputs(inputTables(q, "customer", "orders", "lineitem")...),
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
	//L_OrderKey,O_CUSTKEY,O_ORDERKEY,O_ORDERDATE,O_TOTALPRICE, SUM(L_Quantity)
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
				_ = emitter.Emit(key, concat(right[0], fmt.Sprintf("%f", sum)))
			}

		},
	}

	//C_NAME,C_CUSTKEY,O_ORDERKEY,O_ORDERDATE,O_TOTALPRICE, SUM(L_Quantity)
	customerJoin := &Join{
		Query: q,
		left:  &GenericTable{},
		right: Customer(),
		on:    [2]int{0, int(C_CUSTKEY)},
		filter: [2]Projection{
			//lets take all
			func(table *GenericTable) []int {
				return []int{
					0, 1, 2, 3,
				}
			},
			func(table *GenericTable) []int {
				return []int{
					int(C_NAME),
				}
			},
		},
		customReduce: func(join *Join, key string, left, right []string, emitter corral.Emitter) {
			for _, l := range left {
				for _, r := range right {
					emitter.Emit("", concat(r, l))
				}
			}
		},
	}

	sort := &Sort{
		Query: q,
		from:  &GenericTable{},
		on:    []int{4, 3, 2},
		keyMapper: func(keys []string) string {
			//O_TOTALPROCE
			total := 10000000000 - Integer(keys[0]).(int64)
			//Date
			date := SQLDate(keys[1]).(time.Time).Unix()
			//key
			key := Integer(keys[2]).(int64)

			return fmt.Sprintf("%10d%32d%d", total, date, key)
		},
	}

	return []*corral.Job{
		corral.NewJob(orderJoin, orderJoin),
		corral.NewJob(customerJoin, customerJoin),
		corral.NewJob(sort, sort),
	}
}
