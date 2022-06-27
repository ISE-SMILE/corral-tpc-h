package queries

import (
	"fmt"
	"time"

	"github.com/ISE-SMILE/corral"
)

type Q21 struct {
	Experiment
	NATION string
}

func (q *Q21) Name() string {
	return fmt.Sprintf("%s_tcph_q21_d%s", q.ShortName(), q.NATION)
}

func (q *Q21) Check(driver *corral.Driver) error {
	panic("implement me")
}

func (q *Q21) Configure() []corral.Option {
	return []corral.Option{
		corral.WithMultiStageInputs([][]string{
			//basejoin
			inputTables(q, "nation", "supplier"),
			inputTables(q, "lineitem"),
			//pre_exist join
			inputTables(q, "lineitem"),
			//order_join_filter
			inputTables(q, "orders"),
			//group_by
			[]string{},
			//sort
			[]string{},
		}),
		corral.WithSplitSize(64 * 1024 * 1024),
		corral.WithMapBinSize(192 * 1024 * 1024),
		corral.WithReduceBinSize(192 * 1024 * 1024),
	}
}

func (q *Q21) Validate(strings []string) (bool, error) {
	panic("implement me")
}

func (q *Q21) Serialize() map[string]string {
	m := make(map[string]string)
	m["NATION"] = fmt.Sprintf("%d", q.NATION)
	return m
}

func (q *Q21) Read(m map[string]string) error {
	q.NATION = m["NATION"]
	return nil
}

func (q *Q21) Default() {
	q.NATION = "SAUDI ARABIA"
}

func (q *Q21) Randomize() {
	q.NATION = RandomNation()
}

func (q *Q21) Create() []*corral.Job {

	nationSuplierJoin := &Join{
		Query: q,
		left:  Nation(),
		right: Supplier(),
		on:    [2]int{int(N_NATIONKEY), int(S_NATIONKEY)},
		filter: [2]Projection{
			func(table *GenericTable) []int {
				if table.Get(int(N_NAME)) == q.NATION {
					return []int{int(N_NATIONKEY)}
				} else {
					return nil
				}
			},
			func(table *GenericTable) []int {
				return []int{int(S_SUPPKEY), int(S_NAME)}
			},
		},
	}

	baseJoin := &Join{
		Query: q,
		left:  &GenericTable{},
		right: LineItem(),
		on:    [2]int{1, int(L_SUPPKEY)},
		customReduce: func(join *Join, key string, left, right []string, emitter corral.Emitter) {
			base := LineItem()
			for _, r := range right {
				_ = base.Read(r)
				rec := base.GetAs(int(L_RECEIPTDATE), SQLDate).(time.Time)
				com := base.GetAs(int(L_COMMITDATE), SQLDate).(time.Time)
				if rec.After(com) {
					for _, l := range left {
						emitter.Emit(key, concat(l, r))
					}
				}
			}
		},
	}

	lineSuply := &Join{
		Query: q,
		left:  &GenericTable{},
		right: LineItem(),
		on:    [2]int{int(L_ORDERKEY) + 3, int(L_ORDERKEY)},
		customReduce: func(join *Join, key string, left, right []string, emitter corral.Emitter) {
			base := &GenericTable{}
			cross := LineItem()

			for _, l := range left {
				_ = base.Read(l)
				sub := base.Get(int(L_SUPPKEY) + 3)

				for _, r := range right {
					_ = cross.Read(r)
					if sub != cross.Get(int(L_SUPPKEY)) {
						rec := cross.GetAs(int(L_RECEIPTDATE), SQLDate).(time.Time)
						com := cross.GetAs(int(L_COMMITDATE), SQLDate).(time.Time)
						if !rec.After(com) {
							continue
						}
					}
				}
				emitter.Emit(key, l)
			}
		},
	}

	orderJoin := &Join{
		Query: q,
		left:  &GenericTable{},
		right: Order(),
		on:    [2]int{int(L_ORDERKEY) + 3, int(O_ORDERKEY)},
	}

	grb := &GroupBySum{
		Q21: q,
		tbl: &GenericTable{},
	}
	//TODO: group by suppl
	sort := &Sort{
		Query: q,
		from:  &GenericTable{},
		on:    []int{1},
		keyMapper: func(strings []string) string {
			v := 100000 - Integer(strings[0]).(int64)
			return fmt.Sprintf("%d", v)
		},
	}

	return []*corral.Job{
		corral.NewJob(nationSuplierJoin, nationSuplierJoin),
		corral.NewJob(baseJoin, baseJoin),
		corral.NewJob(lineSuply, lineSuply),
		corral.NewJob(orderJoin, orderJoin),
		corral.NewJob(grb, grb),
		corral.NewJob(sort, sort),
	}
}

type GroupBySum struct {
	*Q21
	tbl *GenericTable
}

func (w *GroupBySum) Map(key, value string, emitter corral.Emitter) {
	w.tbl.Read(value)
	emitter.Emit(w.tbl.Get(2), fmt.Sprint(1))
}

func (w *GroupBySum) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {
	cnt := 0
	for range values.Iter() {
		cnt++
	}
	emitter.Emit(key, fmt.Sprintf("%s|%d", key, cnt))
}
