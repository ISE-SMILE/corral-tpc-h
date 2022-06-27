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
	start time.Time
	end   time.Time
}

func (q *Q15) Name() string {
	return fmt.Sprintf("%s_tcph_q15_%d_%d", q.ShortName(), q.Year, q.Month)
}

func (q *Q15) Check(driver *corral.Driver) error {
	panic("implement me")
}

func (q *Q15) Configure() []corral.Option {
	return []corral.Option{
		corral.WithMultiStageInputs([][]string{
			inputTables(q, "lineitem"),
			inputTables(q, "supplier"),
			[]string{},
		}),

		//corral.WithInputs(inputTables(q, "lineitem", "supplier")...),
		corral.WithSplitSize(64 * 1024 * 1024),
		corral.WithMapBinSize(256 * 1024 * 1024),
		corral.WithReduceBinSize(128 * 1024 * 1024),
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
	q.start = date
	q.end = date.AddDate(0, 3, 0)
}

/**
create view revenue[STREAM_ID] (supplier_no, total_revenue) as
select  l_suppkey,  sum(l_extendedprice * (1 - l_discount))
from  lineitem
where  l_shipdate >= date '[DATE]' and l_shipdate < date '[DATE]' + interval '3' month
group by  l_suppkey;

select s_suppkey,  s_name,  s_address,  s_phone,  total_revenue

from  supplier,  revenue[STREAM_ID]
where  s_suppkey = supplier_no
and total_revenue = ( select  max(total_revenue) from  revenue[STREAM_ID] )
order by  s_suppkey; drop view revenue[STREAM_ID];
*/
func (q *Q15) Create() []*corral.Job {
	view := &Q15RevenueView{q}
	join := &Join{
		Query: q,
		left: &GenericTable{
			Name:      "view",
			numFields: 2,
		},
		right: Supplier(),
		on:    [2]int{0, int(S_SUPPKEY)},
		filter: [2]Projection{
			nil,
			func(table *GenericTable) []int {
				return []int{
					int(S_SUPPKEY), int(S_NAME), int(S_ADDRESS), int(S_PHONE),
				}
			},
		},
		customReduce: nil,
	}

	max := &Q15SelectMax{}

	return []*corral.Job{
		corral.NewJob(view, view),
		corral.NewJob(join, join),
		corral.NewJob(max, max),
	}
}

type Q15RevenueView struct {
	q *Q15
}

func (q Q15RevenueView) Map(key, value string, emitter corral.Emitter) {
	tab := LineItem()
	tab.Read(value)

	shipdate := tab.GetAs(int(L_SHIPDATE), SQLDate).(time.Time)

	if (shipdate == q.q.start || shipdate.After(q.q.start)) && shipdate.Before(q.q.end) {

		l_extendedprice := tab.GetAs(int(L_EXTENDEDPRICE), Float).(float64)
		l_discount := tab.GetAs(int(L_DISCOUNT), Float).(float64)

		emitter.Emit(tab.Get(int(L_SUPPKEY)), fmt.Sprintf("%f", l_extendedprice*(1-l_discount)))
	}
}

func (q Q15RevenueView) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {
	sum := 0.0
	for v := range values.Iter() {
		sum += Float(v).(float64)
	}

	emitter.Emit(key, fmt.Sprintf("%s|%f", key, sum))
}

type Q15SelectMax struct{}

func (q Q15SelectMax) Map(key, value string, emitter corral.Emitter) {
	emitter.Emit("", value)
}
func (q Q15SelectMax) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {
	tab := GenericTable{
		numFields: 6,
	}

	max := 0.0
	maxSub := ""
	for l := range values.Iter() {
		tab.Read(l)

		revenue := tab.GetAs(1, Float).(float64)
		if revenue > max {
			max = revenue
			maxSub = l
		}
	}

	emitter.Emit("", maxSub)
}
