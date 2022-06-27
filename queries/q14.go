package queries

import (
	"bytes"
	"fmt"
	"github.com/ISE-SMILE/corral"
	"io/ioutil"
	"math"
	"math/rand"
	"strconv"
	"strings"
	"time"
)

type Q14 struct {
	Experiment
	Year  int
	start time.Time
	end   time.Time
}

func (q *Q14) Name() string {
	return fmt.Sprintf("%s_tcph_q14_y%.2d", q.ShortName(), q.Year)
}

func (q *Q14) Check(driver *corral.Driver) error {
	panic("implement me")
}

func (q *Q14) Configure() []corral.Option {
	return []corral.Option{
		corral.WithMultiStageInputs([][]string{
			inputTables(q, "lineitem", "part"),
			[]string{},
		}),
		corral.WithSplitSize(64 * 1024 * 1024),
		corral.WithMapBinSize(256 * 1024 * 1024),
		corral.WithReduceBinSize(128 * 1024 * 1024),
	}
}

//only works for validation set
func (q *Q14) Validate(files []string) (bool, error) {
	if len(files) > 1 {
		return false, nil
	}

	data, err := ioutil.ReadFile(files[0])
	if err != nil {
		return false, err
	}

	line := bytes.Split(data, []byte("\t"))
	revenue := Float(string(line[1])).(float64)

	return math.Abs(revenue-16.38) < 0.5, nil
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
	q.start = date
	q.end = date.AddDate(0, 1, 0)
}

/**
select 100.00 * sum(case  when p_type like 'PROMO%' then l_extendedprice*(1-l_discount) else 0 end) / sum(l_extendedprice * (1 - l_discount)) as promo_revenue
from  lineitem,  part
where  l_partkey = p_partkey and l_shipdate >= start '[DATE]' and l_shipdate < start '[DATE]' + interval '1' month;
*/
func (q *Q14) Create() []*corral.Job {
	partLineitmJoin := &Join{
		Query: q,
		left:  Part(),
		right: LineItem(),
		on:    [2]int{int(P_PARTKEY), int(L_PARTKEY)},
		filter: [2]Projection{
			func(table *GenericTable) []int {
				return []int{
					int(P_TYPE),
				}
			},
			func(table *GenericTable) []int {
				shipdate := table.GetAs(int(L_SHIPDATE), SQLDate).(time.Time)
				if shipdate.After(q.start) && shipdate.Before(q.end) {
					return []int{
						int(L_EXTENDEDPRICE), int(L_DISCOUNT),
					}
				}
				return nil
			},
		},
		customReduce: func(join *Join, key string, left, right []string, emitter corral.Emitter) {

			tab := &GenericTable{
				numFields: 2,
			}

			for _, p_type := range left {
				for _, line := range right {
					tab.Read(line)
					l_extendedprice := tab.GetAs(0, Float).(float64)
					l_discount := tab.GetAs(1, Float).(float64)
					revenue := l_extendedprice * (1 - l_discount)
					if strings.HasPrefix(p_type, "PROMO") {
						emitter.Emit("", fmt.Sprintf("%f|%f", revenue, revenue))
					} else {
						emitter.Emit("", fmt.Sprintf("%f|%f", 0.0, revenue))
					}
				}

			}
		},
	}

	return []*corral.Job{
		corral.NewJob(partLineitmJoin, partLineitmJoin),
		corral.NewJob(&Identity{}, q),
	}
}

func (q *Q14) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {
	promo := 0.0
	total := 0.0
	for line := range values.Iter() {
		elems := strings.Split(line, "|")
		promo += Float(elems[0]).(float64)
		total += Float(elems[1]).(float64)
	}

	emitter.Emit("promo_revenue", fmt.Sprintf("%f", (100.0*promo/total)))
}
