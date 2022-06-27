package queries

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ISE-SMILE/corral"
	log "github.com/sirupsen/logrus"
)

type Q6 struct {
	Experiment
	Before   time.Time
	After    time.Time
	Start    int
	Discount float64
	Quantity int64
}

func (q *Q6) Name() string {
	return fmt.Sprintf("%s_tcph_q6_y%.4d_d%d_q%d", q.ShortName(),
		q.Before.Year(), int(q.Discount*100), q.Quantity)
}

func (q *Q6) Check(driver *corral.Driver) error {
	panic("implement me")
}

func (q *Q6) Default() {
	q.Start = 1
	q.Discount = 0.06
	q.Quantity = 24
	q.configure()

}

func (q *Q6) Randomize() {
	q.Start = rand.Intn(4)
	q.Discount = float64(2+rand.Intn(7)) / 100
	q.Quantity = int64(24 + rand.Intn(1))

	q.configure()
}

func (q *Q6) configure() {
	date, _ := time.Parse("2006-01-02", "1993-01-01")
	date = date.AddDate(q.Start, 0, 0)
	after := date.AddDate(1, 0, 0)
	q.Before = date.AddDate(0, 0, -1)
	q.After = after
}

func (q *Q6) Serialize() map[string]string {
	m := make(map[string]string)
	m["start"] = fmt.Sprintf("%d", q.Start)
	m["discount"] = fmt.Sprintf("%d", int(q.Discount*100))
	m["quantity"] = fmt.Sprintf("%d", q.Quantity)

	return m
}

func (q *Q6) Read(m map[string]string) (err error) {
	start, err := strconv.ParseInt(m["start"], 10, 32)
	discount, err := strconv.ParseInt(m["discount"], 10, 32)
	quantity, err := strconv.ParseInt(m["quantity"], 10, 32)

	q.Start = int(start)
	q.Discount = float64(discount) / 100
	q.Quantity = quantity
	q.configure()

	return err
}

func (q *Q6) Configure() []corral.Option {
	return []corral.Option{
		corral.WithInputs(inputTables(q, "lineitem")...),
		corral.WithSplitSize(32 * 1024 * 1024),
		corral.WithMapBinSize(128 * 1024 * 1024),
		corral.WithReduceBinSize(64 * 1024 * 1024),
	}
}

func (q *Q6) Validate(inputs []string) (bool, error) {
	buf := bytes.NewBuffer([]byte{})
	for _, f := range inputs {
		f, err := os.Open(f)
		if err != nil {
			return false, err
		}
		_, err = io.Copy(buf, f)
		if err != nil {
			return false, err
		}
	}

	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			break
		}
		fields := strings.Split(line, "|")
		if len(fields) != 2 {
			return false, fmt.Errorf("output invalid %s", line)
		}
	}

	return true, nil
}

func (q *Q6) Create() []*corral.Job {
	return []*corral.Job{corral.NewJob(q, q)}
}

/**sql
	select
			sum(l_extendedprice * l_discount) as revenue
	from
			lineitem
	where
			l_shipdate >= start '1994-01-01'
			and l_shipdate < start '1995-01-01'
			and l_discount between 0.06 - 0.01 and 0.06 + 0.01
			and l_quantity < 24
**/
func (w *Q6) Map(key, value string, emitter corral.Emitter) {
	line := LineItem()

	err := line.Read(value)
	if err != nil {
		log.Infof("failed to emit %s,+%v", key, err)
		return
	}
	//first the where clause

	quantity, _ := line.LookupAs("L_QUANTITY", Integer)
	discount, _ := line.LookupAs("L_DISCOUNT", Float)
	shipdate, _ := line.LookupAs("L_SHIPDATE", SQLDate)
	//we could optimize this fruther by doing a sting length check before conferting to a string
	where := quantity.(int64) < w.Quantity && math.Abs(discount.(float64)-w.Discount) <= 0.01 && shipdate.(time.Time).After(w.Before) && shipdate.(time.Time).Before(w.After)
	//log.Infof("DATE %v", shipdate)
	//log.Infof("BEFORE DATE %v", w.Before)
	//log.Infof("START DATE %b", shipdate.(time.Time).After(w.Before))
	//log.Infof("END DATE %b", shipdate.(time.Time).Before(w.After))

	if where {

		extendedprice, _ := line.LookupAs("L_EXTENDEDPRICE", Float)
		prod := discount.(float64) * extendedprice.(float64)
		_ = emitter.Emit("revenue", fmt.Sprintf("%f", prod))
	}

}

func (w *Q6) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {

	sum := 0.
	for prod := range values.Iter() {
		sum += Float(prod).(float64)
	}
	_ = emitter.Emit("revenue", fmt.Sprintf("|%f", sum))

}
