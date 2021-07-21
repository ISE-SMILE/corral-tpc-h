package queries

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"os"
	"strings"
	"time"

	"github.com/ISE-SMILE/corral"
	log "github.com/sirupsen/logrus"
)

type Q6 struct {
	Experiment
	Before    time.Time
	After     time.Time
	Discound  float64
	Quantitiy int64
}

func (q *Q6) Name() string {
	return fmt.Sprintf("%s_tcph_q6", q.ShortName())
}

func (q *Q6) Default() {
	before, _ := time.Parse("2006-01-02", "1995-01-01")
	after, _ := time.Parse("2006-01-02", "1994-01-01")

	q.Discound = 0.05
	q.Quantitiy = 24
	q.Before = before
	q.After = after

}

func (q *Q6) Randomize() {
	//TODO:
	q.Default()
}

func (q *Q6) Check(driver *corral.Driver) error {
	//TODO
	return nil
}

func (q *Q6) Inputs() []string {
	return inputTables(q, "lineitem")
}

func (q *Q6) Configure() []corral.Option {
	return []corral.Option{
		corral.WithSplitSize(25 * 1024 * 1024),
		corral.WithMapBinSize(200 * 1024 * 1024),
		corral.WithReduceBinSize(200 * 1024 * 1024),
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
			l_shipdate >= date '1994-01-01'
			and l_shipdate < date '1995-01-01'
			and l_discount between 0.06 - 0.01 and 0.06 + 0.01
			and l_quantity < 24
**/
func (w *Q6) Map(key, value string, emitter corral.Emitter) {
	line := &LineItem{}

	err := line.Read(value)
	if err != nil {
		log.Infof("failed to emit %s,+%v", key, err)
		return
	}
	//first the where clause

	quantitiy, _ := line.GetAs("L_QUANTITY", Integer)
	discound, _ := line.GetAs("L_DISCOUNT", Float)
	shipdate, _ := line.GetAs("L_SHIPDATE", SQLDate)
	//we could optimize this fruther by doing a sting length check before conferting to a string
	where := quantitiy.(int64) < w.Quantitiy && math.Abs(discound.(float64)-w.Discound) <= 0.01 && shipdate.(time.Time).Before(w.Before) && shipdate.(time.Time).After(w.After)

	if where {
		extendedprice, _ := line.GetAs("L_EXTENDEDPRICE", Float)
		prod := discound.(float64) * extendedprice.(float64)
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
