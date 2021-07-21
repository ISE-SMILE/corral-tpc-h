package queries

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/ISE-SMILE/corral"

	log "github.com/sirupsen/logrus"
)

type Q1 struct {
	Experiment
	Shipdate time.Time
}

func (q *Q1) Default() {
	date, _ := time.Parse("2006-01-02", "1998-09-01")
	q.Shipdate = date
}

func (q *Q1) Randomize() {
	//TODO:
	q.Default()
}

func (q *Q1) Name() string {
	return fmt.Sprintf("%s_tcp_q1", q.ShortName())
}

func (q *Q1) Check(driver *corral.Driver) error {
	//TODO
	return nil
}

func (q *Q1) Inputs() []string {
	return inputTables(q, "lineitem")
}

func (q *Q1) Configure() []corral.Option {
	return []corral.Option{
		corral.WithSplitSize(25 * 1024 * 1024),
		corral.WithMapBinSize(100 * 1024 * 1024),
		corral.WithReduceBinSize(200 * 1024 * 1024),
	}
}

func (q *Q1) Validate(inputs []string) (bool, error) {
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
		if len(fields) != 10 {
			return false, fmt.Errorf("output invalid %s", line)
		}
	}

	return true, nil
}

func (q *Q1) Create() []*corral.Job {
	return []*corral.Job{corral.NewJob(q, q)}
}

/**sql
	select
		l_returnflag,
		l_linestatus,
		sum(l_quantity) as sum_qty,
		sum(l_extendedprice) as sum_base_price,
		sum(l_extendedprice*(1-l_discount)) as sum_disc_price,
		sum(l_extendedprice*(1-l_discount)*(1+l_tax)) as sum_charge,
		avg(l_quantity) as avg_qty,
		avg(l_extendedprice) as avg_price, avg(l_discount) as avg_disc,
		count(*) as count_order
	from
		lineitem
	where
		l_shipdate <= '1998-09-01’
	group by
		l_returnflag,
		l_linestatus
	order by
		l_returnflag,
		l_linestatus
**/

func (w *Q1) Map(key, value string, emitter corral.Emitter) {
	line := &LineItem{}

	err := line.Read(value)
	if err != nil {
		log.Infof("failed to emit %s,+%v", key, err)
		return
	}

	shipdate, err := line.GetAs("L_SHIPDATE", SQLDate)
	if err != nil {
		log.Infof("failed to emit %s,+%v", key, err)
		return
	}

	if shipdate.(time.Time).Before(w.Shipdate) {
		key, err := line.SelectWithMask(int(L_RETURNFLAG), int(L_LINESTATUS))
		if err != nil {
			log.Infof("failed to emit %s,+%v", key, err)
			return
		}
		data, err := line.SelectWithMask(
			int(L_QUANTITY),
			int(L_EXTENDEDPRICE),
			int(L_DISCOUNT),
			int(L_TAX),
		)
		if err != nil {
			log.Infof("failed to emit %s,+%v", key, err)
			return
		}

		err = emitter.Emit(strings.Join(key, "|"), strings.Join(data, "|"))
		if err != nil {
			log.Infof("failed to emit %s,+%v", key, err)
		}
	}

}

func saveFloat(line []string, key int) float64 {
	if len(line) < key {
		return 0.
	}
	val, _ := strconv.ParseFloat(line[key], 32)
	return val
}

func (w *Q1) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {

	sum_base_price := 0.
	sum_qty := 0.
	sum_disc_price := 0.
	sum_discount := 0.
	sum_charge := 0.
	count := 0

	for value := range values.Iter() {
		//data, _ := b64.StdEncoding.DecodeString(value)
		line := strings.Split(value, "|")
		//log.Debugf("red(%s,%s)",key,line)

		l_quantity := saveFloat(line, 0)
		l_extendedprice := saveFloat(line, 1)
		l_discount := saveFloat(line, 2)
		l_tax := saveFloat(line, 3)

		sum_base_price += l_extendedprice
		sum_qty += l_quantity
		sum_discount += l_discount
		sum_disc_price += l_extendedprice * (1 - l_discount)
		sum_charge += l_extendedprice * (1 - l_tax)
		count++

	}

	avg_qty := sum_qty / float64(count)
	avg_price := sum_base_price / float64(count)
	avg_disc := sum_discount / float64(count)

	value := fmt.Sprintf("|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%.2f|%d", sum_qty, sum_base_price, sum_disc_price, sum_charge, avg_qty, avg_price, avg_disc, count)

	err := emitter.Emit(key, value)
	if err != nil {
		log.Infof("failed to emit %s,+%v", key, err)
	}

}
