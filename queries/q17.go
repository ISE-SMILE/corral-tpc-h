package queries

import (
	"bytes"
	"fmt"
	"github.com/ISE-SMILE/corral"
	log "github.com/sirupsen/logrus"
	"io"
	"math/rand"
	"os"
	"strconv"
	"strings"
)

type Q17 struct {
	Experiment
	Brand     int
	Container string
}

func (q *Q17) Serialize() map[string]string {
	m := make(map[string]string)

	m["Brand"] = fmt.Sprintf("%d", q.Brand)
	m["Container"] = q.Container

	return m
}

func (q *Q17) Read(m map[string]string) error {
	brand, err := strconv.ParseInt(m["Brand"], 10, 32)
	q.Brand = int(brand)
	q.Container = m["Container"]

	return err
}

func (q *Q17) Name() string {
	return fmt.Sprintf("%s_tcph_q17_b%d_%s", q.ShortName(), q.Brand, q.Container)
}

func (q *Q17) Default() {
	q.Brand = 23
	q.Container = "MED BOX"
}

func (q *Q17) Randomize() {
	q.Brand = 1 + rand.Intn(4) + 10*(1+rand.Intn(4))
	q.Container = RandomContainerName()
}

func (q *Q17) Check(driver *corral.Driver) error {
	panic("implement me")
}

func (q *Q17) Configure() []corral.Option {
	return []corral.Option{
		corral.WithInputs(inputTables(q, "lineitem", "part")...),
		corral.WithSplitSize(64 * 1024 * 1024),
		corral.WithMapBinSize(256 * 1024 * 1024),
		corral.WithReduceBinSize(128 * 1024 * 1024),
	}
}

func (q *Q17) Validate(inputs []string) (bool, error) {

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

	lines := 0
	for {
		line, err := buf.ReadString('\n')
		if err != nil {
			break
		}
		log.Infof("%s", line)
		lines++
	}

	if lines > 1 {
		return false, fmt.Errorf("expected only a single line as the result")
	}
	return true, nil
}

func (q *Q17) Create() []*corral.Job {
	stage01 := &Q17PreCollect{q}
	stage02 := &Q17Finalize{q}
	return []*corral.Job{corral.NewJob(stage01, stage01), corral.NewJob(stage02, stage02)}
}

type Q17PreCollect struct {
	*Q17
}

type Q17Finalize struct {
	*Q17
}

/**
select
        sum(l_extendedprice) / 7.0 as avg_yearly
from
        lineitem,
        part
where
        p_partkey = l_partkey
        and p_brand = 'Brand#33'
        and p_container = 'WRAP PACK'
        and l_quantity < (
                select
                        0.2 * avg(l_quantity)
                from
                        lineitem
                where
                        l_partkey = p_partkey
        );
*/
func (w *Q17PreCollect) Map(key, value string, emitter corral.Emitter) {
	if isInputTable(w, key, "lineitem") {
		line := LineItem()

		err := line.Read(value)
		if err != nil {
			log.Infof("failed to emit %s,+%v", key, err)
			return
		}

		k, _ := line.Lookup("L_PARTKEY")
		proj, _ := line.Select("L_QUANTITY", "L_EXTENDEDPRICE")
		emitter.Emit(k, strings.Join(proj, "|"))
	} else if isInputTable(w, key, "part") {

		//we can alread do the prefilter of the quanity select at this stage...
		line := Part()

		err := line.Read(value)
		if err != nil {
			log.Infof("failed to emit %s,+%v", key, err)
			return
		}

		brand, _ := line.Lookup("P_BRAND")
		container, _ := line.Lookup("P_CONTAINER")

		brandString := fmt.Sprintf("Brand#%d", w.Brand)
		if brand == brandString && container == w.Container {
			k, _ := line.Lookup("P_PARTKEY")
			emitter.Emit(k, "PART")
		}

	} else {
		log.Errorf("unkown key:%s", key)
	}
}

func (w *Q17PreCollect) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {
	avg := float64(0.0)
	group := make([]*GenericTable, 0)
	parts := 0
	for value := range values.Iter() {
		if value != "PART" {
			line := &GenericTable{}
			_ = line.Read(value)
			group = append(group, line)
			err := line.Read(value)
			if err != nil {
				log.Infof("failed to emit %s,+%v", key, err)
				return
			}

			quantiy := line.GetAs(0, Float)
			avg += quantiy.(float64)
		} else {
			parts++
		}
	}
	sum := 0.0
	//skip if we did not see any matching parts to that item
	if parts > 0 && len(group) > 0 {
		avg = (avg / float64(len(group))) * 0.2
		for _, line := range group {
			q := line.GetAs(0, Float)
			//we just assume that err != nil ...
			if q.(float64) < avg {
				p := line.GetAs(1, Float)
				sum += p.(float64)
			}
		}

		emitter.Emit(key, fmt.Sprintf("%f", sum))
	}

}

func (w *Q17Finalize) Map(key, value string, emitter corral.Emitter) {
	emitter.Emit("", value)
}

func (w *Q17Finalize) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {

	sum := 0.0
	for val := range values.Iter() {
		f := Float(val)
		sum += f.(float64)
	}
	sum = sum / 7.0
	emitter.Emit("AVG_YEARLY", fmt.Sprintf("%f", sum))
}
