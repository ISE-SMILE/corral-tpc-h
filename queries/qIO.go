package queries

import (
	"fmt"
	"github.com/ISE-SMILE/corral"
	"strings"
)

type QIO struct {
	Experiment
}

func (q *QIO) Name() string {
	return fmt.Sprintf("%s_tcph_qIO", q.ShortName())
}

func (q *QIO) Check(driver *corral.Driver) error {
	panic("implement me")
}

func (q *QIO) Configure() []corral.Option {
	inputs := [][]string{
		inputTables(q, "lineitem", "orders", "part", "partsupp", "customer", "supplier", "nation"),
	}

	return []corral.Option{
		corral.WithMultiStageInputs(inputs),
		corral.WithSplitSize(192 * 1024 * 1024),
		corral.WithMapBinSize(512 * 1024 * 1024),
		corral.WithReduceBinSize(256 * 1024 * 1024),
	}
}

func (q *QIO) Validate(strings []string) (bool, error) {
	panic("implement me")
}

func (q *QIO) Serialize() map[string]string {
	m := make(map[string]string)
	return m
}

func (q *QIO) Read(m map[string]string) error {

	return nil
}

func (q *QIO) Default() {
}

func (q *QIO) Randomize() {

}

func (q *QIO) Create() []*corral.Job {

	return []*corral.Job{
		corral.NewJob(q, q),
	}
}

func (q QIO) Map(key, value string, emitter corral.Emitter) {
	emitter.Emit("", fmt.Sprintf("%d", len(strings.Split(value, "|"))))
}

func (q QIO) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {
	sum := int64(0)
	for range values.Iter() {
		sum++
	}
	emitter.Emit(key, fmt.Sprintf("%d", sum))
}
