package queries

import (
	"fmt"
	"github.com/ISE-SMILE/corral"
	log "github.com/sirupsen/logrus"
	"strings"
)

//if Projection retuns nil, we assmue the table row was filtered...
type Projection func(table *GenericTable) []int

type Join struct {
	Query
	left  *GenericTable
	right *GenericTable
	on    [2]int

	filter [2]Projection

	customReduce func(join *Join, key string, left, right []string, emitter corral.Emitter)
}

func (q *Join) Map(key, value string, emitter corral.Emitter) {
	if isInputTable(q, key, q.right.Name) {
		q.join(value, 1, q.right, emitter)
	} else {
		//pre defintion we always assume left for all non-right keys?
		q.join(value, 0, q.left, emitter)
	}
}

func (q *Join) join(value string, side int, table *GenericTable, emitter corral.Emitter) {
	err := table.Read(value)
	if err != nil {
		log.Errorf("failed to join %+v", q.on)
	}
	key := table.Get(q.on[side])
	if q.filter[side] != nil {
		mask := q.filter[side](table)
		if mask == nil {
			return
		}

		//do we actually need to project anything?
		if len(mask) < table.numFields {
			values, err := table.SelectWithMask(mask...)
			if err != nil {
				log.Errorf("failed to project in join %+v", err)
				return
			}
			//damm this is some ugly code...
			emitter.Emit(key, strings.Join(append([]string{fmt.Sprintf("%d", side)}, values...), "|"))
			return
		}

		//in this case we default to emmit all
	}

	emitter.Emit(key, concat(fmt.Sprintf("%d", side), value))

}

func (q *Join) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {
	left := make([]string, 0)
	right := make([]string, 0)
	for value := range values.Iter() {
		if value[0] == '0' {
			left = append(left, value[2:])
		} else {
			right = append(right, value[2:])
		}
	}

	if len(left) <= 0 || len(right) <= 0 {
		return
	}
	if q.customReduce != nil {
		q.customReduce(q, key, left, right, emitter)
	} else {
		for _, l := range left {
			for _, r := range right {
				emitter.Emit("", concat(l, r))
			}
		}
	}
}
