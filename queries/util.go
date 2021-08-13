package queries

import (
	"fmt"
	"github.com/ISE-SMILE/corral"
	"strconv"
	"strings"
	"time"
)

func inputTables(q QueryExperiment, tables ...string) []string {
	inputs := make([]string, 0)
	for _, table := range tables {
		inputs = append(inputs, strings.Join([]string{q.GetPrefix(), table, "*"}, "/"))
	}
	return inputs
}

func isInputTable(w Query, key, table string) bool {
	return strings.HasPrefix(key, fmt.Sprintf("%s/%s", w.GetPrefix(), table))
}

func SQLDate(val string) interface{} {
	s_year, _ := strconv.Atoi(val[0:4])
	s_month, _ := strconv.Atoi(val[5:7])
	s_day, _ := strconv.Atoi(val[8:10])

	//Shipdate, _ := time.Parse("2006-01-02", line[L_SHIPDATE])
	return time.Date(s_year, time.Month(s_month), s_day, 0, 0, 0, 0, time.Local)
}

func Integer(val string) interface{} {
	r, _ := strconv.ParseInt(val, 10, 64)
	return r
}

func Float(val string) interface{} {
	r, _ := strconv.ParseFloat(val, 64)
	return r
}

//fast concatenation
func concat(l string, r string) string {
	bs := make([]byte, len(l)+len(r)+1)
	i := copy(bs, l)
	bs[i] = '|'
	copy(bs[i+1:], r)
	return string(bs)
}

type Identity struct{}

func (i Identity) Map(key, value string, emitter corral.Emitter) {
	emitter.Emit(key, value)
}
