package queries

import (
	"github.com/ISE-SMILE/corral"
	log "github.com/sirupsen/logrus"
	"strings"
)

//XXX: this is a bad algorithmus but hey, good for benchmarking?
type Sort struct {
	Query
	from      *GenericTable
	on        []int
	keyMapper func([]string) string
}

func (s *Sort) Map(key, value string, emitter corral.Emitter) {
	err := s.from.Read(value)
	if err != nil {
		log.Errorf("failed to sort")
	}

	keys, err := s.from.SelectWithMask(s.on...)
	if err != nil {
		log.Errorf("failed to sort")
	}

	var k string
	if s.keyMapper != nil {
		k = s.keyMapper(keys)
	} else {
		k = strings.Join(keys, "_")
	}

	err = emitter.Emit(k, value)
	if err != nil {
		log.Errorf("failed to sort")
	}
}

func (s *Sort) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {
	for v := range values.Iter() {
		err := emitter.Emit("", v)
		if err != nil {
			log.Errorf("sort error %+v", err)
			return
		}
	}
}
