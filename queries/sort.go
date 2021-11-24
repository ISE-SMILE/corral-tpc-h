package queries

import (
	"github.com/ISE-SMILE/corral"
	log "github.com/sirupsen/logrus"
)

type Sort struct {
	Query
	from *GenericTable
	on    int
}

func (s *Sort) Map(key, value string, emitter corral.Emitter) {
	err := s.from.Read(value)
	if err != nil {
		log.Errorf("failed to sort")
	}

	k := s.from.Get(s.on)
	err = emitter.Emit(k, value)
	if err != nil {
		log.Errorf("failed to sort")
	}
}


func (s *Sort) Reduce(key string, values corral.ValueIterator, emitter corral.Emitter) {

}