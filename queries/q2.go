package queries

import (
	"fmt"
	"github.com/ISE-SMILE/corral"
	"github.com/google/martian/v3/log"
	"math"
	"math/rand"
	"strconv"
	"strings"
)

type Q2 struct {
	Experiment
	Size   int
	Type   string
	Region string
}

func (q *Q2) Name() string {
	return fmt.Sprintf("%s_tcph_q2_s%.2d_%s_%s", q.ShortName(), q.Size, q.Region, q.Type)
}

func (q *Q2) Check(driver *corral.Driver) error {
	panic("implement me")
}

func (q *Q2) Configure() []corral.Option {
	inputs := [][]string{
		inputTables(q, "nation", "region"),
		inputTables(q, "supplier"),
		inputTables(q, "partsupp"),
		inputTables(q, "part"),
		[]string{},
	}

	return []corral.Option{
		corral.WithMultiStageInputs(inputs),
		corral.WithSplitSize(32 * 1024 * 1024),
		corral.WithMapBinSize(256 * 1024 * 1024),
		corral.WithReduceBinSize(128 * 1024 * 1024),
	}
}

func (q *Q2) Validate(strings []string) (bool, error) {
	panic("implement me")
}

func (q *Q2) Serialize() map[string]string {
	m := make(map[string]string)
	m["size"] = fmt.Sprintf("%d", q.Size)
	m["type"] = q.Type
	m["region"] = q.Region
	return m
}

func (q *Q2) Read(m map[string]string) error {
	size, err := strconv.ParseInt(m["size"], 10, 32)
	if err != nil {
		return err
	}

	q.Size = int(size)
	q.Type = m["type"]
	q.Region = m["region"]

	return nil
}

func (q *Q2) Default() {
	q.Size = 15
	q.Type = "BRASS"
	q.Region = "EUROPE"
}

func (q *Q2) Randomize() {
	q.Size = rand.Intn(50)
	q.Type = RandomType()
	q.Region = RandomRegion()
}

func (q *Q2) Create() []*corral.Job {
	nationJoin := &Join{
		Query: q,
		left:  Nation(),
		right: Region(),
		on:    [2]int{int(N_REGIONKEY), int(R_REGIONKEY)},
		filter: [2]Projection{
			func(table *GenericTable) []int {
				return []int{
					//0,1
					int(N_NATIONKEY), int(N_NAME),
				}
			},
			func(table *GenericTable) []int {
				if table.Get(int(R_NAME)) == q.Region {
					//2
					return []int{int(R_NAME)}
				} else {
					return nil
				}
			},
		},
	}

	supplierJoin := &Join{
		Query: q,
		left: &GenericTable{
			Name:      "job0",
			numFields: 3,
		},
		right: Supplier(),
		on:    [2]int{0, int(S_NATIONKEY)},
		filter: [2]Projection{
			nil,
			func(table *GenericTable) []int {
				return []int{
					//3,4,5,6,7,8
					int(S_SUPPKEY), int(S_ACCTBAL), int(S_NAME), int(S_ADDRESS), int(S_PHONE), int(S_COMMENT),
				}
			},
		},
	}

	psJoin := &Join{
		Query: q,
		left: &GenericTable{
			Name:      "job1",
			numFields: 3 + 6,
		},
		right: Partsupp(),
		on:    [2]int{3, int(PS_SUPPKEY)},
		filter: [2]Projection{
			nil,
			func(table *GenericTable) []int {
				return []int{
					//9,10
					int(PS_PARTKEY), int(PS_SUPPLYCOST),
				}
			},
		},
	}

	partJoin := &Join{
		Query: q,
		left: &GenericTable{
			Name:      "job2",
			numFields: 3 + 6 + 2,
		},
		right: Part(),
		on:    [2]int{9, int(P_PARTKEY)},
		filter: [2]Projection{
			nil,
			func(table *GenericTable) []int {
				if table.Get(int(P_SIZE)) == fmt.Sprintf("%d", q.Size) {
					if strings.HasSuffix(table.Get(int(P_TYPE)), q.Type) {
						return []int{
							//11,12
							int(P_PARTKEY), int(P_MFGR),
							//int(P_TYPE),int(P_SIZE),
						}
					}
				}
				return nil
			},
		},
		customReduce: func(join *Join, key string, left, right []string, emitter corral.Emitter) {
			//first find the minumum ps_supplycost
			min := math.MaxFloat64
			var minLine string
			for _, line := range left {
				err := join.left.Read(line)
				if err != nil {
					log.Errorf("%+v", err)
				}
				supplycost := join.left.GetAs(9, Float).(float64)

				if supplycost < min {
					min = supplycost
					minLine = line
				}
			}

			log.Infof("%s %f", key, min)
			fields := strings.Split(strings.TrimSpace(minLine), "|")

			for _, r := range right {
				part := strings.Split(r, "|")
				var sb strings.Builder
				sb.WriteString(fields[4]) //S_ACC
				sb.WriteRune('|')
				sb.WriteString(fields[5]) //S_Name
				sb.WriteRune('|')
				sb.WriteString(fields[1]) //N_Name
				sb.WriteRune('|')
				sb.WriteString(part[0]) //P_partkey
				key := sb.String()
				sb.WriteRune('|')

				sb.WriteString(part[1]) //P_Mfgr
				sb.WriteRune('|')
				sb.WriteString(fields[6]) //s_address
				sb.WriteRune('|')
				sb.WriteString(fields[7]) //s_phone
				sb.WriteRune('|')
				sb.WriteString(fields[8]) //s_comment
				sb.WriteRune('|')
				emitter.Emit(key, sb.String())
			}
		},
	}

	return []*corral.Job{
		corral.NewJob(nationJoin, nationJoin),
		corral.NewJob(supplierJoin, supplierJoin),
		corral.NewJob(psJoin, psJoin),
		corral.NewJob(partJoin, partJoin),
		//TODO: SORT
		//corral.NewSort(),
	}
}
