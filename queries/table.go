package queries

import (
	"fmt"
	"math/rand"
	"strings"
)

var Syllable3 = [...]string{"CASE", "BOX", "BAG", "PKG", "PACK", "CAN", "DRUM"}

type Filter func(TableEntry) bool
type Convert func(val string) interface{}

type TableEntry interface {
	Read(line string) error

	LookupAs(filed string, conv Convert) (interface{}, error)
	Lookup(filed string) (string, error)

	Get(filed int) string
	GetAs(filed int, conv Convert) interface{}

	Select(fields ...string) ([]string, error)
	SelectWithMask(mask ...int) ([]string, error)
}

func RandomContainerName() string {
	var Syllable1 = [...]string{"SM", "LG", "MED", "JUMBO", "WRAP"}
	var Syllable2 = [...]string{"CASE", "BOX", "BAG", "JAR", "PKG", "PACK", "CAN", "DRUM"}

	i := rand.Intn(len(Syllable1))
	j := rand.Intn(len(Syllable2))

	return fmt.Sprintf("%s %s", Syllable1[i], Syllable2[j])
}

func RandomType() string {
	var Syllable3 = [...]string{"TIN", "NICKEL", "BRASS", "STEEL", "COPPER"}
	i := rand.Intn(len(Syllable3))

	return Syllable3[i]
}

func RandomRegion() string {
	var Regions = [...]string{"AFRICA", "AMERICA", "ASIA", "EUROPE", "MIDDLE EAST"}
	i := rand.Intn(len(Regions))
	return Regions[i]
}

type GenericTable struct {
	Name      string
	raw       []string
	numFields int
	lookup    func(string) (int, error)
}

func (l *GenericTable) LookupAs(filed string, conv Convert) (interface{}, error) {
	raw, err := l.Lookup(filed)
	if err != nil {
		return nil, err
	}
	return conv(raw), nil
}

func (l *GenericTable) Lookup(filed string) (string, error) {
	f, err := l.lookup(filed)
	if err != nil {
		return "", err
	}
	return l.raw[f], nil
}

func (l *GenericTable) GetAs(filed int, conv Convert) interface{} {
	return conv(l.Get(filed))
}

func (l *GenericTable) Get(filed int) string {
	return l.raw[filed]
}

func (l *GenericTable) Read(line string) error {
	data := strings.Split(line, "|")
	if len(data) < l.numFields {
		return fmt.Errorf("line dose not meet expected format")
	}

	l.raw = data
	return nil
}

func (l *GenericTable) Select(fields ...string) ([]string, error) {
	mask := make([]int, len(fields))
	for i := 0; i < len(fields); i++ {
		f, err := l.lookup(fields[i])
		if err != nil {
			return nil, err
		}
		mask[i] = f
	}
	return l.SelectWithMask(mask...)
}

func (l *GenericTable) SelectWithMask(mask ...int) ([]string, error) {
	return maskSelect(mask, l.raw)
}

func maskSelect(mask []int, data []string) ([]string, error) {
	result := make([]string, len(mask))
	for i := 0; i < len(mask); i++ {
		if mask[i] >= 0 && mask[i] < len(data) {
			result[i] = data[mask[i]]
		} else {
			return nil, fmt.Errorf("mask[%d] out of bounds", i)
		}
	}
	return result, nil
}

//go:generate enumer -type=lineItemFields
type lineItemFields int

const (
	L_ORDERKEY lineItemFields = iota
	L_PARTKEY
	L_SUPPKEY
	L_LINENUMBER
	L_QUANTITY
	L_EXTENDEDPRICE
	L_DISCOUNT
	L_TAX
	L_RETURNFLAG
	L_LINESTATUS
	L_SHIPDATE
	L_COMMITDATE
	L_RECEIPTDATE
	L_SHIPINSTRUCT
	L_SHIPMODE
	L_COMMENT
)

func LineItem() *GenericTable {
	return &GenericTable{
		Name:      "lineitem",
		numFields: 15,
		lookup:    func(s string) (int, error) { val, err := lineItemFieldsString(s); return int(val), err },
	}
}

//go:generate enumer -type=partFields
type partFields int

const (
	P_PARTKEY partFields = iota
	P_NAME
	P_MFGR
	P_BRAND
	P_TYPE
	P_SIZE
	P_CONTAINER
	P_RETAILPRICE
	P_COMMENT
)

func Part() *GenericTable {
	return &GenericTable{
		Name:      "part",
		numFields: 9,
		lookup:    func(s string) (int, error) { val, err := partFieldsString(s); return int(val), err },
	}
}

//go:generate enumer -type=supplierFields
type supplierFields int

const (
	S_SUPPKEY supplierFields = iota
	S_NAME
	S_ADDRESS
	S_NATIONKEY
	S_PHONE
	S_ACCTBAL
	S_COMMENT
)

func Supplier() *GenericTable {
	return &GenericTable{
		Name:      "supplier",
		numFields: 7,
		lookup:    func(s string) (int, error) { val, err := supplierFieldsString(s); return int(val), err },
	}
}

//go:generate enumer -type=partsuppFields
type partsuppFields int

const (
	PS_PARTKEY partsuppFields = iota
	PS_SUPPKEY
	PS_AVAILQTY
	PS_SUPPLYCOST
	PS_COMMENT
)

func Partsupp() *GenericTable {
	return &GenericTable{
		Name:      "partsupp",
		numFields: 5,
		lookup:    func(s string) (int, error) { val, err := partsuppFieldsString(s); return int(val), err },
	}
}

//go:generate enumer -type=nationFields
type nationFields int

const (
	N_NATIONKEY nationFields = iota
	N_NAME
	N_REGIONKEY
	N_COMMENT
)

func Nation() *GenericTable {
	return &GenericTable{
		Name:      "nation",
		numFields: 4,
		lookup:    func(s string) (int, error) { val, err := nationFieldsString(s); return int(val), err },
	}
}

//go:generate enumer -type=regionFields
type regionFields int

const (
	R_REGIONKEY regionFields = iota
	R_NAME
	R_COMMENT
)

func Region() *GenericTable {
	return &GenericTable{
		Name:      "region",
		numFields: 3,
		lookup:    func(s string) (int, error) { val, err := regionFieldsString(s); return int(val), err },
	}
}

//go:generate enumer -type=orderFields
type orderFields int

const (
	O_ORDERKEY orderFields = iota
	O_CUSTKEY
	O_ORDERSTATUS
	O_TOTALPRICE
	O_ORDERDATE
	O_ORDERPRIORITY
	O_CLERK
	O_SHIPPRIORITY
	O_COMMENT
)

func Order() *GenericTable {
	return &GenericTable{
		Name:      "order",
		numFields: 9,
		lookup:    func(s string) (int, error) { val, err := orderFieldsString(s); return int(val), err },
	}
}

//go:generate enumer -type=customerFields
type customerFields int

const (
	C_CUSTKEY customerFields = iota
	C_NAME
	C_ADDRESS
	C_NATIONKEY
	C_PHONE
	C_ACCTBAL
	C_MKTSEGMENT
	C_COMMENT
)

func Customer() *GenericTable {
	return &GenericTable{
		Name:      "customer",
		numFields: 9,
		lookup:    func(s string) (int, error) { val, err := customerFieldsString(s); return int(val), err },
	}
}
