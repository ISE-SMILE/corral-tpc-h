package queries

import (
	"fmt"
	"strings"
)


type Filter func(TableEntry) bool
type Convert func(val string) interface{}

type TableEntry interface {
	Read(line string) error

	GetAs(filed string,conv Convert) (interface{},error)
	Get(filed string) (string,error)

	Select(fields ...string) ([]string,error)
	SelectWithMask(mask ...int) ([]string,error)
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

type LineItem struct {
	raw []string
}

func (l *LineItem) GetAs(filed string, conv Convert) (interface{},error) {
	raw,err := l.Get(filed)
	if err != nil {
		return nil,err
	}
	return conv(raw),nil
}

func (l *LineItem) Get(filed string) (string,error) {
	f,err := lineItemFieldsString(filed)
	if err != nil {
		return "",err
	}
	return l.raw[f],nil
}

func (l *LineItem) Read(line string) error {
	data := strings.Split(line, "|")
	if len(data) < 15 {
		return fmt.Errorf("line dose not meet expected format")
	}

	l.raw = data
	return nil
}

func (l *LineItem) Select(fields ...string) ([]string,error) {
	result := make([]string,len(fields))
	for i := 0; i < len(fields); i++ {
		f,err := lineItemFieldsString(fields[i])
		if err != nil {
			return nil,err
		}
		result[i] = l.raw[f]
	}
	return result,nil
}

func (l *LineItem) SelectWithMask(mask ...int) ([]string,error) {
	result := make([]string,len(mask))
	for i := 0; i < len(mask); i++ {
		if mask[i] >= 0 && mask[i] < len(l.raw) {
			result[i] = l.raw[mask[i]]
		} else {
			return nil,fmt.Errorf("mask[%d] out of bounds",i)
		}
	}
	return result,nil
}