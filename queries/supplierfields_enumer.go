// Code generated by "enumer -type=supplierFields"; DO NOT EDIT.

//
package queries

import (
	"fmt"
)

const _supplierFieldsName = "S_SUPPKEYS_NAMES_ADDRESSS_NATIONKEYS_PHONES_ACCTBALS_COMMENT"

var _supplierFieldsIndex = [...]uint8{0, 9, 15, 24, 35, 42, 51, 60}

func (i supplierFields) String() string {
	if i < 0 || i >= supplierFields(len(_supplierFieldsIndex)-1) {
		return fmt.Sprintf("supplierFields(%d)", i)
	}
	return _supplierFieldsName[_supplierFieldsIndex[i]:_supplierFieldsIndex[i+1]]
}

var _supplierFieldsValues = []supplierFields{0, 1, 2, 3, 4, 5, 6}

var _supplierFieldsNameToValueMap = map[string]supplierFields{
	_supplierFieldsName[0:9]:   0,
	_supplierFieldsName[9:15]:  1,
	_supplierFieldsName[15:24]: 2,
	_supplierFieldsName[24:35]: 3,
	_supplierFieldsName[35:42]: 4,
	_supplierFieldsName[42:51]: 5,
	_supplierFieldsName[51:60]: 6,
}

// supplierFieldsString retrieves an enum value from the enum constants string name.
// Throws an error if the param is not part of the enum.
func supplierFieldsString(s string) (supplierFields, error) {
	if val, ok := _supplierFieldsNameToValueMap[s]; ok {
		return val, nil
	}
	return 0, fmt.Errorf("%s does not belong to supplierFields values", s)
}

// supplierFieldsValues returns all values of the enum
func supplierFieldsValues() []supplierFields {
	return _supplierFieldsValues
}

// IsAsupplierFields returns "true" if the value is listed in the enum definition. "false" otherwise
func (i supplierFields) IsAsupplierFields() bool {
	for _, v := range _supplierFieldsValues {
		if i == v {
			return true
		}
	}
	return false
}
