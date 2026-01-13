package dynamo

import (
	tupleUtils "github.com/openfga/openfga/pkg/tuple"
)

type indexType string
type indexValue string

type Filter struct {
	Object   string
	Relation string
	User     string
}

// This function works to build the index dynamically instead of applying filters one by one
// just works for dynamodb
func BuildIndex(store string, filter Filter) map[indexType]indexValue {

	objectType, objectID := tupleUtils.SplitObject(filter.Object)

	index := map[indexType]indexValue{}


	if filter.Object != "" &&
		filter.Relation != "" &&
		filter.User != "" &&
	objectID != "" {

		index[indexType("PK")] = indexValue(createPK(store, filter.User, filter.Object, filter.Relation))
		index[indexType("SK")] = indexValue(filter.User)

		return index
	}

	if filter.User != "" && filter.Object != "" && filter.Relation != "" {
		index[indexType("GSI4PK")] = indexValue(createGSI4PK(store, filter.Object, filter.Relation))
		return index
	}

	if filter.User != "" && objectType != ""  {
		index[indexType("GSI2PK")] = indexValue(createGSI2PK(store, filter.User, filter.Object))
		return index
	}

	if filter.Object != "" {
		index[indexType("GSI1PK")] = indexValue(createGSI1PK(store, filter.Object))
		return index
	}

	index[indexType("GSI3PK")] = indexValue(createGSI3PK(store))

	return index
}
