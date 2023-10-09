package coordinator

import "errors"

var ErrCollectionExists = errors.New("collection already exists")
var ErrCollectionNotFound = errors.New("collection not found")
var ErrCollectionNotExists = errors.New("collection does not exist")
