package types

import (
	"math"

	"github.com/google/uuid"
)

type Timestamp = uint64

const MaxTimestamp = Timestamp(math.MaxUint64)

type UniqueID uuid.UUID

func NewUniqueID() UniqueID {
	return UniqueID(uuid.New())
}

func (id UniqueID) String() string {
	return uuid.UUID(id).String()
}

func MustParse(s string) UniqueID {
	return UniqueID(uuid.MustParse(s))
}

func Parse(s string) (UniqueID, error) {
	id, err := uuid.Parse(s)
	return UniqueID(id), err
}

func NilUniqueID() UniqueID {
	return UniqueID(uuid.Nil)
}
