package utils

import (
	"github.com/zeebo/xxh3"
)

func Xxh332(key string) uint32 {
	return uint32(xxh3.HashString(key))
}

func Xxh364(key string) uint64 {
	return xxh3.HashString(key)
}
