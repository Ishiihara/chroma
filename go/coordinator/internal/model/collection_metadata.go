package model

type MetadataValueType interface {
	IsMetadataValueType()
}

type MetadataValueStringType struct {
	Value string
}

func (s *MetadataValueStringType) IsMetadataValueType() {}

type MetadataValueInt64Type struct {
	Value int64
}

func (s *MetadataValueInt64Type) IsMetadataValueType() {}

type MetadataValueFloat64Type struct {
	Value float64
}

func (s *MetadataValueFloat64Type) IsMetadataValueType() {}

type CollectionMetadata[T MetadataValueType] struct {
	Metadata map[string]T
}

func NewCollectionMetadata[T MetadataValueType]() *CollectionMetadata[T] {
	return &CollectionMetadata[T]{
		Metadata: make(map[string]T),
	}
}

func (m *CollectionMetadata[T]) Add(key string, value T) {
	m.Metadata[key] = value
}

func (m *CollectionMetadata[T]) Get(key string) T {
	return m.Metadata[key]
}

func (m *CollectionMetadata[T]) Remove(key string) {
	delete(m.Metadata, key)
}
