package model

type SegmentMetadataValueType interface {
	IsSegmentMetadataValueType()
}

type SegmentMetadataValueStringType struct {
	Value string
}

func (s *SegmentMetadataValueStringType) IsSegmentMetadataValueType() {}

type SegmentMetadataValueInt64Type struct {
	Value int64
}

func (s *SegmentMetadataValueInt64Type) IsSegmentMetadataValueType() {}

type SegmentMetadataValueFloat64Type struct {
	Value float64
}

func (s *SegmentMetadataValueFloat64Type) IsSegmentMetadataValueType() {}

type SegmentMetadata[T SegmentMetadataValueType] struct {
	Metadata map[string]T
}

func NewSegmentMetadata[T SegmentMetadataValueType]() *SegmentMetadata[T] {
	return &SegmentMetadata[T]{
		Metadata: make(map[string]T),
	}
}

func (m *SegmentMetadata[T]) Add(key string, value T) {
	m.Metadata[key] = value
}

func (m *SegmentMetadata[T]) Get(key string) T {
	return m.Metadata[key]
}

func (m *SegmentMetadata[T]) Remove(key string) {
	delete(m.Metadata, key)
}
