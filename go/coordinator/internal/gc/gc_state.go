package gc

type GCState struct {
	CollectionID string
	Segments     map[string]SegmentState
}

type SegmentStatus int32

const (
	// SegmentStatus_Dropping indicates the segment is being dropped.
	SegmentStatus_Dropping SegmentStatus = 0
	// SegmentStatus_Dropped indicates the segment is dropped.
	SegmentStatus_Dropped SegmentStatus = 1
)

type SegmentState struct {
	SegmentID string
	Path      string
	Status    *SegmentStatus
}
