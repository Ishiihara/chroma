package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

const (
	SuccessLabel = "success"
	FailLabel    = "fail"
	TotalLabel   = "total"

	MetaGetLabel    = "get"
	MetaPutLabel    = "put"
	MetaRemoveLabel = "remove"
	MetaTxnLabel    = "txn"

	metaOpType      = "meta_op_type"
	chromaNamespace = "chroma"
	statusLabelName = "status"
)

var (
	buckets = prometheus.ExponentialBuckets(1, 2, 18)

	MetaKvSize = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: chromaNamespace,
			Subsystem: "meta",
			Name:      "kv_size",
			Help:      "kv size stats",
			Buckets:   buckets,
		}, []string{metaOpType})

	MetaRequestLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Namespace: chromaNamespace,
			Subsystem: "meta",
			Name:      "request_latency",
			Help:      "request latency on the client side ",
			Buckets:   buckets,
		}, []string{metaOpType})

	MetaOpCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: chromaNamespace,
			Subsystem: "meta",
			Name:      "op_count",
			Help:      "count of meta operation",
		}, []string{metaOpType, statusLabelName})
)

// RegisterMetaMetrics registers meta metrics
func RegisterMetaMetrics(registry *prometheus.Registry) {
	registry.MustRegister(MetaKvSize)
	registry.MustRegister(MetaRequestLatency)
	registry.MustRegister(MetaOpCounter)
}
