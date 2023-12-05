package main

import (
	"context"
	"flag"
	"os"
	"strconv"
	"time"

	"github.com/pingcap/log"
	"go.uber.org/zap"

	"github.com/chroma/chroma-coordinator/internal/gc"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	clientset "k8s.io/client-go/kubernetes"
	"k8s.io/client-go/rest"
	"k8s.io/client-go/tools/leaderelection"
	"k8s.io/client-go/tools/leaderelection/resourcelock"
	"k8s.io/klog"
)

var (
	client *clientset.Clientset
)

func main() {
	var (
		leaseLockName      string
		leaseLockNamespace string
		podName            = os.Getenv("POD_NAME")
	)
	flag.StringVar(&leaseLockName, "lease-name", "", "Name of lease lock")
	flag.StringVar(&leaseLockNamespace, "lease-namespace", "default", "Name of lease lock namespace")
	flag.Parse()

	if leaseLockName == "" {
		klog.Fatal("missing lease-name flag")
	}
	if leaseLockNamespace == "" {
		klog.Fatal("missing lease-namespace flag")
	}

	config, err := rest.InClusterConfig()
	client = clientset.NewForConfigOrDie(config)

	if err != nil {
		klog.Fatalf("failed to get kubeconfig")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	lock := getNewLock(leaseLockName, leaseLockNamespace, podName)
	runLeaderElection(ctx, lock, podName)
}

func runLeaderElection(ctx context.Context, lock *resourcelock.LeaseLock, id string) {
	leaderelection.RunOrDie(ctx, leaderelection.LeaderElectionConfig{
		Lock:            lock,
		ReleaseOnCancel: true,
		LeaseDuration:   15 * time.Second,
		RenewDeadline:   10 * time.Second,
		RetryPeriod:     2 * time.Second,
		Callbacks: leaderelection.LeaderCallbacks{
			OnStartedLeading: func(c context.Context) {
				run(ctx)
			},
			OnStoppedLeading: func() {
				klog.Info("no longer the leader, staying inactive.")
			},
			OnNewLeader: func(current_id string) {
				if current_id == id {
					klog.Info("still the leader!")
					return
				}
				klog.Info("The new leader is ", current_id)
			},
		},
	})
}

func getNewLock(lockName string, namespace string, podName string) *resourcelock.LeaseLock {
	return &resourcelock.LeaseLock{
		LeaseMeta: metav1.ObjectMeta{
			Name:      lockName,
			Namespace: namespace,
		},
		Client: client.CoordinationV1(),
		LockConfig: resourcelock.ResourceLockConfig{
			Identity: podName,
		},
	}
}

func run(ctx context.Context) {
	klog.Info("I am the leader!")
	numberWorkers := 5
	gcInterval := 10 * time.Second
	store := gc.NewMemoryGCStateStore()
	gcProcessor := gc.NewSimpleGCProcessor(numberWorkers, store, gcInterval)
	gcProcessor.Start()
	log.Info("GC Processor started")

	eventGenerationTicker := time.NewTicker(1 * time.Second)
	segmentCounter := 0
	for {
		select {
		case <-eventGenerationTicker.C:
			status := gc.SegmentStatus_Dropping
			collectionID := "test_collection_" + strconv.Itoa(segmentCounter)
			segmentID := "test_segment_" + strconv.Itoa(segmentCounter)
			path := "test_segment_path_" + strconv.Itoa(segmentCounter)
			gcState := gc.GCState{
				CollectionID: collectionID,
				Segments: map[string]gc.SegmentState{
					segmentID: {
						SegmentID: segmentID,
						Path:      path,
						Status:    &status,
					},
				},
			}
			store.AddGCState(ctx, gcState)
			log.Info("GC event generated", zap.String("collectionID", collectionID), zap.String("segmentID", segmentID), zap.String("path", path))
			log.Info("GC event added to store", zap.Any("gcState", gcState))
			segmentCounter++
		case <-ctx.Done():
			log.Info("GC Processor is stopped")
		}
	}
}
