package main

import (
	"time"

	"github.com/chroma/chroma-coordinator/internal/memberlist/api/types/v1alpha1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/tools/cache"
)

func WatchResources(clientSet v1alpha1.MemberListV1Alpha1Interface) cache.Store {
	memberListStore, memberListController := cache.NewInformer(
		&cache.ListWatch{
			ListFunc: func(lo metav1.ListOptions) (result runtime.Object, err error) {
				return clientSet.MemberListList("some-namespace").List(lo)
			},
			WatchFunc: func(lo metav1.ListOptions) (watch.Interface, error) {
				return clientSet.MemberListList("some-namespace").Watch(lo)
			},
		},
		&v1alpha1.MemberList{},
		1*time.Minute,
		cache.ResourceEventHandlerFuncs{},
	)

	go memberListController.Run(wait.NeverStop)
	return memberListStore
}
