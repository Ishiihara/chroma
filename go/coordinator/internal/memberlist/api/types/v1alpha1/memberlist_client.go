package v1alpha1

import (
	"context"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/watch"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

type MemberListInterface interface {
	List(opts metav1.ListOptions) (*MemberListList, error)
	Get(name string, options metav1.GetOptions) (*MemberList, error)
	Create(*MemberList) (*MemberList, error)
	Watch(opts metav1.ListOptions) (watch.Interface, error)
}

type memberListClient struct {
	restClient rest.Interface
	ns         string
}

func (c *memberListClient) List(opts metav1.ListOptions) (*MemberListList, error) {
	result := MemberListList{}
	err := c.restClient.
		Get().
		Namespace(c.ns).
		Resource("projects").
		VersionedParams(&opts, scheme.ParameterCodec).
		Do(context.Background()).
		Into(&result)

	return &result, err
}

func (c *memberListClient) Get(name string, opts metav1.GetOptions) (*MemberList, error) {
	result := MemberList{}
	err := c.restClient.
		Get().
		Namespace(c.ns).
		Resource("projects").
		Name(name).
		VersionedParams(&opts, scheme.ParameterCodec).
		Do(context.Background()).
		Into(&result)

	return &result, err
}

func (c *memberListClient) Create(memberList *MemberList) (*MemberList, error) {
	result := MemberList{}
	err := c.restClient.
		Post().
		Namespace(c.ns).
		Resource("projects").
		Body(memberList).
		Do(context.Background()).
		Into(&result)

	return &result, err
}

func (c *memberListClient) Watch(opts metav1.ListOptions) (watch.Interface, error) {
	opts.Watch = true
	return c.restClient.
		Get().
		Namespace(c.ns).
		Resource("projects").
		VersionedParams(&opts, scheme.ParameterCodec).
		Watch(context.Background())
}
