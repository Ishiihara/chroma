package v1alpha1

import (
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/client-go/kubernetes/scheme"
	"k8s.io/client-go/rest"
)

type MemberListV1Alpha1Interface interface {
	MemberListList(namespace string) MemberListInterface
}

type MemberListV1Alpha1Client struct {
	restClient rest.Interface
}

func NewForConfig(c *rest.Config) (*MemberListV1Alpha1Client, error) {
	config := *c
	config.ContentConfig.GroupVersion = &schema.GroupVersion{Group: GroupName, Version: GroupVersion}
	config.APIPath = "/apis"
	config.NegotiatedSerializer = scheme.Codecs.WithoutConversion()
	config.UserAgent = rest.DefaultKubernetesUserAgent()

	client, err := rest.RESTClientFor(&config)
	if err != nil {
		return nil, err
	}

	return &MemberListV1Alpha1Client{restClient: client}, nil
}

func (c *MemberListV1Alpha1Client) MemberListList(namespace string) MemberListInterface {
	return &memberListClient{
		restClient: c.restClient,
		ns:         namespace,
	}
}
