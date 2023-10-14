package v1alpha1

import (
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
)

// MemberListSpec defines the desired state of MemberList
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type MemberListSpec struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:",inline"`
	Members           []Member `json:"members"`
}

// Member defines a member in the MemberList
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type Member struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:",inline"`

	URL string `json:"url" pattern:"^(https?:\\/\\/)?([\\da-z\\.-]+)\\.([a-z\\.]{2,6})([\\/\\w \\.-]*)*\\/?$"`
}

// MemberList is the Schema for the memberlists API
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type MemberList struct {
	metav1.TypeMeta   `json:",inline"`
	metav1.ObjectMeta `json:"metadata,omitempty"`

	Spec MemberListSpec `json:"spec,omitempty"`
}

// MemberListList contains a list of MemberList
// +k8s:deepcopy-gen:interfaces=k8s.io/apimachinery/pkg/runtime.Object
type MemberListList struct {
	metav1.TypeMeta `json:",inline"`
	metav1.ListMeta `json:"metadata,omitempty"`

	Items []MemberList `json:"items"`
}
