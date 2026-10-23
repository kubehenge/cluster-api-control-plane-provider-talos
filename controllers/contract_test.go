// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"testing"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestLatestContractAPIVersion(t *testing.T) {
	for _, tt := range []struct {
		name   string
		labels map[string]string
		want   string
	}{
		{name: "no labels"},
		{name: "v1beta2 contract", labels: map[string]string{"cluster.x-k8s.io/v1beta2": "v1alpha1_v1alpha2"}, want: "v1alpha2"},
		{name: "kube-aware order", labels: map[string]string{"cluster.x-k8s.io/v1beta2": "v1beta1_v1alpha9_v1"}, want: "v1"},
		{
			name: "newest contract first",
			labels: map[string]string{
				"cluster.x-k8s.io/v1beta1": "v1alpha3",
				"cluster.x-k8s.io/v1beta2": "v1alpha2",
			},
			want: "v1alpha2",
		},
		{name: "v1beta1 contract", labels: map[string]string{"cluster.x-k8s.io/v1beta1": "v1alpha3_v1alpha4"}, want: "v1alpha4"},
		{name: "empty label", labels: map[string]string{"cluster.x-k8s.io/v1beta2": "", "cluster.x-k8s.io/v1beta1": "v1alpha1"}, want: "v1alpha1"},
		{name: "unsupported contract", labels: map[string]string{"cluster.x-k8s.io/v1alpha4": "v1alpha1"}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := latestContractAPIVersion(tt.labels)
			if ok != (tt.want != "") || got != tt.want {
				t.Fatalf("latestContractAPIVersion() = %q, %t, want %q", got, ok, tt.want)
			}
		})
	}
}

func TestContractAPIVersionGetter(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := apiextensionsv1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	labelled := schema.GroupKind{Group: "infrastructure.cluster.x-k8s.io", Kind: "ProxmoxMachineTemplate"}
	unlabelled := schema.GroupKind{Group: "infrastructure.example.com", Kind: "ExampleMachineTemplate"}
	unknown := schema.GroupKind{Group: "infrastructure.example.com", Kind: "GoneMachineTemplate"}

	crd := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{
			Name:   "proxmoxmachinetemplates.infrastructure.cluster.x-k8s.io",
			Labels: map[string]string{"cluster.x-k8s.io/v1beta2": "v1alpha1_v1alpha2"},
		},
	}
	unlabelledCRD := &apiextensionsv1.CustomResourceDefinition{
		ObjectMeta: metav1.ObjectMeta{Name: "examplemachinetemplates.infrastructure.example.com"},
	}

	mapper := meta.NewDefaultRESTMapper([]schema.GroupVersion{{Group: labelled.Group, Version: "v1alpha1"}, {Group: unlabelled.Group, Version: "v1"}})
	mapper.Add(schema.GroupVersionKind{Group: labelled.Group, Version: "v1alpha1", Kind: labelled.Kind}, meta.RESTScopeNamespace)
	mapper.Add(schema.GroupVersionKind{Group: unlabelled.Group, Version: "v1", Kind: unlabelled.Kind}, meta.RESTScopeNamespace)

	getter := ContractAPIVersionGetter(fake.NewClientBuilder().WithScheme(scheme).WithObjects(crd, unlabelledCRD).Build(), mapper)

	for _, tt := range []struct {
		gk      schema.GroupKind
		want    string
		wantErr bool
	}{
		{gk: labelled, want: "infrastructure.cluster.x-k8s.io/v1alpha2"},
		{gk: unlabelled, want: "infrastructure.example.com/v1"},
		{gk: unknown, wantErr: true},
	} {
		got, err := getter(tt.gk)
		if (err != nil) != tt.wantErr || got != tt.want {
			t.Fatalf("getter(%s) = %q, %v, want %q (error %t)", tt.gk, got, err, tt.want, tt.wantErr)
		}
	}
}
