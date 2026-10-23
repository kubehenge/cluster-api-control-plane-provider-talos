// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1alpha3_test

import (
	"testing"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/utils/ptr"

	controlplanev1alpha3 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1alpha3"
	controlplanev1beta1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

func TestAddToSchemeRegistersEveryKind(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := controlplanev1alpha3.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	if err := controlplanev1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	for _, gvk := range []string{"TalosControlPlane", "TalosControlPlaneList"} {
		if !scheme.Recognizes(controlplanev1alpha3.GroupVersion.WithKind(gvk)) {
			t.Fatalf("v1alpha3 %s is not registered", gvk)
		}
	}

	for _, gvk := range []string{"TalosControlPlane", "TalosControlPlaneList", "TalosControlPlaneTemplate", "TalosControlPlaneTemplateList"} {
		if !scheme.Recognizes(controlplanev1beta1.GroupVersion.WithKind(gvk)) {
			t.Fatalf("v1beta1 %s is not registered", gvk)
		}
	}
}

// The scheme converts through the hand-written status conversion, which moves the v1beta1 contract
// failure fields into the deprecated block of the hub.
func TestAddToSchemeRegistersTheConversions(t *testing.T) {
	scheme := runtime.NewScheme()
	if err := controlplanev1alpha3.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	if err := controlplanev1beta1.AddToScheme(scheme); err != nil {
		t.Fatal(err)
	}

	spoke := &controlplanev1alpha3.TalosControlPlane{
		Spec:   controlplanev1alpha3.TalosControlPlaneSpec{Version: "v1.36.0"},
		Status: controlplanev1alpha3.TalosControlPlaneStatus{FailureMessage: ptr.To("message")},
	}

	hub := &controlplanev1beta1.TalosControlPlane{}
	if err := scheme.Convert(spoke, hub, nil); err != nil {
		t.Fatal(err)
	}

	if hub.Spec.Version != "v1.36.0" {
		t.Fatalf("version = %q, want v1.36.0", hub.Spec.Version)
	}

	if hub.Status.Deprecated == nil || hub.Status.Deprecated.V1Beta1 == nil || hub.Status.Deprecated.V1Beta1.FailureMessage == nil || *hub.Status.Deprecated.V1Beta1.FailureMessage != "message" {
		t.Fatalf("deprecated status = %+v, want the failure message", hub.Status.Deprecated)
	}
}
