// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package v1alpha3

import (
	"errors"
	"testing"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"

	cpv1beta1 "github.com/siderolabs/cluster-api-control-plane-provider-talos/api/v1beta1"
)

func TestConvertFromWithoutInfrastructureAPIVersion(t *testing.T) {
	getter := apiVersionGetter
	t.Cleanup(func() { SetAPIVersionGetter(getter) })

	// the RESTMapper no longer knows the kind, as when the infrastructure provider is removed first
	SetAPIVersionGetter(func(gk schema.GroupKind) (string, error) {
		return "", errors.New("no matches for kind \"DockerMachineTemplate\" in group \"infrastructure.cluster.x-k8s.io\"")
	})

	hub := &cpv1beta1.TalosControlPlane{
		ObjectMeta: metav1.ObjectMeta{
			Name:      "cp",
			Namespace: "default",
		},
		Spec: cpv1beta1.TalosControlPlaneSpec{
			Version: "v1.31.0",
			MachineTemplate: cpv1beta1.TalosControlPlaneMachineTemplate{
				Spec: cpv1beta1.TalosControlPlaneMachineTemplateSpec{
					InfrastructureRef: clusterv1.ContractVersionedObjectReference{
						APIGroup: "infrastructure.cluster.x-k8s.io",
						Kind:     "DockerMachineTemplate",
						Name:     "cp-template",
					},
				},
			},
		},
	}

	spoke := &TalosControlPlane{}
	if err := spoke.ConvertFrom(hub.DeepCopy()); err != nil {
		t.Fatalf("expected the conversion to v1alpha3 to succeed, got %v", err)
	}

	if got := spoke.Spec.InfrastructureTemplate; got.APIVersion != "infrastructure.cluster.x-k8s.io" || got.Kind != "DockerMachineTemplate" || got.Name != "cp-template" {
		t.Fatalf("expected the infrastructure template to keep its group, kind and name, got %+v", got)
	}

	restored := &cpv1beta1.TalosControlPlane{}
	if err := spoke.ConvertTo(restored); err != nil {
		t.Fatalf("expected the conversion back to v1beta1 to succeed, got %v", err)
	}

	if got := restored.Spec.MachineTemplate.Spec.InfrastructureRef; got != hub.Spec.MachineTemplate.Spec.InfrastructureRef {
		t.Fatalf("expected the infrastructure reference to survive the round trip, got %+v", got)
	}
}
