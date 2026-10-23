// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this
// file, You can obtain one at http://mozilla.org/MPL/2.0/.

package controllers

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime/schema"
	"k8s.io/apimachinery/pkg/version"
	clusterv1 "sigs.k8s.io/cluster-api/api/core/v1beta2"
	"sigs.k8s.io/cluster-api/util/contract"
	"sigs.k8s.io/controller-runtime/pkg/client"
)

// contractLookupTimeout bounds the CRD read of a conversion, which runs inside a webhook request.
const contractLookupTimeout = 5 * time.Second

// compatibleContractVersions are the Cluster API contract versions this provider works with, newest
// first: the v1beta2 contract still accepts providers that implement v1beta1.
var compatibleContractVersions = []string{clusterv1.GroupVersion.Version, "v1beta1"}

// ContractAPIVersionGetter returns the function that resolves the group and kind of a
// ContractVersionedObjectReference to a full apiVersion, as Cluster API does: the newest version the
// kind's CRD labels for the newest compatible contract. A CRD without contract labels falls back to the
// version the RESTMapper prefers.
func ContractAPIVersionGetter(reader client.Reader, mapper meta.RESTMapper) func(schema.GroupKind) (string, error) {
	return func(gk schema.GroupKind) (string, error) {
		ctx, cancel := context.WithTimeout(context.Background(), contractLookupTimeout)
		defer cancel()

		crd := &metav1.PartialObjectMetadata{}
		crd.SetGroupVersionKind(apiextensionsv1.SchemeGroupVersion.WithKind("CustomResourceDefinition"))

		crdErr := reader.Get(ctx, client.ObjectKey{Name: contract.CalculateCRDName(gk.Group, gk.Kind)}, crd)
		if crdErr == nil {
			if apiVersion, ok := latestContractAPIVersion(crd.GetLabels()); ok {
				return schema.GroupVersion{Group: gk.Group, Version: apiVersion}.String(), nil
			}
		}

		mapping, err := mapper.RESTMapping(gk)
		if err != nil {
			if crdErr != nil {
				return "", fmt.Errorf("failed to resolve the apiVersion of %s: %w (CRD: %w)", gk, err, crdErr)
			}

			return "", fmt.Errorf("failed to resolve the apiVersion of %s: %w", gk, err)
		}

		return mapping.GroupVersionKind.GroupVersion().String(), nil
	}
}

// latestContractAPIVersion returns the newest version a CRD's labels map to the newest compatible
// Cluster API contract.
func latestContractAPIVersion(crdLabels map[string]string) (string, bool) {
	for _, contractVersion := range compatibleContractVersions {
		versions := strings.Split(crdLabels[clusterv1.GroupVersion.Group+"/"+contractVersion], "_")
		versions = slices.DeleteFunc(versions, func(v string) bool { return v == "" })

		if len(versions) == 0 {
			continue
		}

		return slices.MaxFunc(versions, version.CompareKubeAwareVersionStrings), true
	}

	return "", false
}
