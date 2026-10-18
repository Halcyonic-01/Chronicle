// Package revision identifies what a Deployment's pods are made from.
//
// A rollback must act on exactly the revision a decision was about. The first
// container's image is not that: a release that changes an environment variable,
// a resource limit or a second container leaves it unchanged. TemplateHash covers
// the whole pod template, so any change to what the pods run changes it.
package revision

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"

	corev1 "k8s.io/api/core/v1"
)

// ControllerLabel is added to a ReplicaSet's pod template by the Deployment
// controller. It is a product of the template, not part of it.
const ControllerLabel = "pod-template-hash"

// TemplateHash fingerprints a whole pod template. A Deployment's template and the
// template of the ReplicaSet it creates from it hash the same, because the
// controller's own label is left out.
func TemplateHash(t corev1.PodTemplateSpec) string {
	c := t.DeepCopy()
	delete(c.Labels, ControllerLabel)
	raw, err := json.Marshal(c) // maps marshal in sorted key order: the bytes are stable
	if err != nil {
		return ""
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])[:16]
}

// Template returns a copy of a ReplicaSet's template as it should be written
// back to the Deployment: without the controller's label.
func Template(t corev1.PodTemplateSpec) corev1.PodTemplateSpec {
	c := t.DeepCopy()
	delete(c.Labels, ControllerLabel)
	return *c
}
