package chaos

import (
	"fmt"
	"strings"
)

// CheckContext refuses any kube context but a local kind cluster, unless that
// exact context is named in allow: a typo must never point faults at a real cluster.
func CheckContext(kubeContext, allow string) error {
	switch {
	case kubeContext == "":
		return fmt.Errorf("no kube context selected")
	case strings.HasPrefix(kubeContext, "kind-"):
		return nil
	case allow != "" && allow == kubeContext:
		return nil
	default:
		return fmt.Errorf("refusing to inject faults into kube context %q: only kind-* contexts are allowed (pass -allow-context %s to override, deliberately)", kubeContext, kubeContext)
	}
}
