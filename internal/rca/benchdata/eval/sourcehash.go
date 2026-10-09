package eval

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
)

// SourceHash identifies an RCA implementation: the non-test files of
// internal/rca and the healing rules and outcome logic, under repository root.
// Every recorded result names the hash it ran against.
func SourceHash(root string) (string, error) {
	files, err := filepath.Glob(filepath.Join(root, "internal", "rca", "*.go"))
	if err != nil {
		return "", err
	}
	var names []string
	for _, f := range files {
		if !strings.HasSuffix(f, "_test.go") {
			names = append(names, f)
		}
	}
	names = append(names, filepath.Join(root, "internal", "heal", "action.go"), filepath.Join(root, "internal", "heal", "outcome.go"))
	sort.Strings(names)
	var manifest strings.Builder
	for _, f := range names {
		raw, err := os.ReadFile(f)
		if err != nil {
			return "", err
		}
		sum := sha256.Sum256(raw)
		rel, _ := filepath.Rel(root, f)
		fmt.Fprintf(&manifest, "%s  %s\n", hex.EncodeToString(sum[:]), rel)
	}
	total := sha256.Sum256([]byte(manifest.String()))
	return hex.EncodeToString(total[:])[:16], nil
}
