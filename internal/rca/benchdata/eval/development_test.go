package eval

import (
	"testing"
)

// The development and held-out sets, scored with the shared evaluation. These
// are the sets the rules were developed against; they are not independent.
func TestDevelopmentAndHeldOutReport(t *testing.T) {
	for _, set := range []struct {
		name  string
		cases []Case
	}{{"DEVELOPMENT (main) set", Development("improved")}, {"HELD-OUT set", HeldOut("improved")}} {
		rows, err := RunAll(set.cases, Options{})
		if err != nil {
			t.Fatal(err)
		}
		t.Log("\n" + Summarize(rows, false).Format(set.name+"  [not independent: used while developing the rules]"))
	}
}
