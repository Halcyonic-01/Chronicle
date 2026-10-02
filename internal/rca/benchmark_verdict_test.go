package rca

// An analyzer that can declare "no root cause" explicitly is measured by that
// declaration. The original analyzer could only return nothing.
func init() {
	declaredNoCause = func(r *Result) bool { return r.Verdict == VerdictNoRootCause }
}
