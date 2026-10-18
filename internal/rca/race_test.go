//go:build race

package rca

// The race detector slows this package about thirteen-fold.
const raceSlowdown = 4
