package rca

import (
	"time"

	"github.com/Halcyonic-01/Chronicle/internal/event"
)

// Config holds every tunable of the ranking, so a change to one can be measured
// and tested instead of edited in place. The zero value is not useful; start
// from DefaultConfig. The ranking stays deterministic: a Config only chooses the
// numbers, never the method.
type Config struct {
	// TypeWeights is the base score of an event type, and DefaultWeight the base
	// of any type not listed.
	TypeWeights   map[string]float64
	DefaultWeight float64

	// Lookback is how far before a symptom its causes are searched, per symptom
	// type; DefaultLookback applies to the rest.
	Lookback        map[string]time.Duration
	DefaultLookback time.Duration

	// DecayDivisor is how many e-folds of time decay fit across the lookback.
	DecayDivisor float64
	// DistanceSlope is the penalty per graph hop: 1 / (1 + hops*slope).
	DistanceSlope float64
	// BlastMax and BlastSaturation shape the blast-radius bonus:
	// 1 + BlastMax*n/(n+BlastSaturation).
	BlastMax, BlastSaturation float64

	// PropagationFactor demotes an observation that something upstream explains;
	// RemediationFactor demotes a change that undoes an earlier one.
	PropagationFactor, RemediationFactor float64

	// PropagationHorizon is how long a cause of a given type may take to
	// surface downstream, and DefaultHorizon the horizon of the rest.
	PropagationHorizon map[string]time.Duration
	DefaultHorizon     time.Duration

	// Episode following; see episodeOnset.
	EpisodeGap, LogEpisodeGap, RestartGap time.Duration
	FlapWindow, MaxEpisode                time.Duration
	// SettleAfterChange is how long a replacing change needs before the change it
	// replaced stops mattering.
	SettleAfterChange time.Duration

	// InconclusiveBelow is the confidence under which a real candidate is called
	// inconclusive.
	InconclusiveBelow float64
	// AmbiguityRatio: a rival root-class cause scoring at least this fraction of
	// the leader's score is a plausible alternative, and the verdict is
	// ambiguous. Chosen as "within 20%" by reasoning, not fitted to a benchmark.
	AmbiguityRatio float64

	// NonRootFactor damps what the evidence says is not the current cause: a
	// capacity increase, or a change whose incident already recovered.
	NonRootFactor float64
	// UncorroboratedFactor damps a change that explains none of the observed
	// failures while failures it cannot explain were seen.
	UncorroboratedFactor float64
	// TerminationGrace: a pod going unready this close before its deletion is
	// being shut down (Kubernetes' default grace period), not failing.
	TerminationGrace time.Duration
	// NewPodWindow: a failure this soon after a controller created the pod is
	// an effect of whatever made the controller act (a rollout's horizon).
	NewPodWindow time.Duration
}

// DefaultConfig returns the shipped values. Every field copies what the package
// constants and tables hold, so nothing changes unless a caller edits the copy.
func DefaultConfig() *Config {
	weights := make(map[string]float64, len(typeWeight))
	for k, v := range typeWeight {
		weights[k] = v
	}
	lookbacks := make(map[string]time.Duration, len(lookback))
	for k, v := range lookback {
		lookbacks[k] = v
	}
	horizons := make(map[string]time.Duration, len(propagationHorizon))
	for k, v := range propagationHorizon {
		horizons[k] = v
	}
	return &Config{
		TypeWeights: weights, DefaultWeight: 0.20,
		Lookback: lookbacks, DefaultLookback: 15 * time.Minute,
		DecayDivisor: defaultDecayDivisor, DistanceSlope: 0.4,
		BlastMax: 0.25, BlastSaturation: 10,
		PropagationFactor: propagationFactor, RemediationFactor: remediationFactor,
		PropagationHorizon: horizons, DefaultHorizon: defaultPropagationHorizon,
		EpisodeGap: episodeGap, LogEpisodeGap: logEpisodeGap, RestartGap: restartGap,
		FlapWindow: flapWindow, MaxEpisode: maxEpisode, SettleAfterChange: settleAfterChange,
		InconclusiveBelow: 0.5, AmbiguityRatio: 0.8,
		// Chosen a priori: as weak as propagation, and "half as plausible".
		NonRootFactor: propagationFactor, UncorroboratedFactor: 0.5,
		TerminationGrace: 30 * time.Second, NewPodWindow: 15 * time.Minute,
	}
}

var defaultConfig = DefaultConfig()

func (cfg *Config) weight(eventType string) float64 {
	if w, ok := cfg.TypeWeights[eventType]; ok && w != 0 {
		return w
	}
	return cfg.DefaultWeight
}

func (cfg *Config) lookbackFor(symptomType string) time.Duration {
	if d := cfg.Lookback[symptomType]; d != 0 {
		return d
	}
	return cfg.DefaultLookback
}

func (a *Analyzer) config() *Config {
	if a.Config != nil {
		return a.Config
	}
	return defaultConfig
}

// The package-level names below delegate to the defaults; they keep the
// functions the tests call directly working unchanged.
func score(c *Candidate, symptom event.Event, onset time.Time, timeConstant float64) {
	defaultConfig.score(c, symptom, onset, timeConstant)
}
func confidence(c []Candidate) (overall, strength, separation float64) {
	return defaultConfig.confidence(c)
}
func capRemediations(c []Candidate) { defaultConfig.capRemediations(c) }
func dampPropagation(c []Candidate, reaches func(from, to string) bool, window time.Duration) {
	defaultConfig.dampPropagation(c, reaches, window)
}
func linkChains(c []Candidate, reaches func(from, to string) bool) {
	defaultConfig.linkChains(c, reaches)
}
func verdictOf(r *Result) string { return defaultConfig.verdict(r) }
