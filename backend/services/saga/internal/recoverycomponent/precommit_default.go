//go:build !reliability_component

package recoverycomponent

// PreCommitControl is an unconditional no-op in ordinary product builds.
func PreCommitControl(PreCommitEvent) {}
