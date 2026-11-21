//go:build !reliability_component && !reliability_e2e

package recoverycomponent

// PostCommit is compiled as an inert no-op in ordinary product builds. No
// environment variable can enable the component observer without one of the
// explicit reliability build tags.
func PostCommit(PostCommitEvent) {}
