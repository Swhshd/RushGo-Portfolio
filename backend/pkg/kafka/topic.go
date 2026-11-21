package kafka

import (
	"errors"
	"regexp"
)

var t23TopicSuffixPattern = regexp.MustCompile(`^[a-f0-9]{24}$`)

// candidateKafkaTopic isolates acceptance-mode records in a campaign-specific
// topic. Normal runtime topics remain byte-for-byte unchanged when the private
// reliability mode is disabled.
func candidateKafkaTopic(base, suffix string, enabled bool) (string, error) {
	if base == "" || len(base) > 249 {
		return "", errors.New("invalid Kafka topic")
	}
	if !enabled {
		return base, nil
	}
	if !t23TopicSuffixPattern.MatchString(suffix) || len(base)+5+len(suffix) > 249 {
		return "", errors.New("invalid candidate Kafka topic suffix")
	}
	return base + ".t23-" + suffix, nil
}
