package params

import (
	"regexp"
	"strconv"
	"time"
)

var parser = regexp.MustCompile(`^(\d+)([a-z]?)$`)

func splitNumSuffix(s string) (int, byte, bool) {
	if len(s) == 0 {
		return 0, 0, false
	}
	matches := parser.FindStringSubmatch(s)
	if matches == nil {
		return 0, 0, false
	}
	if len(matches) == 3 && matches[2] == "" {
		ret, _ := strconv.Atoi(matches[1])
		return ret, 0, true
	}
	ret, _ := strconv.Atoi(matches[1])
	return ret, matches[2][0], true
}

func ParseMaxSize(s string) (uint64, bool) {
	value, suffix, ok := splitNumSuffix(s)
	if !ok {
		return 0, false
	}
	switch suffix {
	case 'b', 0:
		return uint64(value), true
	case 'k':
		return uint64(value) * 1024, true
	case 'm':
		return uint64(value) * 1024 * 1024, true
	case 'g':
		return uint64(value) * 1024 * 1024 * 1024, true
	default:
		return 0, false
	}
}

func ParseDumpInterval(s string) (time.Duration, bool) {
	value, suffix, ok := splitNumSuffix(s)
	if !ok {
		return 0, false
	}
	switch suffix {
	case 0, 's':
		return time.Duration(value) * time.Second, true
	case 'm':
		return time.Duration(value) * time.Minute, true
	case 'h':
		return time.Duration(value) * time.Hour, true
	default:
		return 0, false
	}
}
