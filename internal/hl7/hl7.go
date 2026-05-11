// Package hl7 provides minimal HL7 v2 helpers for educational purposes.
// Full HL7 parsing is out of scope for this demo.
package hl7

import (
	"strings"
)

// Segment represents a single HL7 v2 segment.
type Segment struct {
	Name   string
	Fields []string
}

// ParseSegment parses a single HL7 pipe-delimited segment line.
func ParseSegment(line string) Segment {
	parts := strings.Split(line, "|")
	if len(parts) == 0 {
		return Segment{}
	}
	return Segment{
		Name:   parts[0],
		Fields: parts[1:],
	}
}

// Field returns the field value at the given 1-based index, or empty string.
func (s Segment) Field(n int) string {
	if n < 1 || n > len(s.Fields) {
		return ""
	}
	return s.Fields[n-1]
}
