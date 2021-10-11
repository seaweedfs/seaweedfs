package s3api

import (
	"encoding/xml"
	"time"
)

// Status represents lifecycle configuration status
type ruleStatus string

// Supported status types
const (
	Enabled  ruleStatus = "Enabled"
	Disabled ruleStatus = "Disabled"
)

// Lifecycle - Configuration for bucket lifecycle.
type Lifecycle struct {
	XMLName xml.Name `xml:"LifecycleConfiguration"`
	Rules   []Rule   `xml:"Rule"`
}

// Rule - a rule for lifecycle configuration.
type Rule struct {
	XMLName    xml.Name   `xml:"Rule"`
	ID         string     `xml:"ID,omitempty"`
	Status     ruleStatus `xml:"Status"`
	Filter     Filter     `xml:"Filter,omitempty"`
	Prefix     Prefix     `xml:"Prefix,omitempty"`
	Expiration Expiration `xml:"Expiration,omitempty"`
	Transition Transition `xml:"Transition,omitempty"`
}

// Filter - a filter for a lifecycle configuration Rule.
type Filter struct {
	XMLName xml.Name `xml:"Filter"`
	set     bool

	Prefix Prefix

	And    And
	andSet bool

	Tag    Tag
	tagSet bool
}

// Prefix holds the prefix xml tag in <Rule> and <Filter>
type Prefix struct {
	string
	set bool
}

// And - a tag to combine a prefix and multiple tags for lifecycle configuration rule.
type And struct {
	XMLName xml.Name `xml:"And"`
	Prefix  Prefix   `xml:"Prefix,omitempty"`
	Tags    []Tag    `xml:"Tag,omitempty"`
}

// Expiration - expiration actions for a rule in lifecycle configuration.
type Expiration struct {
	XMLName      xml.Name           `xml:"Expiration"`
	Days         int                `xml:"Days,omitempty"`
	Date         time.Time          `xml:"Date,omitempty"`
	DeleteMarker ExpireDeleteMarker `xml:"ExpiredObjectDeleteMarker"`

	set bool
}

// ExpireDeleteMarker represents value of ExpiredObjectDeleteMarker field in Expiration XML element.
type ExpireDeleteMarker struct {
	val bool
	set bool
}

// Transition - transition actions for a rule in lifecycle configuration.
type Transition struct {
	XMLName      xml.Name  `xml:"Transition"`
	Days         int       `xml:"Days,omitempty"`
	Date         time.Time `xml:"Date,omitempty"`
	StorageClass string    `xml:"StorageClass,omitempty"`

	set bool
}

// TransitionDays is a type alias to unmarshal Days in Transition
type TransitionDays int
