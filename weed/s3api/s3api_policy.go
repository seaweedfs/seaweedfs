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
	XMLName xml.Name `xml:"Prefix"`
	set     bool

	val string
}

func (p Prefix) String() string {
	return p.val
}

// MarshalXML encodes Prefix field into an XML form.
func (p Prefix) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if !p.set {
		return nil
	}
	return e.EncodeElement(p.val, startElement)
}

func (p *Prefix) UnmarshalXML(d *xml.Decoder, startElement xml.StartElement) error {
	prefix := ""
	_ = d.DecodeElement(&prefix, &startElement)
	*p = Prefix{set: true, val: prefix}
	return nil
}

// MarshalXML encodes Filter field into an XML form.
func (f Filter) MarshalXML(e *xml.Encoder, start xml.StartElement) error {
	if !f.set {
		return nil
	}
	if err := e.EncodeToken(start); err != nil {
		return err
	}
	if err := e.EncodeElement(f.Prefix, xml.StartElement{Name: xml.Name{Local: "Prefix"}}); err != nil {
		return err
	}
	return e.EncodeToken(xml.EndElement{Name: start.Name})
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
	Date         ExpirationDate     `xml:"Date,omitempty"`
	DeleteMarker ExpireDeleteMarker `xml:"ExpiredObjectDeleteMarker"`

	set bool
}

// MarshalXML encodes expiration field into an XML form.
func (e Expiration) MarshalXML(enc *xml.Encoder, startElement xml.StartElement) error {
	if !e.set {
		return nil
	}
	type expirationWrapper Expiration
	return enc.EncodeElement(expirationWrapper(e), startElement)
}

// ExpireDeleteMarker represents value of ExpiredObjectDeleteMarker field in Expiration XML element.
type ExpireDeleteMarker struct {
	val bool
	set bool
}

// MarshalXML encodes delete marker boolean into an XML form.
func (b ExpireDeleteMarker) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if !b.set {
		return nil
	}
	return e.EncodeElement(b.val, startElement)
}

// ExpirationDate is a embedded type containing time.Time to unmarshal
// Date in Expiration
type ExpirationDate struct {
	time.Time
}

// MarshalXML encodes expiration date if it is non-zero and encodes
// empty string otherwise
func (eDate ExpirationDate) MarshalXML(e *xml.Encoder, startElement xml.StartElement) error {
	if eDate.Time.IsZero() {
		return nil
	}
	return e.EncodeElement(eDate.Format(time.RFC3339), startElement)
}

// Transition - transition actions for a rule in lifecycle configuration.
type Transition struct {
	XMLName      xml.Name  `xml:"Transition"`
	Days         int       `xml:"Days,omitempty"`
	Date         time.Time `xml:"Date,omitempty"`
	StorageClass string    `xml:"StorageClass,omitempty"`

	set bool
}

// MarshalXML encodes transition field into an XML form.
func (t Transition) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	if !t.set {
		return nil
	}
	type transitionWrapper Transition
	return enc.EncodeElement(transitionWrapper(t), start)
}

// TransitionDays is a type alias to unmarshal Days in Transition
type TransitionDays int
