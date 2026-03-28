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
	XMLName                        xml.Name                       `xml:"Rule"`
	ID                             string                         `xml:"ID,omitempty"`
	Status                         ruleStatus                     `xml:"Status"`
	Filter                         Filter                         `xml:"Filter,omitempty"`
	Prefix                         Prefix                         `xml:"Prefix,omitempty"`
	Expiration                     Expiration                     `xml:"Expiration,omitempty"`
	Transition                     Transition                     `xml:"Transition,omitempty"`
	NoncurrentVersionExpiration    NoncurrentVersionExpiration    `xml:"NoncurrentVersionExpiration,omitempty"`
	NoncurrentVersionTransition    NoncurrentVersionTransition    `xml:"NoncurrentVersionTransition,omitempty"`
	AbortIncompleteMultipartUpload AbortIncompleteMultipartUpload `xml:"AbortIncompleteMultipartUpload,omitempty"`
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

	ObjectSizeGreaterThan int64
	ObjectSizeLessThan    int64
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
	if f.andSet {
		if err := e.EncodeElement(f.And, xml.StartElement{Name: xml.Name{Local: "And"}}); err != nil {
			return err
		}
	} else if f.tagSet {
		if err := e.EncodeElement(f.Tag, xml.StartElement{Name: xml.Name{Local: "Tag"}}); err != nil {
			return err
		}
	} else {
		if err := e.EncodeElement(f.Prefix, xml.StartElement{Name: xml.Name{Local: "Prefix"}}); err != nil {
			return err
		}
	}
	if f.ObjectSizeGreaterThan > 0 {
		if err := e.EncodeElement(f.ObjectSizeGreaterThan, xml.StartElement{Name: xml.Name{Local: "ObjectSizeGreaterThan"}}); err != nil {
			return err
		}
	}
	if f.ObjectSizeLessThan > 0 {
		if err := e.EncodeElement(f.ObjectSizeLessThan, xml.StartElement{Name: xml.Name{Local: "ObjectSizeLessThan"}}); err != nil {
			return err
		}
	}
	return e.EncodeToken(xml.EndElement{Name: start.Name})
}

// UnmarshalXML decodes Filter from XML, handling all child elements.
func (f *Filter) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	f.set = true
	for {
		tok, err := d.Token()
		if err != nil {
			return err
		}
		switch t := tok.(type) {
		case xml.StartElement:
			switch t.Name.Local {
			case "Prefix":
				if err := d.DecodeElement(&f.Prefix, &t); err != nil {
					return err
				}
			case "Tag":
				f.tagSet = true
				if err := d.DecodeElement(&f.Tag, &t); err != nil {
					return err
				}
			case "And":
				f.andSet = true
				if err := d.DecodeElement(&f.And, &t); err != nil {
					return err
				}
			case "ObjectSizeGreaterThan":
				if err := d.DecodeElement(&f.ObjectSizeGreaterThan, &t); err != nil {
					return err
				}
			case "ObjectSizeLessThan":
				if err := d.DecodeElement(&f.ObjectSizeLessThan, &t); err != nil {
					return err
				}
			default:
				if err := d.Skip(); err != nil {
					return err
				}
			}
		case xml.EndElement:
			return nil
		}
	}
}

// And - a tag to combine a prefix and multiple tags for lifecycle configuration rule.
type And struct {
	XMLName               xml.Name `xml:"And"`
	Prefix                Prefix   `xml:"Prefix,omitempty"`
	Tags                  []Tag    `xml:"Tag,omitempty"`
	ObjectSizeGreaterThan int64    `xml:"ObjectSizeGreaterThan,omitempty"`
	ObjectSizeLessThan    int64    `xml:"ObjectSizeLessThan,omitempty"`
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

func (e *Expiration) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type wrapper Expiration
	var w wrapper
	if err := d.DecodeElement(&w, &start); err != nil {
		return err
	}
	*e = Expiration(w)
	e.set = true
	return nil
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

func (b *ExpireDeleteMarker) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	var v bool
	if err := d.DecodeElement(&v, &start); err != nil {
		return err
	}
	*b = ExpireDeleteMarker{val: v, set: true}
	return nil
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

func (t *Transition) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type wrapper Transition
	var w wrapper
	if err := d.DecodeElement(&w, &start); err != nil {
		return err
	}
	*t = Transition(w)
	t.set = true
	return nil
}

// TransitionDays is a type alias to unmarshal Days in Transition
type TransitionDays int

// NoncurrentVersionExpiration - expiration actions for non-current object versions.
type NoncurrentVersionExpiration struct {
	XMLName                 xml.Name `xml:"NoncurrentVersionExpiration"`
	NoncurrentDays          int      `xml:"NoncurrentDays,omitempty"`
	NewerNoncurrentVersions int      `xml:"NewerNoncurrentVersions,omitempty"`

	set bool
}

// MarshalXML encodes NoncurrentVersionExpiration field into an XML form.
func (n NoncurrentVersionExpiration) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	if !n.set {
		return nil
	}
	type wrapper NoncurrentVersionExpiration
	return enc.EncodeElement(wrapper(n), start)
}

func (n *NoncurrentVersionExpiration) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type wrapper NoncurrentVersionExpiration
	var w wrapper
	if err := d.DecodeElement(&w, &start); err != nil {
		return err
	}
	*n = NoncurrentVersionExpiration(w)
	n.set = true
	return nil
}

// NoncurrentVersionTransition - transition actions for non-current object versions.
type NoncurrentVersionTransition struct {
	XMLName        xml.Name `xml:"NoncurrentVersionTransition"`
	NoncurrentDays int      `xml:"NoncurrentDays,omitempty"`
	StorageClass   string   `xml:"StorageClass,omitempty"`

	set bool
}

// MarshalXML encodes NoncurrentVersionTransition field into an XML form.
func (n NoncurrentVersionTransition) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	if !n.set {
		return nil
	}
	type wrapper NoncurrentVersionTransition
	return enc.EncodeElement(wrapper(n), start)
}

func (n *NoncurrentVersionTransition) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type wrapper NoncurrentVersionTransition
	var w wrapper
	if err := d.DecodeElement(&w, &start); err != nil {
		return err
	}
	*n = NoncurrentVersionTransition(w)
	n.set = true
	return nil
}

// AbortIncompleteMultipartUpload - abort action for incomplete multipart uploads.
type AbortIncompleteMultipartUpload struct {
	XMLName             xml.Name `xml:"AbortIncompleteMultipartUpload"`
	DaysAfterInitiation int      `xml:"DaysAfterInitiation,omitempty"`

	set bool
}

// MarshalXML encodes AbortIncompleteMultipartUpload field into an XML form.
func (a AbortIncompleteMultipartUpload) MarshalXML(enc *xml.Encoder, start xml.StartElement) error {
	if !a.set {
		return nil
	}
	type wrapper AbortIncompleteMultipartUpload
	return enc.EncodeElement(wrapper(a), start)
}

func (a *AbortIncompleteMultipartUpload) UnmarshalXML(d *xml.Decoder, start xml.StartElement) error {
	type wrapper AbortIncompleteMultipartUpload
	var w wrapper
	if err := d.DecodeElement(&w, &start); err != nil {
		return err
	}
	*a = AbortIncompleteMultipartUpload(w)
	a.set = true
	return nil
}
