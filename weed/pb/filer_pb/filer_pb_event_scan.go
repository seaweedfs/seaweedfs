package filer_pb

import (
	"google.golang.org/protobuf/encoding/protowire"
)

// ScanMetadataEventSkeleton extracts just the fields the subscription matcher
// reads, without materializing the entries' chunk lists. ok=false means the
// caller must fall back to a full unmarshal.
func ScanMetadataEventSkeleton(data []byte) (skeleton *SubscribeMetadataResponse, ok bool) {
	skeleton = &SubscribeMetadataResponse{}
	rest := data
	for len(rest) > 0 {
		num, typ, n := protowire.ConsumeTag(rest)
		if n < 0 {
			return nil, false
		}
		rest = rest[n:]
		switch {
		case num == 1 && typ == protowire.BytesType: // directory
			v, m := protowire.ConsumeBytes(rest)
			if m < 0 {
				return nil, false
			}
			skeleton.Directory = string(v)
			rest = rest[m:]
		case num == 2 && typ == protowire.BytesType: // event_notification
			if skeleton.EventNotification != nil {
				// repeated occurrences of a message field merge; punt to a full decode
				return nil, false
			}
			v, m := protowire.ConsumeBytes(rest)
			if m < 0 {
				return nil, false
			}
			notification, scanOk := scanEventNotificationSkeleton(v)
			if !scanOk {
				return nil, false
			}
			skeleton.EventNotification = notification
			rest = rest[m:]
		case num == 3 && typ == protowire.VarintType: // ts_ns
			v, m := protowire.ConsumeVarint(rest)
			if m < 0 {
				return nil, false
			}
			skeleton.TsNs = int64(v)
			rest = rest[m:]
		default:
			m := protowire.ConsumeFieldValue(num, typ, rest)
			if m < 0 {
				return nil, false
			}
			rest = rest[m:]
		}
	}
	return skeleton, true
}

func scanEventNotificationSkeleton(data []byte) (*EventNotification, bool) {
	notification := &EventNotification{}
	rest := data
	for len(rest) > 0 {
		num, typ, n := protowire.ConsumeTag(rest)
		if n < 0 {
			return nil, false
		}
		rest = rest[n:]
		switch {
		case num == 1 && typ == protowire.BytesType: // old_entry
			if notification.OldEntry != nil {
				return nil, false
			}
			v, m := protowire.ConsumeBytes(rest)
			if m < 0 {
				return nil, false
			}
			name, scanOk := scanEntryName(v)
			if !scanOk {
				return nil, false
			}
			notification.OldEntry = &Entry{Name: name}
			rest = rest[m:]
		case num == 2 && typ == protowire.BytesType: // new_entry
			if notification.NewEntry != nil {
				return nil, false
			}
			v, m := protowire.ConsumeBytes(rest)
			if m < 0 {
				return nil, false
			}
			name, scanOk := scanEntryName(v)
			if !scanOk {
				return nil, false
			}
			notification.NewEntry = &Entry{Name: name}
			rest = rest[m:]
		case num == 4 && typ == protowire.BytesType: // new_parent_path
			v, m := protowire.ConsumeBytes(rest)
			if m < 0 {
				return nil, false
			}
			notification.NewParentPath = string(v)
			rest = rest[m:]
		default:
			m := protowire.ConsumeFieldValue(num, typ, rest)
			if m < 0 {
				return nil, false
			}
			rest = rest[m:]
		}
	}
	return notification, true
}

// scanEntryName walks the whole entry so field order does not matter.
func scanEntryName(data []byte) (string, bool) {
	name := ""
	rest := data
	for len(rest) > 0 {
		num, typ, n := protowire.ConsumeTag(rest)
		if n < 0 {
			return "", false
		}
		rest = rest[n:]
		if num == 1 && typ == protowire.BytesType { // name; last occurrence wins like proto merge
			v, m := protowire.ConsumeBytes(rest)
			if m < 0 {
				return "", false
			}
			name = string(v)
			rest = rest[m:]
			continue
		}
		m := protowire.ConsumeFieldValue(num, typ, rest)
		if m < 0 {
			return "", false
		}
		rest = rest[m:]
	}
	return name, true
}
