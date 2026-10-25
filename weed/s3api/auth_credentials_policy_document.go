package s3api

import (
	"encoding/json"
	"strings"
)

// normalizeAdvancedIAMPolicies rewrites policies written in the advanced IAM
// form ({"name": ..., "document": {...}}) into the S3 config form
// ({"name": ..., "content": "{...}"}).
//
// The advanced IAM file given by -s3.iam.config is also parsed as the S3
// identity config when no -s3.config is given, and protojson drops the unknown
// "document" field. That leaves a policy with empty content, which fails every
// later parse and takes the whole runtime policy sync down with it.
func normalizeAdvancedIAMPolicies(configContent []byte) []byte {
	var root map[string]json.RawMessage
	if err := json.Unmarshal(configContent, &root); err != nil {
		return configContent
	}
	rawPolicies, found := root["policies"]
	if !found {
		return configContent
	}
	var policies []map[string]json.RawMessage
	if err := json.Unmarshal(rawPolicies, &policies); err != nil {
		return configContent
	}

	rewritten := false
	for _, policy := range policies {
		document, hasDocument := policy["document"]
		if !hasDocument || hasPolicyContent(policy) {
			continue
		}
		// A document already written as a JSON string is the content verbatim;
		// an inline object becomes the JSON encoding of its own bytes. Nothing
		// is mutated until the content is in hand, so a failure leaves the
		// policy as it was rather than stripping its only definition.
		content := document
		if !isJSONString(document) {
			encoded, err := json.Marshal(string(document))
			if err != nil {
				continue
			}
			content = encoded
		}
		delete(policy, "document")
		policy["content"] = content
		rewritten = true
	}
	if !rewritten {
		return configContent
	}

	encodedPolicies, err := json.Marshal(policies)
	if err != nil {
		return configContent
	}
	root["policies"] = encodedPolicies
	normalized, err := json.Marshal(root)
	if err != nil {
		return configContent
	}
	return normalized
}

func hasPolicyContent(policy map[string]json.RawMessage) bool {
	raw, found := policy["content"]
	if !found {
		return false
	}
	var content string
	if err := json.Unmarshal(raw, &content); err != nil {
		// Not a string: leave whatever it is for the proto parser to reject.
		return true
	}
	return strings.TrimSpace(content) != ""
}

func isJSONString(raw json.RawMessage) bool {
	var s string
	return json.Unmarshal(raw, &s) == nil
}
