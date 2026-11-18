package http

// Write outcomes describe a refusal of this HTTP attempt before append, not the
// absence of a stored needle from an earlier attempt with a lost response.
const (
	WriteOutcomeHeader                 = "X-Seaweed-Write-Outcome"
	WriteOutcomeRefusedBeforeAppend    = "refused-before-append"
	WriteOutcomeExpiredJwtBeforeAppend = "expired-jwt-before-append"
)
