package engine

import "fmt"

// Error types for better error handling and testing

// AggregationError represents errors that occur during aggregation computation
type AggregationError struct {
	Operation string
	Column    string
	Cause     error
}

func (e AggregationError) Error() string {
	return fmt.Sprintf("aggregation error in %s(%s): %v", e.Operation, e.Column, e.Cause)
}

// DataSourceError represents errors that occur when accessing data sources
type DataSourceError struct {
	Source string
	Cause  error
}

func (e DataSourceError) Error() string {
	return fmt.Sprintf("data source error in %s: %v", e.Source, e.Cause)
}

// OptimizationError represents errors that occur during query optimization
type OptimizationError struct {
	Strategy string
	Reason   string
}

func (e OptimizationError) Error() string {
	return fmt.Sprintf("optimization failed for %s: %s", e.Strategy, e.Reason)
}

// ParseError represents SQL parsing errors
type ParseError struct {
	Query   string
	Message string
	Cause   error
}

func (e ParseError) Error() string {
	if e.Cause != nil {
		return fmt.Sprintf("SQL parse error: %s (%v)", e.Message, e.Cause)
	}
	return fmt.Sprintf("SQL parse error: %s", e.Message)
}

// TableNotFoundError represents table/topic not found errors
type TableNotFoundError struct {
	Database string
	Table    string
}

func (e TableNotFoundError) Error() string {
	if e.Database != "" {
		return fmt.Sprintf("table %s.%s not found", e.Database, e.Table)
	}
	return fmt.Sprintf("table %s not found", e.Table)
}

// ColumnNotFoundError represents column not found errors
type ColumnNotFoundError struct {
	Table  string
	Column string
}

func (e ColumnNotFoundError) Error() string {
	if e.Table != "" {
		return fmt.Sprintf("column %s not found in table %s", e.Column, e.Table)
	}
	return fmt.Sprintf("column %s not found", e.Column)
}

// UnsupportedFeatureError represents unsupported SQL features
type UnsupportedFeatureError struct {
	Feature string
	Reason  string
}

func (e UnsupportedFeatureError) Error() string {
	if e.Reason != "" {
		return fmt.Sprintf("feature not supported: %s (%s)", e.Feature, e.Reason)
	}
	return fmt.Sprintf("feature not supported: %s", e.Feature)
}
