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
