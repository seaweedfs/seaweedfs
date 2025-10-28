package schema

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestManager_SchemaEvolution tests schema evolution integration in the manager
func TestManager_SchemaEvolution(t *testing.T) {
	// Create a manager without registry (for testing evolution logic only)
	manager := &Manager{
		evolutionChecker: NewSchemaEvolutionChecker(),
	}

	t.Run("Compatible Avro evolution", func(t *testing.T) {
		oldSchema := `{
			"type": "record",
			"name": "User",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"}
			]
		}`

		newSchema := `{
			"type": "record",
			"name": "User",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"},
				{"name": "email", "type": "string", "default": ""}
			]
		}`

		result, err := manager.CheckSchemaCompatibility(oldSchema, newSchema, FormatAvro, CompatibilityBackward)
		require.NoError(t, err)
		assert.True(t, result.Compatible)
		assert.Empty(t, result.Issues)
	})

	t.Run("Incompatible Avro evolution", func(t *testing.T) {
		oldSchema := `{
			"type": "record",
			"name": "User",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"},
				{"name": "email", "type": "string"}
			]
		}`

		newSchema := `{
			"type": "record",
			"name": "User",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"}
			]
		}`

		result, err := manager.CheckSchemaCompatibility(oldSchema, newSchema, FormatAvro, CompatibilityBackward)
		require.NoError(t, err)
		assert.False(t, result.Compatible)
		assert.NotEmpty(t, result.Issues)
		assert.Contains(t, result.Issues[0], "Field 'email' was removed")
	})

	t.Run("Schema evolution suggestions", func(t *testing.T) {
		oldSchema := `{
			"type": "record",
			"name": "User",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"}
			]
		}`

		newSchema := `{
			"type": "record",
			"name": "User",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"},
				{"name": "email", "type": "string"}
			]
		}`

		suggestions, err := manager.SuggestSchemaEvolution(oldSchema, newSchema, FormatAvro, CompatibilityBackward)
		require.NoError(t, err)
		assert.NotEmpty(t, suggestions)

		// Should suggest adding default values
		found := false
		for _, suggestion := range suggestions {
			if strings.Contains(suggestion, "default") {
				found = true
				break
			}
		}
		assert.True(t, found, "Should suggest adding default values, got: %v", suggestions)
	})

	t.Run("JSON Schema evolution", func(t *testing.T) {
		oldSchema := `{
			"type": "object",
			"properties": {
				"id": {"type": "integer"},
				"name": {"type": "string"}
			},
			"required": ["id", "name"]
		}`

		newSchema := `{
			"type": "object",
			"properties": {
				"id": {"type": "integer"},
				"name": {"type": "string"},
				"email": {"type": "string"}
			},
			"required": ["id", "name"]
		}`

		result, err := manager.CheckSchemaCompatibility(oldSchema, newSchema, FormatJSONSchema, CompatibilityBackward)
		require.NoError(t, err)
		assert.True(t, result.Compatible)
	})

	t.Run("Full compatibility check", func(t *testing.T) {
		oldSchema := `{
			"type": "record",
			"name": "User",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"}
			]
		}`

		newSchema := `{
			"type": "record",
			"name": "User",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"},
				{"name": "email", "type": "string", "default": ""}
			]
		}`

		result, err := manager.CheckSchemaCompatibility(oldSchema, newSchema, FormatAvro, CompatibilityFull)
		require.NoError(t, err)
		assert.True(t, result.Compatible)
	})

	t.Run("Type promotion compatibility", func(t *testing.T) {
		oldSchema := `{
			"type": "record",
			"name": "User",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "score", "type": "int"}
			]
		}`

		newSchema := `{
			"type": "record",
			"name": "User",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "score", "type": "long"}
			]
		}`

		result, err := manager.CheckSchemaCompatibility(oldSchema, newSchema, FormatAvro, CompatibilityBackward)
		require.NoError(t, err)
		assert.True(t, result.Compatible)
	})
}

// TestManager_CompatibilityLevels tests compatibility level management
func TestManager_CompatibilityLevels(t *testing.T) {
	manager := &Manager{
		evolutionChecker: NewSchemaEvolutionChecker(),
	}

	t.Run("Get default compatibility level", func(t *testing.T) {
		level := manager.GetCompatibilityLevel("test-subject")
		assert.Equal(t, CompatibilityBackward, level)
	})

	t.Run("Set compatibility level", func(t *testing.T) {
		err := manager.SetCompatibilityLevel("test-subject", CompatibilityFull)
		assert.NoError(t, err)
	})
}

// TestManager_CanEvolveSchema tests the CanEvolveSchema method
func TestManager_CanEvolveSchema(t *testing.T) {
	manager := &Manager{
		evolutionChecker: NewSchemaEvolutionChecker(),
	}

	t.Run("Compatible evolution", func(t *testing.T) {
		currentSchema := `{
			"type": "record",
			"name": "User",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"}
			]
		}`

		newSchema := `{
			"type": "record",
			"name": "User",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"},
				{"name": "email", "type": "string", "default": ""}
			]
		}`

		result, err := manager.CanEvolveSchema("test-subject", currentSchema, newSchema, FormatAvro)
		require.NoError(t, err)
		assert.True(t, result.Compatible)
	})

	t.Run("Incompatible evolution", func(t *testing.T) {
		currentSchema := `{
			"type": "record",
			"name": "User",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"},
				{"name": "email", "type": "string"}
			]
		}`

		newSchema := `{
			"type": "record",
			"name": "User",
			"fields": [
				{"name": "id", "type": "int"},
				{"name": "name", "type": "string"}
			]
		}`

		result, err := manager.CanEvolveSchema("test-subject", currentSchema, newSchema, FormatAvro)
		require.NoError(t, err)
		assert.False(t, result.Compatible)
		assert.Contains(t, result.Issues[0], "Field 'email' was removed")
	})
}

// TestManager_SchemaEvolutionWorkflow tests a complete schema evolution workflow
func TestManager_SchemaEvolutionWorkflow(t *testing.T) {
	manager := &Manager{
		evolutionChecker: NewSchemaEvolutionChecker(),
	}

	t.Run("Complete evolution workflow", func(t *testing.T) {
		// Step 1: Define initial schema
		initialSchema := `{
			"type": "record",
			"name": "UserEvent",
			"fields": [
				{"name": "userId", "type": "int"},
				{"name": "action", "type": "string"}
			]
		}`

		// Step 2: Propose schema evolution (compatible)
		evolvedSchema := `{
			"type": "record",
			"name": "UserEvent",
			"fields": [
				{"name": "userId", "type": "int"},
				{"name": "action", "type": "string"},
				{"name": "timestamp", "type": "long", "default": 0}
			]
		}`

		// Check compatibility explicitly
		result, err := manager.CanEvolveSchema("user-events", initialSchema, evolvedSchema, FormatAvro)
		require.NoError(t, err)
		assert.True(t, result.Compatible)

		// Step 3: Try incompatible evolution
		incompatibleSchema := `{
			"type": "record",
			"name": "UserEvent",
			"fields": [
				{"name": "userId", "type": "int"}
			]
		}`

		result, err = manager.CanEvolveSchema("user-events", initialSchema, incompatibleSchema, FormatAvro)
		require.NoError(t, err)
		assert.False(t, result.Compatible)
		assert.Contains(t, result.Issues[0], "Field 'action' was removed")

		// Step 4: Get suggestions for incompatible evolution
		suggestions, err := manager.SuggestSchemaEvolution(initialSchema, incompatibleSchema, FormatAvro, CompatibilityBackward)
		require.NoError(t, err)
		assert.NotEmpty(t, suggestions)
	})
}

// BenchmarkSchemaEvolution benchmarks schema evolution operations
func BenchmarkSchemaEvolution(b *testing.B) {
	manager := &Manager{
		evolutionChecker: NewSchemaEvolutionChecker(),
	}

	oldSchema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": "string", "default": ""}
		]
	}`

	newSchema := `{
		"type": "record",
		"name": "User",
		"fields": [
			{"name": "id", "type": "int"},
			{"name": "name", "type": "string"},
			{"name": "email", "type": "string", "default": ""},
			{"name": "age", "type": "int", "default": 0}
		]
	}`

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, err := manager.CheckSchemaCompatibility(oldSchema, newSchema, FormatAvro, CompatibilityBackward)
		if err != nil {
			b.Fatal(err)
		}
	}
}
