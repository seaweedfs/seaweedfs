package plugins

import (
	"path/filepath"
	"strings"
	
	"github.com/seaweedfs/seaweedfs/weed/mount/ml"
)

// TensorFlowPlugin provides TensorFlow-specific optimizations
type TensorFlowPlugin struct {
	name     string
	version  string
}

// NewTensorFlowPlugin creates a new TensorFlow optimization plugin
func NewTensorFlowPlugin() *TensorFlowPlugin {
	return &TensorFlowPlugin{
		name:    "tensorflow",
		version: "1.0.0",
	}
}

// GetFrameworkName returns the framework name
func (p *TensorFlowPlugin) GetFrameworkName() string {
	return p.name
}

// DetectFramework detects if a file belongs to TensorFlow framework
func (p *TensorFlowPlugin) DetectFramework(filePath string, content []byte) float64 {
	confidence := 0.0
	
	// File extension-based detection
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".pb":
		confidence = 0.85 // Could be TensorFlow or other protobuf
	case ".h5", ".hdf5":
		confidence = 0.80 // Common for Keras/TensorFlow models
	case ".ckpt":
		confidence = 0.75 // TensorFlow checkpoint format
	case ".tflite":
		confidence = 0.95 // TensorFlow Lite model
	case ".tfrecord":
		confidence = 0.95 // TensorFlow record format
	}
	
	// Content-based detection (if content is provided)
	if len(content) > 0 {
		contentStr := string(content[:minIntTF(len(content), 1024)]) // First 1KB
		if strings.Contains(contentStr, "tensorflow") ||
		   strings.Contains(contentStr, "tf.") ||
		   strings.Contains(contentStr, "keras") ||
		   strings.Contains(contentStr, "SavedModel") {
			confidence = maxFloat64TF(confidence, 0.85)
		}
		
		// Check for TensorFlow protobuf signatures
		if strings.Contains(contentStr, "\x08\x01\x12") || // TF SavedModel signature
		   strings.Contains(contentStr, "saved_model") {
			confidence = maxFloat64TF(confidence, 0.90)
		}
	}
	
	// Path-based detection
	lowerPath := strings.ToLower(filePath)
	if strings.Contains(lowerPath, "tensorflow") ||
	   strings.Contains(lowerPath, "savedmodel") ||
	   strings.Contains(lowerPath, "keras") ||
	   strings.Contains(lowerPath, "tfhub") {
		confidence = maxFloat64TF(confidence, 0.7)
	}
	
	// Directory structure hints
	if strings.Contains(lowerPath, "variables/variables") ||
	   strings.Contains(lowerPath, "saved_model.pb") {
		confidence = 0.95
	}
	
	return confidence
}

// GetOptimizationHints provides TensorFlow-specific optimization hints
func (p *TensorFlowPlugin) GetOptimizationHints(context *ml.OptimizationContext) []ml.OptimizationHint {
	hints := make([]ml.OptimizationHint, 0)
	
	// SavedModel optimizations
	if p.isTensorFlowSavedModel(context.FilePath) {
		hints = append(hints, ml.OptimizationHint{
			Type:        "savedmodel_optimization",
			Description: "TensorFlow SavedModel optimizations",
			Priority:    95,
			Parameters: map[string]interface{}{
				"preload_signatures": true,
				"cache_variables":    true,
				"parallel_load":      true,
				"memory_mapping":     context.FileSize > 100*1024*1024, // > 100MB
			},
		})
	}
	
	// TFRecord dataset optimizations
	if p.isTFRecord(context.FilePath) {
		hints = append(hints, ml.OptimizationHint{
			Type:        "tfrecord_optimization",
			Description: "TFRecord dataset reading optimization",
			Priority:    85,
			Parameters: map[string]interface{}{
				"parallel_reads":     8,
				"buffer_size":        64 * 1024 * 1024, // 64MB
				"compression":        "auto_detect",
				"prefetch_buffer":    "auto",
				"interleave_datasets": true,
			},
		})
	}
	
	// Training optimizations
	if context.WorkloadType == "training" {
		hints = append(hints, ml.OptimizationHint{
			Type:        "tf_training_optimization",
			Description: "TensorFlow training performance optimizations",
			Priority:    80,
			Parameters: map[string]interface{}{
				"mixed_precision":     true,
				"xla_compilation":     true,
				"dataset_prefetch":    "autotune",
				"gradient_compression": context.ModelSize > 500*1024*1024, // > 500MB
			},
		})
	}
	
	// Inference optimizations
	if context.WorkloadType == "inference" {
		hints = append(hints, ml.OptimizationHint{
			Type:        "tf_inference_optimization", 
			Description: "TensorFlow inference optimizations",
			Priority:    75,
			Parameters: map[string]interface{}{
				"optimize_for_inference": true,
				"use_trt":               len(context.AvailableGPUs) > 0, // TensorRT if GPU available
				"batch_inference":       context.BatchSize > 1,
				"model_pruning":         false, // Conservative default
			},
		})
	}
	
	return hints
}

// GetDefaultRules returns TensorFlow-specific optimization rules
func (p *TensorFlowPlugin) GetDefaultRules() []*ml.OptimizationRule {
	return []*ml.OptimizationRule{
		{
			ID:          "tensorflow_savedmodel_caching",
			Name:        "TensorFlow SavedModel Caching",
			Description: "Optimized caching for TensorFlow SavedModel files",
			Priority:    95,
			Conditions: []ml.RuleCondition{
				{
					Type:     "file_pattern",
					Property: "name_pattern",
					Operator: "matches",
					Value:    ".*(saved_model\\.pb|variables/).*",
					Weight:   1.0,
				},
				{
					Type:     "file_context",
					Property: "size",
					Operator: "greater_than",
					Value:    1024 * 1024, // > 1MB
					Weight:   0.8,
				},
			},
			Actions: []ml.RuleAction{
				{
					Type:   "cache",
					Target: "savedmodel",
					Parameters: map[string]interface{}{
						"strategy":           "tensorflow_savedmodel",
						"cache_type":         "memory",
						"preload_metadata":   true,
						"parallel_loading":   true,
						"variable_caching":   true,
					},
				},
			},
			Metadata: map[string]interface{}{
				"framework": "tensorflow",
				"category":  "savedmodel",
			},
		},
		{
			ID:          "tfrecord_streaming_optimization",
			Name:        "TFRecord Streaming Optimization",
			Description: "Optimized streaming for TFRecord datasets",
			Priority:    90,
			Conditions: []ml.RuleCondition{
				{
					Type:     "file_pattern",
					Property: "extension",
					Operator: "equals",
					Value:    ".tfrecord",
					Weight:   1.0,
				},
				{
					Type:     "access_pattern",
					Property: "pattern_type",
					Operator: "in",
					Value:    []string{"sequential", "batch"},
					Weight:   0.9,
				},
			},
			Actions: []ml.RuleAction{
				{
					Type:   "stream_optimization",
					Target: "tfrecord",
					Parameters: map[string]interface{}{
						"parallel_reads":      8,
						"buffer_size":         64 * 1024 * 1024, // 64MB
						"prefetch_buffer":     "autotune",
						"compression_aware":   true,
						"record_batching":     true,
					},
				},
			},
			Metadata: map[string]interface{}{
				"framework": "tensorflow",
				"category":  "dataset",
			},
		},
		{
			ID:          "tensorflow_checkpoint_optimization",
			Name:        "TensorFlow Checkpoint Optimization",
			Description: "Optimized handling for TensorFlow checkpoints",
			Priority:    85,
			Conditions: []ml.RuleCondition{
				{
					Type:     "file_pattern",
					Property: "extension",
					Operator: "equals",
					Value:    ".ckpt",
					Weight:   1.0,
				},
				{
					Type:     "workload_context",
					Property: "workload_type",
					Operator: "equals",
					Value:    "training",
					Weight:   0.9,
				},
			},
			Actions: []ml.RuleAction{
				{
					Type:   "checkpoint_optimization",
					Target: "tensorflow_checkpoint",
					Parameters: map[string]interface{}{
						"async_save":         true,
						"compression":        "gzip",
						"sharding":          true,
						"metadata_caching":   true,
					},
				},
			},
			Metadata: map[string]interface{}{
				"framework": "tensorflow",
				"category":  "checkpoint",
			},
		},
		{
			ID:          "keras_model_optimization",
			Name:        "Keras Model Optimization",
			Description: "Optimizations for Keras model files",
			Priority:    80,
			Conditions: []ml.RuleCondition{
				{
					Type:     "file_pattern",
					Property: "extension",
					Operator: "in",
					Value:    []string{".h5", ".hdf5"},
					Weight:   1.0,
				},
				{
					Type:     "workload_context",
					Property: "framework",
					Operator: "equals",
					Value:    "tensorflow",
					Weight:   0.8,
				},
			},
			Actions: []ml.RuleAction{
				{
					Type:   "model_optimization",
					Target: "keras_model",
					Parameters: map[string]interface{}{
						"lazy_loading":       true,
						"weight_compression": false,
						"architecture_cache": true,
						"parallel_loading":   true,
					},
				},
			},
			Metadata: map[string]interface{}{
				"framework": "tensorflow",
				"category":  "keras_model",
			},
		},
	}
}

// GetDefaultTemplates returns TensorFlow-specific optimization templates
func (p *TensorFlowPlugin) GetDefaultTemplates() []*ml.OptimizationTemplate {
	return []*ml.OptimizationTemplate{
		{
			ID:          "tensorflow_training_template",
			Name:        "TensorFlow Training Optimization",
			Description: "Complete optimization template for TensorFlow training workloads",
			Category:    "training",
			Rules: []string{
				"tensorflow_savedmodel_caching",
				"tfrecord_streaming_optimization",
				"tensorflow_checkpoint_optimization",
				"keras_model_optimization",
				"sequential_prefetch", // From base rules
				"dataset_batch_optimize", // From base rules
			},
			Parameters: map[string]interface{}{
				"framework":        "tensorflow",
				"training_phase":   "active",
				"optimization_level": "O2",
				"dataset_config": map[string]interface{}{
					"parallel_calls":    "autotune",
					"buffer_size":       "autotune", 
					"prefetch":          "autotune",
					"cache":             true,
				},
				"model_config": map[string]interface{}{
					"mixed_precision":   true,
					"xla_compilation":   true,
					"gradient_clipping": true,
				},
				"checkpoint_config": map[string]interface{}{
					"save_best_only":    false,
					"save_frequency":    "epoch",
					"async_save":        true,
				},
			},
		},
		{
			ID:          "tensorflow_inference_template",
			Name:        "TensorFlow Inference Optimization",
			Description: "Optimized template for TensorFlow inference workloads",
			Category:    "inference",
			Rules: []string{
				"tensorflow_savedmodel_caching",
				"keras_model_optimization",
			},
			Parameters: map[string]interface{}{
				"framework":         "tensorflow",
				"inference_mode":    true,
				"batch_processing":  true,
				"model_config": map[string]interface{}{
					"optimize_for_inference": true,
					"use_tensorrt":          false, // Conservative default
					"precision":             "fp32",
					"max_batch_size":        32,
				},
				"serving_config": map[string]interface{}{
					"model_warmup":      true,
					"request_batching":  true,
					"response_caching":  false,
				},
			},
		},
		{
			ID:          "tensorflow_data_pipeline_template",
			Name:        "TensorFlow Data Pipeline Optimization",
			Description: "Optimized template for TensorFlow data processing pipelines",
			Category:    "data_processing",
			Rules: []string{
				"tfrecord_streaming_optimization",
				"dataset_batch_optimize",
			},
			Parameters: map[string]interface{}{
				"framework":          "tensorflow",
				"pipeline_focus":     "data",
				"performance_mode":   "throughput",
				"data_config": map[string]interface{}{
					"parallel_interleave": true,
					"deterministic":       false,
					"experimental_optimization": true,
					"autotune":           true,
				},
				"io_config": map[string]interface{}{
					"num_parallel_reads":  "autotune",
					"compression_type":    "auto",
					"buffer_size":         "autotune",
				},
			},
		},
		{
			ID:          "tensorflow_distributed_template",
			Name:        "TensorFlow Distributed Training",
			Description: "Optimization template for TensorFlow distributed training",
			Category:    "distributed_training",
			Rules: []string{
				"tensorflow_savedmodel_caching",
				"tensorflow_checkpoint_optimization",
				"tfrecord_streaming_optimization",
			},
			Parameters: map[string]interface{}{
				"framework":           "tensorflow",
				"distribution_strategy": "MultiWorkerMirroredStrategy",
				"distributed_config": map[string]interface{}{
					"all_reduce_alg":      "ring",
					"gradient_compression": true,
					"collective_ops":      true,
				},
				"communication_config": map[string]interface{}{
					"compression":         "auto",
					"timeout_seconds":     300,
					"retry_count":         3,
				},
			},
		},
	}
}

// Helper methods
func (p *TensorFlowPlugin) isTensorFlowSavedModel(filePath string) bool {
	lowerPath := strings.ToLower(filePath)
	return strings.Contains(lowerPath, "saved_model.pb") ||
	       strings.Contains(lowerPath, "variables/variables") ||
	       strings.Contains(lowerPath, "savedmodel")
}

func (p *TensorFlowPlugin) isTFRecord(filePath string) bool {
	ext := strings.ToLower(filepath.Ext(filePath))
	return ext == ".tfrecord" || ext == ".tfrecords"
}

func (p *TensorFlowPlugin) isKerasModel(filePath string) bool {
	ext := strings.ToLower(filepath.Ext(filePath))
	return ext == ".h5" || ext == ".hdf5"
}

// Utility functions
func minIntTF(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxFloat64TF(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
