package plugins

import (
	"path/filepath"
	"strings"
	
	"github.com/seaweedfs/seaweedfs/weed/mount/ml"
)

// PyTorchPlugin provides PyTorch-specific optimizations
type PyTorchPlugin struct {
	name     string
	version  string
}

// NewPyTorchPlugin creates a new PyTorch optimization plugin
func NewPyTorchPlugin() *PyTorchPlugin {
	return &PyTorchPlugin{
		name:    "pytorch",
		version: "1.0.0",
	}
}

// GetFrameworkName returns the framework name
func (p *PyTorchPlugin) GetFrameworkName() string {
	return p.name
}

// DetectFramework detects if a file belongs to PyTorch framework
func (p *PyTorchPlugin) DetectFramework(filePath string, content []byte) float64 {
	confidence := 0.0
	
	// File extension-based detection
	ext := strings.ToLower(filepath.Ext(filePath))
	switch ext {
	case ".pth", ".pt":
		confidence = 0.95
	case ".pkl":
		if strings.Contains(strings.ToLower(filePath), "pytorch") || 
		   strings.Contains(strings.ToLower(filePath), "torch") {
			confidence = 0.7
		} else {
			confidence = 0.3
		}
	}
	
	// Content-based detection (if content is provided)
	if len(content) > 0 {
		contentStr := string(content[:minInt(len(content), 1024)]) // First 1KB
		if strings.Contains(contentStr, "torch") || 
		   strings.Contains(contentStr, "pytorch") ||
		   strings.Contains(contentStr, "PytorchStreamReader") {
			confidence = maxFloat64(confidence, 0.8)
		}
	}
	
	// Path-based detection
	if strings.Contains(strings.ToLower(filePath), "torch") ||
	   strings.Contains(strings.ToLower(filePath), "pytorch") {
		confidence = maxFloat64(confidence, 0.6)
	}
	
	return confidence
}

// GetOptimizationHints provides PyTorch-specific optimization hints
func (p *PyTorchPlugin) GetOptimizationHints(context *ml.OptimizationContext) []ml.OptimizationHint {
	hints := make([]ml.OptimizationHint, 0)
	
	// Model file optimizations
	if context.FileType == "model" && p.isPyTorchModel(context.FilePath) {
		hints = append(hints, ml.OptimizationHint{
			Type:        "cache_strategy",
			Description: "PyTorch models benefit from persistent memory caching",
			Priority:    90,
			Parameters: map[string]interface{}{
				"cache_type":     "memory",
				"persistence":    true,
				"compression":    false,
				"prefetch_size":  "25%", // 25% of model size
			},
		})
		
		if context.FileSize > 500*1024*1024 { // > 500MB
			hints = append(hints, ml.OptimizationHint{
				Type:        "loading_strategy",
				Description: "Large PyTorch model - consider lazy loading",
				Priority:    85,
				Parameters: map[string]interface{}{
					"lazy_loading":   true,
					"chunk_size":     64 * 1024 * 1024, // 64MB chunks
					"parallel_load":  true,
				},
			})
		}
	}
	
	// Dataset optimizations
	if p.isPyTorchDataset(context.FilePath) {
		hints = append(hints, ml.OptimizationHint{
			Type:        "dataloader_optimization",
			Description: "PyTorch DataLoader optimization for training efficiency",
			Priority:    80,
			Parameters: map[string]interface{}{
				"num_workers":    4,
				"pin_memory":     true,
				"prefetch_factor": 2,
				"persistent_workers": true,
			},
		})
	}
	
	// Training-specific optimizations
	if context.WorkloadType == "training" {
		hints = append(hints, ml.OptimizationHint{
			Type:        "training_optimization",
			Description: "PyTorch training optimizations",
			Priority:    75,
			Parameters: map[string]interface{}{
				"gradient_checkpointing": context.FileSize > 1024*1024*1024, // > 1GB
				"mixed_precision":        true,
				"batch_accumulation":     context.BatchSize > 32,
			},
		})
	}
	
	return hints
}

// GetDefaultRules returns PyTorch-specific optimization rules
func (p *PyTorchPlugin) GetDefaultRules() []*ml.OptimizationRule {
	return []*ml.OptimizationRule{
		{
			ID:          "pytorch_model_caching",
			Name:        "PyTorch Model Caching",
			Description: "Optimized caching for PyTorch model files",
			Priority:    95,
			Conditions: []ml.RuleCondition{
				{
					Type:     "file_pattern",
					Property: "extension",
					Operator: "in",
					Value:    []string{".pth", ".pt"},
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
					Target: "file",
					Parameters: map[string]interface{}{
						"strategy":         "pytorch_model",
						"cache_type":       "memory",
						"eviction_policy":  "lfu",
						"compression":      false,
						"preload":          true,
					},
				},
			},
			Metadata: map[string]interface{}{
				"framework": "pytorch",
				"category":  "model_caching",
			},
		},
		{
			ID:          "pytorch_checkpoint_handling",
			Name:        "PyTorch Checkpoint Optimization",
			Description: "Optimized handling for PyTorch training checkpoints",
			Priority:    85,
			Conditions: []ml.RuleCondition{
				{
					Type:     "file_pattern",
					Property: "name_pattern",
					Operator: "matches",
					Value:    ".*checkpoint.*\\.(pth|pt)$",
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
					Target: "file",
					Parameters: map[string]interface{}{
						"incremental_save": true,
						"compression":      true,
						"backup_strategy":  "rolling",
						"sync_frequency":   "epoch",
					},
				},
			},
			Metadata: map[string]interface{}{
				"framework": "pytorch",
				"category":  "checkpoint",
			},
		},
		{
			ID:          "pytorch_tensor_prefetch",
			Name:        "PyTorch Tensor Prefetching",
			Description: "Intelligent prefetching for PyTorch tensor operations",
			Priority:    80,
			Conditions: []ml.RuleCondition{
				{
					Type:     "access_pattern",
					Property: "pattern_type",
					Operator: "in",
					Value:    []string{"sequential", "strided"},
					Weight:   1.0,
				},
				{
					Type:     "workload_context",
					Property: "framework",
					Operator: "equals",
					Value:    "pytorch",
					Weight:   0.9,
				},
				{
					Type:     "workload_context",
					Property: "batch_size",
					Operator: "greater_than",
					Value:    8,
					Weight:   0.7,
				},
			},
			Actions: []ml.RuleAction{
				{
					Type:   "prefetch",
					Target: "tensor",
					Parameters: map[string]interface{}{
						"strategy":        "pytorch_tensor",
						"prefetch_size":   "batch_aligned",
						"parallel_workers": 2,
						"cuda_streams":     true,
					},
				},
			},
			Metadata: map[string]interface{}{
				"framework": "pytorch",
				"category":  "tensor_ops",
			},
		},
	}
}

// GetDefaultTemplates returns PyTorch-specific optimization templates
func (p *PyTorchPlugin) GetDefaultTemplates() []*ml.OptimizationTemplate {
	return []*ml.OptimizationTemplate{
		{
			ID:          "pytorch_training_template",
			Name:        "PyTorch Training Optimization",
			Description: "Complete optimization template for PyTorch training workloads",
			Category:    "training",
			Rules: []string{
				"pytorch_model_caching",
				"pytorch_checkpoint_handling", 
				"pytorch_tensor_prefetch",
				"sequential_prefetch", // From base rules
				"dataset_batch_optimize", // From base rules
			},
			Parameters: map[string]interface{}{
				"framework":           "pytorch",
				"training_phase":      "active",
				"memory_optimization": true,
				"gpu_optimization":    true,
				"dataloader_config": map[string]interface{}{
					"num_workers":         4,
					"pin_memory":          true,
					"persistent_workers":  true,
					"prefetch_factor":     2,
				},
				"model_config": map[string]interface{}{
					"gradient_checkpointing": false,
					"mixed_precision":        true,
					"compile_model":          true,
				},
			},
		},
		{
			ID:          "pytorch_inference_template", 
			Name:        "PyTorch Inference Optimization",
			Description: "Optimized template for PyTorch inference workloads",
			Category:    "inference",
			Rules: []string{
				"pytorch_model_caching",
				"pytorch_tensor_prefetch",
			},
			Parameters: map[string]interface{}{
				"framework":       "pytorch",
				"inference_mode":  true,
				"batch_inference": true,
				"model_config": map[string]interface{}{
					"torch_compile":     true,
					"optimization_level": "O2",
					"precision":         "fp16",
				},
			},
		},
		{
			ID:          "pytorch_research_template",
			Name:        "PyTorch Research & Experimentation",
			Description: "Flexible template for PyTorch research and experimentation",
			Category:    "research",
			Rules: []string{
				"pytorch_model_caching",
				"pytorch_checkpoint_handling",
			},
			Parameters: map[string]interface{}{
				"framework":         "pytorch",
				"experiment_tracking": true,
				"flexible_caching":   true,
				"checkpoint_config": map[string]interface{}{
					"save_frequency":   "auto",
					"version_control":  true,
					"metadata_tracking": true,
				},
			},
		},
	}
}

// Helper methods
func (p *PyTorchPlugin) isPyTorchModel(filePath string) bool {
	ext := strings.ToLower(filepath.Ext(filePath))
	return ext == ".pth" || ext == ".pt"
}

func (p *PyTorchPlugin) isPyTorchDataset(filePath string) bool {
	// Common PyTorch dataset patterns
	baseName := strings.ToLower(filepath.Base(filePath))
	return strings.Contains(baseName, "dataset") || 
	       strings.Contains(baseName, "train") ||
	       strings.Contains(baseName, "val") ||
	       strings.Contains(baseName, "test")
}

// Utility functions
func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func maxFloat64(a, b float64) float64 {
	if a > b {
		return a
	}
	return b
}
