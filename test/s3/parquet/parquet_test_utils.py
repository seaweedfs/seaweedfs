"""
Shared utility functions for PyArrow Parquet tests.

This module provides common test utilities used across multiple test scripts
to avoid code duplication and ensure consistency.
"""

import pyarrow as pa


def create_sample_table(num_rows: int = 5) -> pa.Table:
    """Create a sample PyArrow table for testing.
    
    Args:
        num_rows: Number of rows to generate (default: 5)
    
    Returns:
        PyArrow Table with test data containing:
        - id: int64 sequential IDs (0 to num_rows-1)
        - name: string user names (user_0, user_1, ...)
        - value: float64 values (id * 1.5)
        - flag: bool alternating True/False based on even/odd id
    
    Example:
        >>> table = create_sample_table(3)
        >>> print(table)
        pyarrow.Table
        id: int64
        name: string
        value: double
        flag: bool
    """
    return pa.table(
        {
            "id": pa.array(range(num_rows), type=pa.int64()),
            "name": pa.array([f"user_{i}" for i in range(num_rows)], type=pa.string()),
            "value": pa.array([float(i) * 1.5 for i in range(num_rows)], type=pa.float64()),
            "flag": pa.array([i % 2 == 0 for i in range(num_rows)], type=pa.bool_()),
        }
    )

