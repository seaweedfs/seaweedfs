package com.seaweedfs.jdbc;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.util.List;

/**
 * ResultSetMetaData implementation for SeaweedFS JDBC
 */
public class SeaweedFSResultSetMetaData implements ResultSetMetaData {
    
    private final List<String> columnNames;
    
    public SeaweedFSResultSetMetaData(List<String> columnNames) {
        this.columnNames = columnNames;
    }

    @Override
    public int getColumnCount() throws SQLException {
        return columnNames.size();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        checkColumnIndex(column);
        return false; // SeaweedFS doesn't have auto-increment columns
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        checkColumnIndex(column);
        return true; // Assume case sensitive
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        checkColumnIndex(column);
        return true; // All columns are searchable
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        checkColumnIndex(column);
        return false; // No currency columns
    }

    @Override
    public int isNullable(int column) throws SQLException {
        checkColumnIndex(column);
        return columnNullable; // Assume nullable
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        checkColumnIndex(column);
        // For simplicity, assume all numeric types are signed
        return true;
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        checkColumnIndex(column);
        return 50; // Default display size
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        checkColumnIndex(column);
        return columnNames.get(column - 1);
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        checkColumnIndex(column);
        return columnNames.get(column - 1);
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        checkColumnIndex(column);
        return ""; // No schema concept in SeaweedFS
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        checkColumnIndex(column);
        return 0; // Unknown precision
    }

    @Override
    public int getScale(int column) throws SQLException {
        checkColumnIndex(column);
        return 0; // Unknown scale
    }

    @Override
    public String getTableName(int column) throws SQLException {
        checkColumnIndex(column);
        return ""; // Table name not available in result set metadata
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        checkColumnIndex(column);
        return ""; // No catalog concept in SeaweedFS
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        checkColumnIndex(column);
        // For simplicity, we'll determine type based on column name patterns
        String columnName = columnNames.get(column - 1).toLowerCase();
        
        if (columnName.contains("timestamp") || columnName.contains("time") || columnName.equals("_timestamp_ns")) {
            return Types.TIMESTAMP;
        } else if (columnName.contains("id") || columnName.contains("count") || columnName.contains("size")) {
            return Types.BIGINT;
        } else if (columnName.contains("amount") || columnName.contains("price") || columnName.contains("rate")) {
            return Types.DECIMAL;
        } else if (columnName.contains("flag") || columnName.contains("enabled") || columnName.contains("active")) {
            return Types.BOOLEAN;
        } else {
            return Types.VARCHAR; // Default to VARCHAR
        }
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        int sqlType = getColumnType(column);
        switch (sqlType) {
            case Types.VARCHAR:
                return "VARCHAR";
            case Types.BIGINT:
                return "BIGINT";
            case Types.DECIMAL:
                return "DECIMAL";
            case Types.BOOLEAN:
                return "BOOLEAN";
            case Types.TIMESTAMP:
                return "TIMESTAMP";
            default:
                return "VARCHAR";
        }
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        checkColumnIndex(column);
        return true; // SeaweedFS is read-only
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        checkColumnIndex(column);
        return false; // SeaweedFS is read-only
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        checkColumnIndex(column);
        return false; // SeaweedFS is read-only
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        int sqlType = getColumnType(column);
        switch (sqlType) {
            case Types.VARCHAR:
                return "java.lang.String";
            case Types.BIGINT:
                return "java.lang.Long";
            case Types.DECIMAL:
                return "java.math.BigDecimal";
            case Types.BOOLEAN:
                return "java.lang.Boolean";
            case Types.TIMESTAMP:
                return "java.sql.Timestamp";
            default:
                return "java.lang.String";
        }
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        if (iface.isAssignableFrom(getClass())) {
            return iface.cast(this);
        }
        throw new SQLException("Cannot unwrap to " + iface.getName());
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return iface.isAssignableFrom(getClass());
    }
    
    private void checkColumnIndex(int column) throws SQLException {
        if (column < 1 || column > columnNames.size()) {
            throw new SQLException("Column index " + column + " is out of range (1-" + columnNames.size() + ")");
        }
    }
}
