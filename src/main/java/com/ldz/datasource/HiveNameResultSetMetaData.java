package com.ldz.datasource;

import java.sql.ResultSetMetaData;
import java.sql.SQLException;

public class HiveNameResultSetMetaData implements ResultSetMetaData {

    private ResultSetMetaData metaData;

    public HiveNameResultSetMetaData(ResultSetMetaData metaData) {
        this.metaData = metaData;
    }

    @Override
    public <T> T unwrap(Class<T> iface) throws SQLException {
        return this.metaData.unwrap(iface);
    }

    @Override
    public boolean isWrapperFor(Class<?> iface) throws SQLException {
        return this.metaData.isWrapperFor(iface);
    }

    @Override
    public int getColumnCount() throws SQLException {
        return this.metaData.getColumnCount();
    }

    @Override
    public boolean isAutoIncrement(int column) throws SQLException {
        return this.metaData.isAutoIncrement(column);
    }

    @Override
    public boolean isCaseSensitive(int column) throws SQLException {
        return this.metaData.isCaseSensitive(column);
    }

    @Override
    public boolean isSearchable(int column) throws SQLException {
        return this.metaData.isSearchable(column);
    }

    @Override
    public boolean isCurrency(int column) throws SQLException {
        return this.metaData.isCurrency(column);
    }

    @Override
    public int isNullable(int column) throws SQLException {
        return this.metaData.isNullable(column);
    }

    @Override
    public boolean isSigned(int column) throws SQLException {
        return this.metaData.isSigned(column);
    }

    @Override
    public int getColumnDisplaySize(int column) throws SQLException {
        return this.metaData.getColumnDisplaySize(column);
    }

    @Override
    public String getColumnLabel(int column) throws SQLException {
        return this.parserGenericFieldName(this.metaData.getColumnLabel(column));
    }

    @Override
    public String getColumnName(int column) throws SQLException {
        return this.parserGenericFieldName(this.metaData.getCatalogName(column));
    }

    @Override
    public String getSchemaName(int column) throws SQLException {
        return this.parserGenericFieldName(this.getSchemaName(column));
    }

    @Override
    public int getPrecision(int column) throws SQLException {
        return this.metaData.getPrecision(column);
    }

    @Override
    public int getScale(int column) throws SQLException {
        return this.metaData.getScale(column);
    }

    @Override
    public String getTableName(int column) throws SQLException {
        return this.metaData.getTableName(column);
    }

    @Override
    public String getCatalogName(int column) throws SQLException {
        return this.metaData.getCatalogName(column);
    }

    @Override
    public int getColumnType(int column) throws SQLException {
        return this.metaData.getColumnType(column);
    }

    @Override
    public String getColumnTypeName(int column) throws SQLException {
        return this.metaData.getColumnTypeName(column);
    }

    @Override
    public boolean isReadOnly(int column) throws SQLException {
        return this.metaData.isReadOnly(column);
    }

    @Override
    public boolean isWritable(int column) throws SQLException {
        return this.metaData.isWritable(column);
    }

    @Override
    public boolean isDefinitelyWritable(int column) throws SQLException {
        return this.metaData.isDefinitelyWritable(column);
    }

    @Override
    public String getColumnClassName(int column) throws SQLException {
        return this.metaData.getColumnClassName(column);
    }
    private String parserGenericFieldName(String fieldName) {
        int index = -1;
        if((index = fieldName.indexOf('.')) < 0) {
            return fieldName;
        }

        return fieldName.substring(index + 1);
    }
}