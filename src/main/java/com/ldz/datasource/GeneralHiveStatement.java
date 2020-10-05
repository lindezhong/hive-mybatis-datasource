package com.ldz.datasource;

import java.sql.SQLException;

import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HiveStatement;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.hive.service.rpc.thrift.TCLIService.Iface;

public class GeneralHiveStatement extends HiveStatement {

    private int maxFieldSize = 1000;

    private int maxRows = 1000;

    private long largeMaxRows = 1000;

    public GeneralHiveStatement(final HiveConnection connection, final Iface client, final TSessionHandle sessHandle) {
        super(connection, client, sessHandle);
    }

    public GeneralHiveStatement(final HiveConnection connection, final Iface client, final TSessionHandle sessHandle,
            final int fetchSize) {
        super(connection, client, sessHandle, fetchSize);
    }

    public GeneralHiveStatement(final HiveConnection connection, final Iface client, final TSessionHandle sessHandle,
            final boolean isScrollableResultset) {
        super(connection, client, sessHandle, isScrollableResultset);
    }

    public GeneralHiveStatement(final HiveConnection connection, final Iface client, final TSessionHandle sessHandle,
            final boolean isScrollableResultset, final int fetchSize) {
        super(connection, client, sessHandle, isScrollableResultset, fetchSize);
    }

    /**
     * @return int return the maxFieldSize
     */

    @Override
    public int getMaxFieldSize() {
        return maxFieldSize;
    }

    /**
     * @param maxFieldSize the maxFieldSize to set
     */

    @Override
    public void setMaxFieldSize(final int maxFieldSize) {
        this.maxFieldSize = maxFieldSize;
    }

    /**
     * @return int return the maxRows
     */

    @Override
    public int getMaxRows() {
        return maxRows;
    }

    /**
     * @param maxRows the maxRows to set
     */

    @Override
    public void setMaxRows(final int maxRows) {
        this.maxRows = maxRows;
    }

    /**
     * @return int return the largeMaxRows
     */

    @Override
    public long getLargeMaxRows() {
        return largeMaxRows;
    }

    @Override
    public void setLargeMaxRows(long largeMaxRows) throws SQLException {
        this.largeMaxRows = largeMaxRows;
    }



}