package com.ldz.datasource;

import java.sql.SQLException;

import org.apache.hive.jdbc.HiveConnection;
import org.apache.hive.jdbc.HivePreparedStatement;
import org.apache.hive.service.rpc.thrift.TSessionHandle;
import org.apache.hive.service.rpc.thrift.TCLIService.Iface;

public class GeneralHivePreparedStatement extends HivePreparedStatement {


    private int maxFieldSize = 1000;

    private int maxRows = 1000;

    private long largeMaxRows = 1000;

    public GeneralHivePreparedStatement(HiveConnection connection, Iface client, TSessionHandle sessHandle,
            String sql) {
        super(connection, client, sessHandle, sql);
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