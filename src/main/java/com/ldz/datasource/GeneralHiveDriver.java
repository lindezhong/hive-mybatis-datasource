package com.ldz.datasource;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

import org.apache.hive.jdbc.HiveDriver;

public class GeneralHiveDriver extends HiveDriver {

    @Override
    public Connection connect(String url, Properties info) throws SQLException {  
        return acceptsURL(url) ? new GeneralHiveConnection(url, info) : null;
    }
    
}