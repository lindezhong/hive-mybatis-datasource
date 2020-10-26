package com.ldz.mybatis;

import org.apache.ibatis.executor.resultset.DefaultResultSetHandler;
import org.apache.ibatis.executor.resultset.ResultSetHandler;
import org.apache.ibatis.plugin.*;

import java.sql.ResultSet;
import java.sql.Statement;
import java.util.*;

import com.ldz.datasource.HiveNameStatement;

/**
 * MyBatis 插件 : 使得
 *
 */
@Intercepts(
    @Signature(type = ResultSetHandler.class, method = "handleResultSets", args = {Statement.class})
)

public class CameHiveNameInterceptor implements Interceptor {

    public Object intercept(Invocation invocation) throws Throwable {
        if(!(invocation.getTarget() instanceof DefaultResultSetHandler) || invocation.getArgs() == null 
            || invocation.getArgs().length != 1 || !(invocation.getArgs()[0] instanceof Statement)) {
            return invocation.proceed();
        }

        Statement statement = (Statement) invocation.getArgs()[0];
        HiveNameStatement hiveNameStatement = new HiveNameStatement(statement);

        // 封装hivenamestatement使得支持hive select * 的时候能将 tablename.fieldname -> filldname 格式 
        return invocation.getMethod().invoke(invocation.getTarget(), new Object[]{hiveNameStatement});

    }

    public Object plugin(Object target) {
        if (target instanceof ResultSetHandler) {
            return Plugin.wrap(target, this);
        }
        return target;
    }

    public void setProperties(Properties properties) {
    }
}