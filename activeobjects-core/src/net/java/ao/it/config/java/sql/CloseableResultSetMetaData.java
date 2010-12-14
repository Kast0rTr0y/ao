package net.java.ao.it.config.java.sql;

import java.sql.ResultSetMetaData;

public interface CloseableResultSetMetaData extends ResultSetMetaData
{
    void close();
}
