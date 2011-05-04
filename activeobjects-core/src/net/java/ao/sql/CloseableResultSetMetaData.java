package net.java.ao.sql;

import java.sql.ResultSetMetaData;

public interface CloseableResultSetMetaData extends ResultSetMetaData
{
    void close();
}
