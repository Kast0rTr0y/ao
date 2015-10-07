package net.java.ao.db;

import net.java.ao.DisposableDataSource;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;

public class MsJdbcSQLServerDatabaseProvider extends SQLServerDatabaseProvider {

    public MsJdbcSQLServerDatabaseProvider(DisposableDataSource dataSource, String schema) {
        super(dataSource, schema);
    }

    @Override
    public void putNull(PreparedStatement stmt, int index) throws SQLException {
        stmt.setNull(index, Types.VARCHAR);
    }
}
