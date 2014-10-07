package net.java.ao;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.Set;

import net.java.ao.db.EmbeddedDerbyDatabaseProvider;
import net.java.ao.db.HSQLDatabaseProvider;
import net.java.ao.db.MySQLDatabaseProvider;
import net.java.ao.db.OracleDatabaseProvider;
import net.java.ao.db.PostgreSQLDatabaseProvider;
import net.java.ao.db.SQLServerDatabaseProvider;
import net.java.ao.schema.IndexNameConverter;
import net.java.ao.schema.ddl.DDLIndex;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DatabaseProviders
{
    public static HSQLDatabaseProvider getHsqlDatabaseProvider()
    {
        return new HSQLDatabaseProvider(newDataSource(""));
    }

    public static PostgreSQLDatabaseProvider getPostgreSqlDatabaseProvider()
    {
        return new PostgreSQLDatabaseProvider(newDataSource("\""))
        {
            @Override
            protected boolean hasIndex(IndexNameConverter indexNameConverter, DDLIndex index)
            {
                return true;
            }

            @Override
            protected boolean hasIndex(String table, String index)
            {
                return true;
            }
        };
    }

    public static OracleDatabaseProvider getOracleDatabaseProvider()
    {
        return new OracleDatabaseProvider(newDataSource(""));
    }

    public static MySQLDatabaseProvider getMySqlDatabaseProvider()
    {
        return new MySQLDatabaseProvider(newDataSource(""))
        {
            @Override
            protected boolean hasIndex(IndexNameConverter indexNameConverter, DDLIndex index)
            {
                return true;
            }

            @Override
            protected boolean hasIndex(String table, String index)
            {
                return true;
            }
        };
    }

    public static SQLServerDatabaseProvider getMsSqlDatabaseProvider()
    {
        return new SQLServerDatabaseProvider(newDataSource(""))
        {
            @Override
            protected boolean hasIndex(IndexNameConverter indexNameConverter, DDLIndex index)
            {
                return true;
            }

            @Override
            protected boolean hasIndex(String table, String index)
            {
                return true;
            }
        };
    }

    public static EmbeddedDerbyDatabaseProvider getEmbeddedDerbyDatabaseProvider()
    {
        return new EmbeddedDerbyDatabaseProvider(newDataSource(""), "")
        {
            @Override
            protected void setPostConnectionProperties(Connection conn) throws SQLException
            {
                // nothing
            }
        };
    }

    public static DatabaseProvider getDatabaseProviderWithNoIndex()
    {
        return new DatabaseProvider(newDataSource(""), "")
        {
            @Override
            protected Set<String> getReservedWords()
            {
                return null;
            }

            @Override
            protected boolean hasIndex(String tableName, String indexName)
            {
                return false;
            }
        };
    }

    private static DisposableDataSource newDataSource(String quote)
    {
        final DisposableDataSource dataSource = mock(DisposableDataSource.class);
        final Connection connection = mock(Connection.class);
        final DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        try
        {
            when(dataSource.getConnection()).thenReturn(connection);
            when(connection.getMetaData()).thenReturn(metaData);
            when(metaData.getIdentifierQuoteString()).thenReturn(quote);
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
        return dataSource;
    }
}
