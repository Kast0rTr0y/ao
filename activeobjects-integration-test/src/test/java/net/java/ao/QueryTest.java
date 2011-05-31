/*
 * Copyright 2007 Daniel Spiewak
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 *	    http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.java.ao;

import net.java.ao.db.EmbeddedDerbyDatabaseProvider;
import net.java.ao.db.HSQLDatabaseProvider;
import net.java.ao.db.MySQLDatabaseProvider;
import net.java.ao.db.OracleDatabaseProvider;
import net.java.ao.db.PostgreSQLDatabaseProvider;
import net.java.ao.db.SQLServerDatabaseProvider;
import net.java.ao.it.DatabaseProcessor;
import net.java.ao.it.model.Comment;
import net.java.ao.it.model.Company;
import net.java.ao.it.model.Person;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/** @author Daniel Spiewak */
@Data(DatabaseProcessor.class)
public class QueryTest extends ActiveObjectsIntegrationTest
{
    private static final Query QUERY_1 = Query.select();
    private static final Query QUERY_2 = Query.select("ID,firstName,lastName");
    private static final Query QUERY_3 = Query.select().where("name IS NULL AND age = 3");
    private static final Query QUERY_4 = Query.select().order("name DESC");
    private static final Query QUERY_5 = Query.select().where("name IS NULL AND age = 3").limit(10);
    private static final Query QUERY_6 = Query.select().where("name IS NULL AND age = 3").limit(10).offset(4);
    private static final Query QUERY_7 = Query.select().where("name IS NULL AND age = 3").limit(4).group("age");
    private static final Query QUERY_8 = Query.select().join(Company.class).where("name IS NULL AND age = 3").group("url");

    private TableNameConverter tableNameConverter;
    private FieldNameConverter fieldNameConverter;

    @Before
    public void instanceSetUp()
    {
        tableNameConverter = entityManager.getTableNameConverter();
        fieldNameConverter = entityManager.getFieldNameConverter();
    }

    @Test
    public void testToSQL()
    {
        DatabaseProvider provider = DatabaseProviders.getEmbeddedDerbyDatabaseProvider();
        String personTableName = getTableName(provider, tableNameConverter, Person.class);
        String companyTableName = getTableName(provider, tableNameConverter, Company.class);
        String id = getFieldName(Person.class, "getID");

        assertSqlEquals(provider, QUERY_1, false, "SELECT %s FROM %s", id, personTableName);
        assertSqlEquals(provider, QUERY_2, false, "SELECT ID,firstName,lastName FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_3, false, "SELECT %s FROM %s WHERE name IS NULL AND age = 3", id, personTableName);
        assertSqlEquals(provider, QUERY_4, false, "SELECT %s FROM %s ORDER BY name DESC", id, personTableName);
        assertSqlEquals(provider, QUERY_5, false, "SELECT %s FROM %s WHERE name IS NULL AND age = 3", id, personTableName);
        assertSqlEquals(provider, QUERY_6, false, "SELECT %s FROM %s WHERE name IS NULL AND age = 3", id, personTableName);
        assertSqlEquals(provider, QUERY_7, false, "SELECT %s FROM %s WHERE name IS NULL AND age = 3 GROUP BY age", id, personTableName);
        assertSqlEquals(provider, QUERY_8, false, "SELECT %s FROM %s JOIN %s WHERE name IS NULL AND age = 3 GROUP BY url", id, personTableName, companyTableName);

        assertSqlEquals(provider, QUERY_1, true, "SELECT COUNT(*) FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_2, true, "SELECT COUNT(*) FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_3, true, "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3", personTableName);
        assertSqlEquals(provider, QUERY_4, true, "SELECT COUNT(*) FROM %s ORDER BY name DESC", personTableName);
        assertSqlEquals(provider, QUERY_5, true, "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3", personTableName);
        assertSqlEquals(provider, QUERY_6, true, "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3", personTableName);
        assertSqlEquals(provider, QUERY_7, true, "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3 GROUP BY age", personTableName);
        assertSqlEquals(provider, QUERY_8, true, "SELECT COUNT(*) FROM %s JOIN %s WHERE name IS NULL AND age = 3 GROUP BY url", personTableName, companyTableName);

        provider = DatabaseProviders.getHsqlDatabaseProvider();
        personTableName = getTableName(provider, tableNameConverter, Person.class);
        companyTableName = getTableName(provider, tableNameConverter, Company.class);

        assertSqlEquals(provider, QUERY_1, false, "SELECT %s FROM %s", id, personTableName);
        assertSqlEquals(provider, QUERY_2, false, "SELECT ID,firstName,lastName FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_3, false, "SELECT %s FROM %s WHERE name IS NULL AND age = 3", id, personTableName);
        assertSqlEquals(provider, QUERY_4, false, "SELECT %s FROM %s ORDER BY name DESC", id, personTableName);
        assertSqlEquals(provider, QUERY_5, false, "SELECT LIMIT 0 10 %s FROM %s WHERE name IS NULL AND age = 3", id, personTableName);
        assertSqlEquals(provider, QUERY_6, false, "SELECT LIMIT 4 10 %s FROM %s WHERE name IS NULL AND age = 3", id, personTableName);
        assertSqlEquals(provider, QUERY_7, false, "SELECT LIMIT 0 4 %s FROM %s WHERE name IS NULL AND age = 3 GROUP BY age", id, personTableName);
        assertSqlEquals(provider, QUERY_8, false, "SELECT %s FROM %s JOIN %s WHERE name IS NULL AND age = 3 GROUP BY url", id, personTableName, companyTableName);

        assertSqlEquals(provider, QUERY_1, true, "SELECT COUNT(*) FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_2, true, "SELECT COUNT(*) FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_3, true, "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3", personTableName);
        assertSqlEquals(provider, QUERY_4, true, "SELECT COUNT(*) FROM %s ORDER BY name DESC", personTableName);
        assertSqlEquals(provider, QUERY_5, true, "SELECT LIMIT 0 10 COUNT(*) FROM %s WHERE name IS NULL AND age = 3", personTableName);
        assertSqlEquals(provider, QUERY_6, true, "SELECT LIMIT 4 10 COUNT(*) FROM %s WHERE name IS NULL AND age = 3", personTableName);
        assertSqlEquals(provider, QUERY_7, true, "SELECT LIMIT 0 4 COUNT(*) FROM %s WHERE name IS NULL AND age = 3 GROUP BY age", personTableName);
        assertSqlEquals(provider, QUERY_8, true, "SELECT COUNT(*) FROM %s JOIN %s WHERE name IS NULL AND age = 3 GROUP BY url", personTableName, companyTableName);

        provider = DatabaseProviders.getMsSqlDatabaseProvider();
        personTableName = getTableName(provider, tableNameConverter, Person.class);
        companyTableName = getTableName(provider, tableNameConverter, Company.class);

        assertSqlEquals(provider, QUERY_1, false, "SELECT %s FROM %s", id, personTableName);
        assertSqlEquals(provider, QUERY_2, false, "SELECT ID,firstName,lastName FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_3, false, "SELECT %s FROM %s WHERE name IS NULL AND age = 3", id, personTableName);
        assertSqlEquals(provider, QUERY_4, false, "SELECT %s FROM %s ORDER BY name DESC", id, personTableName);
        assertSqlEquals(provider, QUERY_5, false, "SELECT TOP 10 %s FROM %s WHERE name IS NULL AND age = 3", id, personTableName);
        assertSqlEquals(provider, QUERY_6, false, "SELECT TOP 14 %s FROM %s WHERE name IS NULL AND age = 3", id, personTableName);
        assertSqlEquals(provider, QUERY_7, false, "SELECT TOP 4 %s FROM %s WHERE name IS NULL AND age = 3 GROUP BY age", id,  personTableName);
        assertSqlEquals(provider, QUERY_8, false, "SELECT %s FROM %s JOIN %s WHERE name IS NULL AND age = 3 GROUP BY url", id, personTableName, companyTableName);

        assertSqlEquals(provider, QUERY_1, true, "SELECT COUNT(*) FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_2, true, "SELECT COUNT(*) FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_3, true, "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3", personTableName);
        assertSqlEquals(provider, QUERY_4, true, "SELECT COUNT(*) FROM %s ORDER BY name DESC", personTableName);
        assertSqlEquals(provider, QUERY_5, true, "SELECT TOP 10 COUNT(*) FROM %s WHERE name IS NULL AND age = 3", personTableName);
        assertSqlEquals(provider, QUERY_6, true, "SELECT TOP 14 COUNT(*) FROM %s WHERE name IS NULL AND age = 3", personTableName);
        assertSqlEquals(provider, QUERY_7, true, "SELECT TOP 4 COUNT(*) FROM %s WHERE name IS NULL AND age = 3 GROUP BY age", personTableName);
        assertSqlEquals(provider, QUERY_8, true, "SELECT COUNT(*) FROM %s JOIN %s WHERE name IS NULL AND age = 3 GROUP BY url", personTableName, companyTableName);

        provider = DatabaseProviders.getMySqlDatabaseProvider();
        personTableName = getTableName(provider, tableNameConverter, Person.class);
        companyTableName = getTableName(provider, tableNameConverter, Company.class);

        assertSqlEquals(provider, QUERY_1, false, "SELECT %s FROM %s", id, personTableName);
        assertSqlEquals(provider, QUERY_2, false, "SELECT ID,firstName,lastName FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_3, false, "SELECT %s FROM %s WHERE name IS NULL AND age = 3", id, personTableName);
        assertSqlEquals(provider, QUERY_4, false, "SELECT %s FROM %s ORDER BY name DESC", id, personTableName);
        assertSqlEquals(provider, QUERY_5, false, "SELECT %s FROM %s WHERE name IS NULL AND age = 3 LIMIT 10", id, personTableName);
        assertSqlEquals(provider, QUERY_6, false, "SELECT %s FROM %s WHERE name IS NULL AND age = 3 LIMIT 10 OFFSET 4", id, personTableName);
        assertSqlEquals(provider, QUERY_7, false, "SELECT %s FROM %s WHERE name IS NULL AND age = 3 GROUP BY age LIMIT 4", id, personTableName);
        assertSqlEquals(provider, QUERY_8, false, "SELECT %s FROM %s JOIN %s WHERE name IS NULL AND age = 3 GROUP BY url", id, personTableName, companyTableName);

        assertSqlEquals(provider, QUERY_1, true, "SELECT COUNT(*) FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_2, true, "SELECT COUNT(*) FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_3, true, "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3", personTableName);
        assertSqlEquals(provider, QUERY_4, true, "SELECT COUNT(*) FROM %s ORDER BY name DESC", personTableName);
        assertSqlEquals(provider, QUERY_5, true, "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3 LIMIT 10", personTableName);
        assertSqlEquals(provider, QUERY_6, true, "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3 LIMIT 10 OFFSET 4", personTableName);
        assertSqlEquals(provider, QUERY_7, true, "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3 GROUP BY age LIMIT 4", personTableName);
        assertSqlEquals(provider, QUERY_8, true, "SELECT COUNT(*) FROM %s JOIN %s WHERE name IS NULL AND age = 3 GROUP BY url", personTableName, companyTableName);

        provider = DatabaseProviders.getOracleDatabaseProvider();
        personTableName = getTableName(provider, tableNameConverter, Person.class);
        companyTableName = getTableName(provider, tableNameConverter, Company.class);

        assertSqlEquals(provider, QUERY_1, false, "SELECT %s FROM %s", id, personTableName);
        assertSqlEquals(provider, QUERY_2, false, "SELECT ID,firstName,lastName FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_3, false, "SELECT %s FROM %s WHERE name IS NULL AND age = 3", id, personTableName);
        assertSqlEquals(provider, QUERY_4, false, "SELECT %s FROM %s ORDER BY name DESC", id, personTableName);
        assertSqlEquals(provider, QUERY_5, false, "SELECT %s FROM %s WHERE name IS NULL AND age = 3", id, personTableName);
        assertSqlEquals(provider, QUERY_6, false, "SELECT %s FROM %s WHERE name IS NULL AND age = 3", id, personTableName);
        assertSqlEquals(provider, QUERY_7, false, "SELECT %s FROM %s WHERE name IS NULL AND age = 3 GROUP BY age", id, personTableName);
        assertSqlEquals(provider, QUERY_8, false, "SELECT %s FROM %s JOIN %s WHERE name IS NULL AND age = 3 GROUP BY url", id, personTableName, companyTableName);

        assertSqlEquals(provider, QUERY_1, true, "SELECT COUNT(*) FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_2, true, "SELECT COUNT(*) FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_3, true, "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3", personTableName);
        assertSqlEquals(provider, QUERY_4, true, "SELECT COUNT(*) FROM %s ORDER BY name DESC", personTableName);
        assertSqlEquals(provider, QUERY_5, true, "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3", personTableName);
        assertSqlEquals(provider, QUERY_6, true, "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3", personTableName);
        assertSqlEquals(provider, QUERY_7, true, "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3 GROUP BY age", personTableName);
        assertSqlEquals(provider, QUERY_8, true, "SELECT COUNT(*) FROM %s JOIN %s WHERE name IS NULL AND age = 3 GROUP BY url", personTableName, companyTableName);

        provider = DatabaseProviders.getPostgreSqlDatabaseProvider();
        personTableName = getTableName(provider, tableNameConverter, Person.class);
        companyTableName = getTableName(provider, tableNameConverter, Company.class);

        assertSqlEquals(provider, QUERY_1, false, "SELECT '%s' FROM %s", id, personTableName);
        assertSqlEquals(provider, QUERY_2, false, "SELECT 'ID','firstName','lastName' FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_3, false, "SELECT '%s' FROM %s WHERE 'name' IS NULL AND 'age' = 3", id, personTableName);
        assertSqlEquals(provider, QUERY_4, false, "SELECT '%s' FROM %s ORDER BY 'name' DESC", id, personTableName);
        assertSqlEquals(provider, QUERY_5, false, "SELECT '%s' FROM %s WHERE 'name' IS NULL AND 'age' = 3 LIMIT 10", id, personTableName);
        assertSqlEquals(provider, QUERY_6, false, "SELECT '%s' FROM %s WHERE 'name' IS NULL AND 'age' = 3 LIMIT 10 OFFSET 4", id, personTableName);
        assertSqlEquals(provider, QUERY_7, false, "SELECT '%s' FROM %s WHERE 'name' IS NULL AND 'age' = 3 GROUP BY 'age' LIMIT 4", id, personTableName);
        assertSqlEquals(provider, QUERY_8, false, "SELECT '%s' FROM %s JOIN %s WHERE 'name' IS NULL AND 'age' = 3 GROUP BY 'url'", id, personTableName, companyTableName);

        assertSqlEquals(provider, QUERY_1, true, "SELECT COUNT(*) FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_2, true, "SELECT COUNT(*) FROM %s", personTableName);
        assertSqlEquals(provider, QUERY_3, true, "SELECT COUNT(*) FROM %s WHERE 'name' IS NULL AND 'age' = 3", personTableName);
        assertSqlEquals(provider, QUERY_4, true, "SELECT COUNT(*) FROM %s ORDER BY 'name' DESC", personTableName);
        assertSqlEquals(provider, QUERY_5, true, "SELECT COUNT(*) FROM %s WHERE 'name' IS NULL AND 'age' = 3 LIMIT 10", personTableName);
        assertSqlEquals(provider, QUERY_6, true, "SELECT COUNT(*) FROM %s WHERE 'name' IS NULL AND 'age' = 3 LIMIT 10 OFFSET 4", personTableName);
        assertSqlEquals(provider, QUERY_7, true, "SELECT COUNT(*) FROM %s WHERE 'name' IS NULL AND 'age' = 3 GROUP BY 'age' LIMIT 4", personTableName);
        assertSqlEquals(provider, QUERY_8, true, "SELECT COUNT(*) FROM %s JOIN %s WHERE 'name' IS NULL AND 'age' = 3 GROUP BY 'url'", personTableName, companyTableName);
    }

    private void assertSqlEquals(DatabaseProvider provider, Query query, boolean count, String expected, String... args)
    {
        assertEquals(String.format(expected, args), toSql(provider, query, count));
    }

    private String toSql(DatabaseProvider provider, Query query, boolean count)
    {
        return query.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, count);
    }

    @Test
    public void testLimitOffset() throws SQLException
    {
        Query query = Query.select().limit(3).offset(1);

        Comment[] unlimited = entityManager.find(Comment.class);
        Comment[] comments = entityManager.find(Comment.class, query);

        assertEquals(3, comments.length);
        assertEquals(unlimited[1].getID(), comments[0].getID());
        assertEquals(unlimited[2].getID(), comments[1].getID());
        assertEquals(unlimited[3].getID(), comments[2].getID());
    }

    private String getTableName(DatabaseProvider provider, TableNameConverter converter, Class<? extends RawEntity<?>> entityType)
    {
        return provider.processID(converter.getName(entityType));
    }

    private static class DatabaseProviders
    {
        public static HSQLDatabaseProvider getHsqlDatabaseProvider()
        {
            return new HSQLDatabaseProvider(newDataSource(""));
        }

        public static PostgreSQLDatabaseProvider getPostgreSqlDatabaseProvider()
        {
            return new PostgreSQLDatabaseProvider(newDataSource("'"));
        }

        public static OracleDatabaseProvider getOracleDatabaseProvider()
        {
            return new OracleDatabaseProvider(newDataSource(""));
        }

        public static MySQLDatabaseProvider getMySqlDatabaseProvider()
        {
            return new MySQLDatabaseProvider(newDataSource(""));
        }

        public static SQLServerDatabaseProvider getMsSqlDatabaseProvider()
        {
            return new SQLServerDatabaseProvider(newDataSource(""));
        }

        public static EmbeddedDerbyDatabaseProvider getEmbeddedDerbyDatabaseProvider()
        {
            return new EmbeddedDerbyDatabaseProvider(newDataSource(""), "");
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
}
