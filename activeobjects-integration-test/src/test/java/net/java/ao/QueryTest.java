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
import net.java.ao.it.model.CompanyAddressInfo;
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
    private static final Query QUERY_8 = Query.select().join(Company.class, "person.companyId = company.id").where("name IS NULL AND age = 3").group("url");
    private static final Query QUERY_9 = Query.select().join(Company.class).join(CompanyAddressInfo.class).where("addressLine1 IS NULL");

    private TableNameConverter tableNameConverter;
    private FieldNameConverter fieldNameConverter;

    @Before
    public void instanceSetUp()
    {
        tableNameConverter = entityManager.getTableNameConverter();
        fieldNameConverter = entityManager.getFieldNameConverter();
    }

    @Test
    public void testToSqlWithPostgreSqlDatabaseProvider()
    {
        testToSql(DatabaseProviders.getPostgreSqlDatabaseProvider(),
                "SELECT '%s' FROM %s",
                "SELECT 'ID','firstName','lastName' FROM %s",
                "SELECT '%s' FROM %s WHERE 'name' IS NULL AND 'age' = 3",
                "SELECT '%s' FROM %s ORDER BY 'name' DESC",
                "SELECT '%s' FROM %s WHERE 'name' IS NULL AND 'age' = 3 LIMIT 10",
                "SELECT '%s' FROM %s WHERE 'name' IS NULL AND 'age' = 3 LIMIT 10 OFFSET 4",
                "SELECT '%s' FROM %s WHERE 'name' IS NULL AND 'age' = 3 GROUP BY 'age' LIMIT 4",
                "SELECT '%s' FROM %s JOIN %s ON person.'companyId' = company.'id' WHERE 'name' IS NULL AND 'age' = 3 GROUP BY 'url'",
                "SELECT '%s' FROM %s JOIN %s JOIN %s WHERE 'addressLine1' IS NULL",
                "SELECT COUNT(*) FROM %s",
                "SELECT COUNT(*) FROM %s",
                "SELECT COUNT(*) FROM %s WHERE 'name' IS NULL AND 'age' = 3",
                "SELECT COUNT(*) FROM %s ORDER BY 'name' DESC",
                "SELECT COUNT(*) FROM %s WHERE 'name' IS NULL AND 'age' = 3 LIMIT 10",
                "SELECT COUNT(*) FROM %s WHERE 'name' IS NULL AND 'age' = 3 LIMIT 10 OFFSET 4",
                "SELECT COUNT(*) FROM %s WHERE 'name' IS NULL AND 'age' = 3 GROUP BY 'age' LIMIT 4",
                "SELECT COUNT(*) FROM %s JOIN %s ON person.'companyId' = company.'id' WHERE 'name' IS NULL AND 'age' = 3 GROUP BY 'url'",
                "SELECT COUNT(*) FROM %s JOIN %s JOIN %s WHERE 'addressLine1' IS NULL");
    }

    @Test
    public void testToSqlWithEmbeddedDerbyDatabaseProvider()
    {
        testToSql(DatabaseProviders.getEmbeddedDerbyDatabaseProvider(),
                "SELECT %s FROM %s",
                "SELECT ID,firstName,lastName FROM %s",
                "SELECT %s FROM %s WHERE name IS NULL AND age = 3",
                "SELECT %s FROM %s ORDER BY name DESC",
                "SELECT %s FROM %s WHERE name IS NULL AND age = 3",
                "SELECT %s FROM %s WHERE name IS NULL AND age = 3",
                "SELECT %s FROM %s WHERE name IS NULL AND age = 3 GROUP BY age",
                "SELECT %s FROM %s JOIN %s ON person.companyId = company.id WHERE name IS NULL AND age = 3 GROUP BY url",
                "SELECT %s FROM %s JOIN %s JOIN %s WHERE addressLine1 IS NULL",
                "SELECT COUNT(*) FROM %s",
                "SELECT COUNT(*) FROM %s",
                "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3",
                "SELECT COUNT(*) FROM %s ORDER BY name DESC",
                "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3",
                "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3",
                "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3 GROUP BY age",
                "SELECT COUNT(*) FROM %s JOIN %s ON person.companyId = company.id WHERE name IS NULL AND age = 3 GROUP BY url",
                "SELECT COUNT(*) FROM %s JOIN %s JOIN %s WHERE addressLine1 IS NULL");
    }

    @Test
    public void testToSqlWithHsqlDatabaseProvider()
    {
        testToSql(DatabaseProviders.getHsqlDatabaseProvider(),
                "SELECT %s FROM %s",
                "SELECT ID,firstName,lastName FROM %s",
                "SELECT %s FROM %s WHERE name IS NULL AND age = 3",
                "SELECT %s FROM %s ORDER BY name DESC",
                "SELECT LIMIT 0 10 %s FROM %s WHERE name IS NULL AND age = 3",
                "SELECT LIMIT 4 10 %s FROM %s WHERE name IS NULL AND age = 3",
                "SELECT LIMIT 0 4 %s FROM %s WHERE name IS NULL AND age = 3 GROUP BY age",
                "SELECT %s FROM %s JOIN %s ON person.companyId = company.id WHERE name IS NULL AND age = 3 GROUP BY url",
                "SELECT %s FROM %s JOIN %s JOIN %s WHERE addressLine1 IS NULL",
                "SELECT COUNT(*) FROM %s",
                "SELECT COUNT(*) FROM %s",
                "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3",
                "SELECT COUNT(*) FROM %s ORDER BY name DESC",
                "SELECT LIMIT 0 10 COUNT(*) FROM %s WHERE name IS NULL AND age = 3",
                "SELECT LIMIT 4 10 COUNT(*) FROM %s WHERE name IS NULL AND age = 3",
                "SELECT LIMIT 0 4 COUNT(*) FROM %s WHERE name IS NULL AND age = 3 GROUP BY age",
                "SELECT COUNT(*) FROM %s JOIN %s ON person.companyId = company.id WHERE name IS NULL AND age = 3 GROUP BY url",
                "SELECT COUNT(*) FROM %s JOIN %s JOIN %s WHERE addressLine1 IS NULL");
    }

    @Test
    public void testToSqlWithMsSqlDatabaseProvider()
    {
        testToSql(DatabaseProviders.getMsSqlDatabaseProvider(),
                "SELECT %s FROM %s",
                "SELECT ID,firstName,lastName FROM %s",
                "SELECT %s FROM %s WHERE name IS NULL AND age = 3",
                "SELECT %s FROM %s ORDER BY name DESC",
                "SELECT TOP 10 %s FROM %s WHERE name IS NULL AND age = 3",
                "SELECT TOP 14 %s FROM %s WHERE name IS NULL AND age = 3",
                "SELECT TOP 4 %s FROM %s WHERE name IS NULL AND age = 3 GROUP BY age",
                "SELECT %s FROM %s JOIN %s ON person.companyId = company.id WHERE name IS NULL AND age = 3 GROUP BY url",
                "SELECT %s FROM %s JOIN %s JOIN %s WHERE addressLine1 IS NULL",
                "SELECT COUNT(*) FROM %s",
                "SELECT COUNT(*) FROM %s",
                "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3",
                "SELECT COUNT(*) FROM %s ORDER BY name DESC",
                "SELECT TOP 10 COUNT(*) FROM %s WHERE name IS NULL AND age = 3",
                "SELECT TOP 14 COUNT(*) FROM %s WHERE name IS NULL AND age = 3",
                "SELECT TOP 4 COUNT(*) FROM %s WHERE name IS NULL AND age = 3 GROUP BY age",
                "SELECT COUNT(*) FROM %s JOIN %s ON person.companyId = company.id WHERE name IS NULL AND age = 3 GROUP BY url",
                "SELECT COUNT(*) FROM %s JOIN %s JOIN %s WHERE addressLine1 IS NULL");
    }

    @Test
    public void testToSqlWithMySqlDatabaseProvider()
    {
        testToSql(DatabaseProviders.getMySqlDatabaseProvider(),
                "SELECT %s FROM %s",
                "SELECT ID,firstName,lastName FROM %s",
                "SELECT %s FROM %s WHERE name IS NULL AND age = 3",
                "SELECT %s FROM %s ORDER BY name DESC",
                "SELECT %s FROM %s WHERE name IS NULL AND age = 3 LIMIT 10",
                "SELECT %s FROM %s WHERE name IS NULL AND age = 3 LIMIT 10 OFFSET 4",
                "SELECT %s FROM %s WHERE name IS NULL AND age = 3 GROUP BY age LIMIT 4",
                "SELECT %s FROM %s JOIN %s ON person.companyId = company.id WHERE name IS NULL AND age = 3 GROUP BY url",
                "SELECT %s FROM %s JOIN %s JOIN %s WHERE addressLine1 IS NULL",
                "SELECT COUNT(*) FROM %s",
                "SELECT COUNT(*) FROM %s",
                "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3",
                "SELECT COUNT(*) FROM %s ORDER BY name DESC",
                "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3 LIMIT 10",
                "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3 LIMIT 10 OFFSET 4",
                "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3 GROUP BY age LIMIT 4",
                "SELECT COUNT(*) FROM %s JOIN %s ON person.companyId = company.id WHERE name IS NULL AND age = 3 GROUP BY url",
                "SELECT COUNT(*) FROM %s JOIN %s JOIN %s WHERE addressLine1 IS NULL");
    }

    @Test
    public void testToSqlWithOracleDatabaseProvider()
    {
        testToSql(DatabaseProviders.getOracleDatabaseProvider(),
                "SELECT %s FROM %s",
                "SELECT ID,firstName,lastName FROM %s",
                "SELECT %s FROM %s WHERE name IS NULL AND age = 3",
                "SELECT %s FROM %s ORDER BY name DESC",
                "SELECT %s FROM %s WHERE name IS NULL AND age = 3",
                "SELECT %s FROM %s WHERE name IS NULL AND age = 3",
                "SELECT %s FROM %s WHERE name IS NULL AND age = 3 GROUP BY age",
                "SELECT %s FROM %s JOIN %s ON person.companyId = company.id WHERE name IS NULL AND age = 3 GROUP BY url",
                "SELECT %s FROM %s JOIN %s JOIN %s WHERE addressLine1 IS NULL",
                "SELECT COUNT(*) FROM %s",
                "SELECT COUNT(*) FROM %s",
                "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3",
                "SELECT COUNT(*) FROM %s ORDER BY name DESC",
                "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3",
                "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3",
                "SELECT COUNT(*) FROM %s WHERE name IS NULL AND age = 3 GROUP BY age",
                "SELECT COUNT(*) FROM %s JOIN %s ON person.companyId = company.id WHERE name IS NULL AND age = 3 GROUP BY url",
                "SELECT COUNT(*) FROM %s JOIN %s JOIN %s WHERE addressLine1 IS NULL");
    }

    private void testToSql(DatabaseProvider provider, String... sql)
    {
        final String personTableName = getTableName(provider, tableNameConverter, Person.class);
        final String companyTableName = getTableName(provider, tableNameConverter, Company.class);
        final String companyAdressInfoTableName = getTableName(provider, tableNameConverter, CompanyAddressInfo.class);
        final String id = getFieldName(Person.class, "getID");

        assertSqlEquals(provider, QUERY_1, false, sql[0], id, personTableName);
        assertSqlEquals(provider, QUERY_2, false, sql[1], personTableName);
        assertSqlEquals(provider, QUERY_3, false, sql[2], id, personTableName);
        assertSqlEquals(provider, QUERY_4, false, sql[3], id, personTableName);
        assertSqlEquals(provider, QUERY_5, false, sql[4], id, personTableName);
        assertSqlEquals(provider, QUERY_6, false, sql[5], id, personTableName);
        assertSqlEquals(provider, QUERY_7, false, sql[6], id, personTableName);
        assertSqlEquals(provider, QUERY_8, false, sql[7], id, personTableName, companyTableName);
        assertSqlEquals(provider, QUERY_9, false, sql[8], id, personTableName, companyTableName, companyAdressInfoTableName);

        assertSqlEquals(provider, QUERY_1, true, sql[9], personTableName);
        assertSqlEquals(provider, QUERY_2, true, sql[10], personTableName);
        assertSqlEquals(provider, QUERY_3, true, sql[11], personTableName);
        assertSqlEquals(provider, QUERY_4, true, sql[12], personTableName);
        assertSqlEquals(provider, QUERY_5, true, sql[13], personTableName);
        assertSqlEquals(provider, QUERY_6, true, sql[14], personTableName);
        assertSqlEquals(provider, QUERY_7, true, sql[15], personTableName);
        assertSqlEquals(provider, QUERY_8, true, sql[16], personTableName, companyTableName);
        assertSqlEquals(provider, QUERY_9, true, sql[17], personTableName, companyTableName, companyAdressInfoTableName);
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
        return provider.withSchema(converter.getName(entityType));
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
