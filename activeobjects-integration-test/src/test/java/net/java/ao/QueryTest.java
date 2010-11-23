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
import net.java.ao.it.config.DynamicJdbcConfiguration;
import net.java.ao.it.model.Comment;
import net.java.ao.it.model.Company;
import net.java.ao.it.model.Person;
import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.Jdbc;
import org.junit.Before;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

/**
 * @author Daniel Spiewak
 */
@Jdbc(DynamicJdbcConfiguration.class)
@Data(DatabaseProcessor.class)
public class QueryTest extends ActiveObjectsIntegrationTest
{
    private static final Query QUERY_1 = Query.select();
    private static final Query QUERY_2 = Query.select("id,firstName,lastName");
    private static final Query QUERY_3 = Query.select().where("name IS NULL AND age = 3");
    private static final Query QUERY_4 = Query.select().order("name DESC");
    private static final Query QUERY_5 = Query.select().where("name IS NULL AND age = 3").limit(10);
    private static final Query QUERY_6 = Query.select().where("name IS NULL AND age = 3").limit(10).offset(4);
    private static final Query QUERY_7 = Query.select().where("name IS NULL AND age = 3").limit(4).group("age");
    private static final Query QUERY_8 = Query.select().join(Company.class).where("name IS NULL AND age = 3").group("url");

    private DisposableDataSource dataSource;
    private TableNameConverter tableNameConverter;
    private FieldNameConverter fieldNameConverter;

    @Before
    public void instanceSetUp()
    {
        tableNameConverter = entityManager.getTableNameConverter();
        fieldNameConverter = entityManager.getFieldNameConverter();
        dataSource = mock(DisposableDataSource.class);
    }

    @Test
    public void testToSQL()
    {
        DatabaseProvider provider = new EmbeddedDerbyDatabaseProvider(dataSource, "");
        String personTableName = getTableName(provider, tableNameConverter, Person.class);
        String companyTableName = getTableName(provider, tableNameConverter, Company.class);

        assertEquals("SELECT id FROM " + personTableName, QUERY_1.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id,firstName,lastName FROM " + personTableName, QUERY_2.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_3.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " ORDER BY name DESC", QUERY_4.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_5.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_6.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3 GROUP BY age", QUERY_7.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " JOIN " + companyTableName + " WHERE name IS NULL AND age = 3 GROUP BY url", QUERY_8.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));

        assertEquals("SELECT COUNT(*) FROM " + personTableName, QUERY_1.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName, QUERY_2.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_3.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " ORDER BY name DESC", QUERY_4.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_5.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_6.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3 GROUP BY age", QUERY_7.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " JOIN " + companyTableName + " WHERE name IS NULL AND age = 3 GROUP BY url", QUERY_8.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));

        provider = new HSQLDatabaseProvider(dataSource);
        personTableName = getTableName(provider, tableNameConverter, Person.class);
        companyTableName = getTableName(provider, tableNameConverter, Company.class);

        assertEquals("SELECT id FROM " + personTableName, QUERY_1.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id,firstName,lastName FROM " + personTableName, QUERY_2.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_3.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " ORDER BY name DESC", QUERY_4.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT LIMIT 0 10 id FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_5.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT LIMIT 4 10 id FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_6.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT LIMIT 0 4 id FROM " + personTableName + " WHERE name IS NULL AND age = 3 GROUP BY age", QUERY_7.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " JOIN " + companyTableName + " WHERE name IS NULL AND age = 3 GROUP BY url", QUERY_8.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));

        assertEquals("SELECT COUNT(*) FROM " + personTableName, QUERY_1.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName, QUERY_2.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_3.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " ORDER BY name DESC", QUERY_4.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT LIMIT 0 10 COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_5.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT LIMIT 4 10 COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_6.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT LIMIT 0 4 COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3 GROUP BY age", QUERY_7.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " JOIN " + companyTableName + " WHERE name IS NULL AND age = 3 GROUP BY url", QUERY_8.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));

        provider = new SQLServerDatabaseProvider(dataSource);
        personTableName = getTableName(provider, tableNameConverter, Person.class);
        companyTableName = getTableName(provider, tableNameConverter, Company.class);

        assertEquals("SELECT id FROM " + personTableName, QUERY_1.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id,firstName,lastName FROM " + personTableName, QUERY_2.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_3.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " ORDER BY name DESC", QUERY_4.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT TOP 10 id FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_5.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT TOP 14 id FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_6.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT TOP 4 id FROM " + personTableName + " WHERE name IS NULL AND age = 3 GROUP BY age", QUERY_7.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " JOIN " + companyTableName + " WHERE name IS NULL AND age = 3 GROUP BY url", QUERY_8.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));

        assertEquals("SELECT COUNT(*) FROM " + personTableName, QUERY_1.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName, QUERY_2.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_3.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " ORDER BY name DESC", QUERY_4.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT TOP 10 COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_5.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT TOP 14 COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_6.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT TOP 4 COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3 GROUP BY age", QUERY_7.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " JOIN " + companyTableName + " WHERE name IS NULL AND age = 3 GROUP BY url", QUERY_8.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));

        provider = new MySQLDatabaseProvider(dataSource);
        personTableName = getTableName(provider, tableNameConverter, Person.class);
        companyTableName = getTableName(provider, tableNameConverter, Company.class);

        assertEquals("SELECT id FROM " + personTableName, QUERY_1.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id,firstName,lastName FROM " + personTableName, QUERY_2.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_3.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " ORDER BY name DESC", QUERY_4.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3 LIMIT 10", QUERY_5.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3 LIMIT 10 OFFSET 4", QUERY_6.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3 GROUP BY age LIMIT 4", QUERY_7.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " JOIN " + companyTableName + " WHERE name IS NULL AND age = 3 GROUP BY url", QUERY_8.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));

        assertEquals("SELECT COUNT(*) FROM " + personTableName, QUERY_1.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName, QUERY_2.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_3.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " ORDER BY name DESC", QUERY_4.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3 LIMIT 10", QUERY_5.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3 LIMIT 10 OFFSET 4", QUERY_6.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3 GROUP BY age LIMIT 4", QUERY_7.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " JOIN " + companyTableName + " WHERE name IS NULL AND age = 3 GROUP BY url", QUERY_8.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));

        provider = new OracleDatabaseProvider(dataSource);
        personTableName = getTableName(provider, tableNameConverter, Person.class);
        companyTableName = getTableName(provider, tableNameConverter, Company.class);

        assertEquals("SELECT id FROM " + personTableName, QUERY_1.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id,firstName,lastName FROM " + personTableName, QUERY_2.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_3.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " ORDER BY name DESC", QUERY_4.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_5.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_6.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3 GROUP BY age", QUERY_7.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " JOIN " + companyTableName + " WHERE name IS NULL AND age = 3 GROUP BY url", QUERY_8.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));

        assertEquals("SELECT COUNT(*) FROM " + personTableName, QUERY_1.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName, QUERY_2.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_3.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " ORDER BY name DESC", QUERY_4.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_5.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_6.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3 GROUP BY age", QUERY_7.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " JOIN " + companyTableName + " WHERE name IS NULL AND age = 3 GROUP BY url", QUERY_8.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));

        provider = new PostgreSQLDatabaseProvider(dataSource);
        personTableName = getTableName(provider, tableNameConverter, Person.class);
        companyTableName = getTableName(provider, tableNameConverter, Company.class);

        assertEquals("SELECT id FROM " + personTableName, QUERY_1.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id,firstName,lastName FROM " + personTableName, QUERY_2.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_3.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " ORDER BY name DESC", QUERY_4.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3 LIMIT 10", QUERY_5.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3 LIMIT 10 OFFSET 4", QUERY_6.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " WHERE name IS NULL AND age = 3 GROUP BY age LIMIT 4", QUERY_7.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));
        assertEquals("SELECT id FROM " + personTableName + " JOIN " + companyTableName + " WHERE name IS NULL AND age = 3 GROUP BY url", QUERY_8.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, false));

        assertEquals("SELECT COUNT(*) FROM " + personTableName, QUERY_1.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName, QUERY_2.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3", QUERY_3.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " ORDER BY name DESC", QUERY_4.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3 LIMIT 10", QUERY_5.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3 LIMIT 10 OFFSET 4", QUERY_6.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " WHERE name IS NULL AND age = 3 GROUP BY age LIMIT 4", QUERY_7.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
        assertEquals("SELECT COUNT(*) FROM " + personTableName + " JOIN " + companyTableName + " WHERE name IS NULL AND age = 3 GROUP BY url", QUERY_8.toSQL(Person.class, provider, tableNameConverter, fieldNameConverter, true));
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
}
