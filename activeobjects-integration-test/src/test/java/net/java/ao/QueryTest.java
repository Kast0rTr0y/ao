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

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;

import org.junit.Test;

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
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;

import static java.lang.String.format;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@Data(DatabaseProcessor.class)
public abstract class QueryTest extends ActiveObjectsIntegrationTest
{
    protected abstract DatabaseProvider getDatabaseProvider();

    @Test
    public final void testSimpleSelect()
    {
        assertSelectSqlEquals(getSimpleSelectQuery(), getExpectedSqlForSimpleSelect());
    }

    @Test
    public final void testSimpleCount()
    {
        assertCountSqlEquals(getSimpleSelectQuery(), getExpectedSqlForSimpleCount());
    }

    private Query getSimpleSelectQuery()
    {
        return Query.select();
    }

    protected abstract String getExpectedSqlForSimpleSelect();

    protected abstract String getExpectedSqlForSimpleCount();

    @Test
    public final void testSelectSomeFields()
    {
        assertSelectSqlEquals(getSelectSomeFieldsQuery(), getExpectSqlForSelectSomeFields());
    }

    @Test
    public final void testCountSomeFields()
    {
        assertCountSqlEquals(getSelectSomeFieldsQuery(), getExpectSqlForCountSomeFields());
    }

    private Query getSelectSomeFieldsQuery()
    {
        return Query.select(getPersonId() + ", " + getPersonFirstName() + ", " + getPersonLastName());
    }

    protected abstract String getExpectSqlForSelectSomeFields();

    protected abstract String getExpectSqlForCountSomeFields();

    @Test
    public final void testSelectWithWhereClause()
    {
        assertSelectSqlEquals(getSelectWithWhereClauseForQuery(), getExpectedSqlForSelectWithWhereClause());
    }

    @Test
    public final void testCountWithWhereClause()
    {
        assertCountSqlEquals(getSelectWithWhereClauseForQuery(), getExpectedSqlForCountWithWhereClause());
    }

    private Query getSelectWithWhereClauseForQuery()
    {
        return Query.select().where(getPersonLastName() + " IS NULL AND " + getPersonAge() + " = 3");
    }

    protected abstract String getExpectedSqlForSelectWithWhereClause();

    protected abstract String getExpectedSqlForCountWithWhereClause();

    @Test
    public final void testSelectWithOrderClause()
    {
        assertSelectSqlEquals(getSelectWithOrderClauseQuery(), getExpectedSqlForSelectWithOrderClause());
    }

    @Test
    public final void testSelectWithMultipleOrderClauses()
    {
        assertSelectSqlEquals(getSelectWithMultipleOrderClausesQuery(), getExpectedSqlForSelectWithMultipleOrderClauses());
    }

    @Test
    public final void testCountWithOrderClause()
    {
        assertCountSqlEquals(getSelectWithOrderClauseQuery(), getExpectedSqlForCountWithOrderClause());
    }

    @Test
    public final void testCountWithMultipleOrderClauses()
    {
        assertCountSqlEquals(getSelectWithMultipleOrderClausesQuery(), getExpectedSqlForCountWithMultipleOrderClauses());
    }

    private Query getSelectWithOrderClauseQuery()
    {
        return Query.select().order(getPersonLastName() + " DESC");
    }

    private Query getSelectWithMultipleOrderClausesQuery()
    {
        //return Query.select().order(getPersonLastName() + " DESC").order(getPersonAge() + " ASC").order(getPersonId() + " ASC");
        return Query.select().order(getPersonLastName() + " DESC, " + getPersonAge() + " ASC, " + getPersonId() + " ASC");
    }

    protected abstract String getExpectedSqlForSelectWithOrderClause();

    protected String getExpectedSqlForSelectWithMultipleOrderClauses()
    {
        return format("SELECT %s FROM %s ORDER BY %s DESC, %s ASC, %s ASC", getPersonId(), getExpectedTableName(Person.class), getPersonLastName(), getPersonAge(), getPersonId());
    }


    protected abstract String getExpectedSqlForCountWithOrderClause();

    protected String getExpectedSqlForCountWithMultipleOrderClauses()
    {
        return format("SELECT COUNT(*) FROM %s ORDER BY %s DESC, %s ASC, %s ASC", getExpectedTableName(Person.class), getPersonLastName(), getPersonAge(), getPersonId());
    }

    @Test
    public final void testSelectWithLimit()
    {
        assertSelectSqlEquals(getSelectWithLimitQuery(), getExpectedSqlForSelectWithLimit());
    }

    @Test
    public final void testCountWithLimit()
    {
        assertCountSqlEquals(getSelectWithLimitQuery(), getExpectedSqlForCountWithLimit());
    }

    private Query getSelectWithLimitQuery()
    {
        return Query.select().where(getPersonLastName() + " IS NULL AND " + getPersonAge() + " = 3").limit(10);
    }

    protected abstract String getExpectedSqlForSelectWithLimit();

    protected abstract String getExpectedSqlForCountWithLimit();

    @Test
    public final void testSelectWithLimitAndOffset()
    {
        assertSelectSqlEquals(getSelectWithLimitAndOffsetQuery(), getExpectedSqlForSelectWithLimitAndOffset());
    }

    @Test
    public final void testCountWithLimitAndOffset()
    {
        assertCountSqlEquals(getSelectWithLimitAndOffsetQuery(), getExpectedSqlForCountWithLimitAndOffset());
    }

    private Query getSelectWithLimitAndOffsetQuery()
    {
        return Query.select().where(getPersonLastName() + " IS NULL AND " + getPersonAge() + " = 3").limit(10).offset(4);
    }

    protected abstract String getExpectedSqlForSelectWithLimitAndOffset();

    protected abstract String getExpectedSqlForCountWithLimitAndOffset();

    @Test
    public final void testSelectWithGroupBy()
    {
        assertSelectSqlEquals(getSelectWithGroupByQuery(), getExpectedSqlForSelectWithGroupBy());
    }

    @Test
    public final void testCountWithGroupBy()
    {
        assertCountSqlEquals(getSelectWithGroupByQuery(), getExpectedSqlForCountWithGroupBy());
    }

    private Query getSelectWithGroupByQuery()
    {
        return Query.select().where(getPersonLastName() + " IS NULL AND " + getPersonAge() + " = 3").limit(4).group(getPersonAge());
    }

    protected abstract String getExpectedSqlForSelectWithGroupBy();

    protected abstract String getExpectedSqlForCountWithGroupBy();

    @Test
    public final void testSelectWithExplicitJoin()
    {
        assertSelectSqlEquals(getSelectWithExplicitJoinQuery(), getExpectedSqlForSelectWithExplicitJoin());
    }

    @Test
    public final void testCountWithExplicitJoin()
    {
        assertCountSqlEquals(getSelectWithExplicitJoinQuery(), getExpectedSqlForCountWithExplicitJoin());
    }

    private Query getSelectWithExplicitJoinQuery()
    {
        return Query.select().join(Company.class, getTableNameForQuery(Person.class) + "." + getPersonCompany() + " = " + getTableNameForQuery(Company.class) + "." + getCompanyId()).where(getPersonLastName() + " IS NULL AND " + getPersonAge() + " = 3").group(getPersonUrl());
    }

    protected abstract String getExpectedSqlForSelectWithExplicitJoin();

    protected abstract String getExpectedSqlForCountWithExplicitJoin();

    @Test
    public final void testSelectWithDefaultJoin()
    {
        assertSelectSqlEquals(getSelectWithDefaultJoinQuery(), getExpectedSqlForSelectWithDefaultJoin());
    }

    @Test
    public final void testCountWithDefaultJoin()
    {
        assertCountSqlEquals(getSelectWithDefaultJoinQuery(), getExpectedSqlForCountWithDefaultJoin());
    }

    private Query getSelectWithDefaultJoinQuery()
    {
        return Query.select().join(Company.class).join(CompanyAddressInfo.class).where(getCompanyAddressInfoLine1() + " IS NULL");
    }

    protected abstract String getExpectedSqlForSelectWithDefaultJoin();

    protected abstract String getExpectedSqlForCountWithDefaultJoin();

    @Test
    public final void testSelectWithAliasedJoin()
    {
        assertSelectSqlEquals(getSelectWithAliasedJoinQuery(), getExpectedSqlForSelectWithAliasedJoin());
    }

    private Query getSelectWithAliasedJoinQuery()
    {
        return Query.select()
                .alias(Person.class, "p").alias(Company.class, "c").alias(CompanyAddressInfo.class, "ca")
                .join(Company.class).join(CompanyAddressInfo.class).where("ca." + getCompanyAddressInfoLine1() + " IS NULL");
    }

    protected abstract String getExpectedSqlForSelectWithAliasedJoin();

    @Test
    public final void testSelectWithAliasedJoinAndSomeFields()
    {
        assertSelectSqlEquals(getSelectWithAliasedJoinAndSomeFields(), getExpectedSqlForSelectWithAliasedJoinAndSomeFields());
    }

    private Query getSelectWithAliasedJoinAndSomeFields()
    {
        return Query.select(getPersonId() + ", " + getPersonFirstName() + ", " + getPersonLastName())
                .alias(Person.class, "p").alias(Company.class, "c").alias(CompanyAddressInfo.class, "ca")
                .join(Company.class).join(CompanyAddressInfo.class).where("ca." + getCompanyAddressInfoLine1() + " IS NULL");
    }

    protected abstract String getExpectedSqlForSelectWithAliasedJoinAndSomeFields();

    @Test
    public final void testSelectWithAliasedExplicitJoin()
    {
        assertSelectSqlEquals(getSelectWithAliasedExplicitJoinQuery(), getExpectedSqlForSelectWithAliasedExplicitJoin());
    }

    private Query getSelectWithAliasedExplicitJoinQuery()
    {
        return Query.select()
                .alias(Person.class, "p").alias(Company.class, "c")
                .join(Company.class, "p." + getPersonCompany() + " = " + "c." + getCompanyId()).where("p." + getPersonLastName() + " IS NULL AND p." + getPersonAge() + " = 3")
                .group("p." + getPersonUrl());
    }

    protected abstract String getExpectedSqlForSelectWithAliasedExplicitJoin();

    private String getTableNameForQuery(Class<? extends RawEntity<?>> clazz)
    {
        final String schema = getDatabaseProvider().getSchema();
        final String tableName = entityManager.getTableNameConverter().getName(clazz);
        return schema == null ? tableName : schema + "." + tableName;
    }

    protected final String getExpectedTableName(Class<? extends RawEntity<?>> clazz)
    {
        return getDatabaseProvider().withSchema(entityManager.getTableNameConverter().getName(clazz));
    }

    protected final String getPersonId()
    {
        return getFieldName(Person.class, "getID");
    }

    protected final String getPersonFirstName()
    {
        return getFieldName(Person.class, "getFirstName");
    }

    protected final String getPersonLastName()
    {
        return getFieldName(Person.class, "getLastName");
    }

    protected final String getPersonAge()
    {
        return getFieldName(Person.class, "getAge");
    }

    protected final String getPersonCompany()
    {
        return getFieldName(Person.class, "getCompany");
    }

    protected final String getPersonUrl()
    {
        return getFieldName(Person.class, "getURL");
    }

    protected final String getCompanyId()
    {
        return getFieldName(Company.class, "getCompanyID");
    }

    protected final String getCompanyAddressInfoLine1()
    {
        return getFieldName(CompanyAddressInfo.class, "getAddressLine1");
    }

    private void assertSelectSqlEquals(Query query, String expected)
    {
        assertEquals(expected, toSql(getDatabaseProvider(), query, false));
    }

    private void assertCountSqlEquals(Query query, String expected)
    {
        assertEquals(expected, toSql(getDatabaseProvider(), query, true));
    }

    private String toSql(DatabaseProvider provider, Query query, boolean count)
    {
        return query.toSQL(Person.class, provider, entityManager.getTableNameConverter(), entityManager.getFieldNameConverter(), count);
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

    static class DatabaseProviders
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
            return new EmbeddedDerbyDatabaseProvider(newDataSource(""), "")
            {
                @Override
                protected void setPostConnectionProperties(Connection conn) throws SQLException
                {
                    // nothing
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
}
