package net.java.ao.types;

import net.java.ao.it.DatabaseProcessor;
import net.java.ao.it.config.DynamicJdbcConfiguration;
import net.java.ao.it.model.Person;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.Jdbc;
import org.junit.Test;

import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static net.java.ao.it.DatabaseProcessor.*;
import static org.junit.Assert.*;

/**
 *
 */
@Data(DatabaseProcessor.class)
@Jdbc(DynamicJdbcConfiguration.class)
public class DatabaseTypeIntegrationTest extends ActiveObjectsIntegrationTest
{
    @Test
    public void testPutToDatabase() throws Exception
    {
        final String personTableName = getTableName(Person.class);
        final String firstName = "firstName";
        final String age = "age";
        final String url = "url";
        final String favoriteClass = "favoriteClass";
        final String id = "id";

        executeUpdate("UPDATE " + personTableName + " SET "
                + escapeKeyword(firstName) + " = ?, "
                + escapeKeyword(age) + " = ?, "
                + escapeKeyword(url) + " = ?, "
                + escapeKeyword(favoriteClass) + " = ? "
                + "WHERE " + escapeKeyword(id) + " = ?", new UpdateCallback()
        {
            public void setParameters(PreparedStatement stmt) throws Exception
            {
                int index = 1;

                new VarcharType().putToDatabase(entityManager, stmt, index++, "JoeJoe");
                new IntegerType().putToDatabase(entityManager, stmt, index++, 123);
                new URLType().putToDatabase(entityManager, stmt, index++, new URL("http://www.google.com"));
                new ClassType().putToDatabase(entityManager, stmt, index++, DatabaseTypeIntegrationTest.class);

                stmt.setInt(index++, PersonData.getId());
            }
        });

        executeStatement("SELECT " + escapeKeyword(firstName) + ","
                + escapeKeyword(age) + ","
                + escapeKeyword(url) + ","
                + escapeKeyword(favoriteClass)
                + " FROM " + personTableName + " WHERE " + escapeKeyword(id) + " = ?",
                new StatementCallback()
                {
                    public void setParameters(PreparedStatement stmt) throws Exception
                    {
                        stmt.setInt(1, PersonData.getId());
                    }

                    public void processResult(ResultSet res) throws Exception
                    {
                        if (res.next())
                        {
                            assertEquals("JoeJoe", res.getString(firstName));
                            assertEquals(123, res.getInt(age));
                            assertEquals("http://www.google.com", res.getString(url));
                            assertEquals(DatabaseTypeIntegrationTest.class.getName(), res.getString(favoriteClass));
                        }
                    }
                });
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testConvert() throws Exception
    {
        final String personTableName = getTableName(Person.class);
        final String firstName = "firstName";
        final String age = "age";
        final String url = "url";
        final String favoriteClass = "favoriteClass";
        final String id = "id";

        executeUpdate("UPDATE " + personTableName + " SET "
                + escapeKeyword(firstName) + " = ?, "
                + escapeKeyword(age) + " = ?, "
                + escapeKeyword(url) + " = ?, "
                + escapeKeyword(favoriteClass) + " = ? "
                + "WHERE " + escapeKeyword(id) + " = ?",
                new UpdateCallback()
                {
                    public void setParameters(PreparedStatement stmt) throws Exception
                    {
                        int index = 1;

                        stmt.setString(index++, "JoeJoe");
                        stmt.setInt(index++, 123);
                        stmt.setString(index++, "http://www.google.com");
                        stmt.setString(index++, DatabaseTypeIntegrationTest.class.getName());

                        stmt.setInt(index++, PersonData.getId());
                    }

                });

        executeStatement("SELECT " + escapeKeyword(firstName) + ","
                + escapeKeyword(age) + ","
                + escapeKeyword(url) + ","
                + escapeKeyword(favoriteClass)
                + " FROM " + personTableName + " WHERE " + escapeKeyword(id) + " = ?",
                new StatementCallback()
                {
                    public void setParameters(PreparedStatement stmt) throws Exception
                    {
                        stmt.setInt(1, PersonData.getId());
                    }

                    public void processResult(ResultSet res) throws Exception
                    {
                        if (res.next())
                        {
                            assertEquals("JoeJoe", new VarcharType().pullFromDatabase(entityManager, res, String.class, "firstName"));
                            assertEquals(123, new IntegerType().pullFromDatabase(entityManager, res, int.class, "age").intValue());
                            assertEquals(new URL("http://www.google.com"), new URLType().pullFromDatabase(entityManager, res, URL.class, "url"));
                            assertEquals(DatabaseTypeIntegrationTest.class, new ClassType().pullFromDatabase(entityManager, res, (Class) Class.class, "favoriteClass"));
                        }
                    }
                });
    }
}
