package net.java.ao.types;

import net.java.ao.it.DatabaseProcessor;
import net.java.ao.it.model.Person;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.DbUtils;
import net.java.ao.test.jdbc.Data;
import org.junit.Test;

import static java.sql.Types.INTEGER;

import static java.sql.Types.VARCHAR;

import java.net.URL;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static net.java.ao.it.DatabaseProcessor.*;
import static org.junit.Assert.*;

/**
 *
 */
@Data(DatabaseProcessor.class)
public class DatabaseTypeIntegrationTest extends ActiveObjectsIntegrationTest
{
    @Test
    public void testPutToDatabase() throws Exception
    {
        final String personTableName = getTableName(Person.class);
        final String firstName = escapeFieldName(Person.class, "getFirstName");
        final String age = escapeFieldName(Person.class, "getAge");
        final String url = escapeFieldName(Person.class, "getURL");
        final String id = escapeFieldName(Person.class, "getID");

        executeUpdate("UPDATE " + personTableName + " SET "
                + firstName + " = ?, "
                + age + " = ?, "
                + url + " = ? "
                + "WHERE " + id + " = ?", new DbUtils.UpdateCallback()
        {
            public void setParameters(PreparedStatement stmt) throws Exception
            {
                int index = 1;

                new StringType().putToDatabase(entityManager, stmt, index++, "JoeJoe", VARCHAR);
                new IntegerType().putToDatabase(entityManager, stmt, index++, 123, INTEGER);
                new URLType().putToDatabase(entityManager, stmt, index++, new URL("http://www.google.com"), VARCHAR);
                stmt.setInt(index++, PersonData.getId());
            }
        });

        executeStatement("SELECT " + firstName + ","
                + age + ","
                + url
                + " FROM " + personTableName + " WHERE " + id + " = ?",
                new DbUtils.StatementCallback()
                {
                    public void setParameters(PreparedStatement stmt) throws Exception
                    {
                        stmt.setInt(1, PersonData.getId());
                    }

                    public void processResult(ResultSet res) throws Exception
                    {
                        if (res.next())
                        {
                            assertEquals("JoeJoe", res.getString(getFieldName(Person.class, "getFirstName")));
                            assertEquals(123, res.getInt(getFieldName(Person.class, "getAge")));
                            assertEquals("http://www.google.com", res.getString(getFieldName(Person.class, "getURL")));
                        }
                    }
                });
    }

    @SuppressWarnings("unchecked")
    @Test
    public void testConvert() throws Exception
    {
        final String personTableName = getTableName(Person.class);
        final String firstName = escapeFieldName(Person.class, "getFirstName");
        final String age = escapeFieldName(Person.class, "getAge");
        final String url = escapeFieldName(Person.class, "getURL");
        final String id = escapeFieldName(Person.class, "getID");

        executeUpdate("UPDATE " + personTableName + " SET "
                + firstName + " = ?, "
                + age + " = ?, "
                + url + " = ? "
                + "WHERE " + id + " = ?",
                new DbUtils.UpdateCallback()
                {
                    public void setParameters(PreparedStatement stmt) throws Exception
                    {
                        int index = 1;

                        stmt.setString(index++, "JoeJoe");
                        stmt.setInt(index++, 123);
                        stmt.setString(index++, "http://www.google.com");
                        stmt.setInt(index++, PersonData.getId());
                    }

                });

        executeStatement("SELECT " + firstName + ","
                + age + ","
                + url
                + " FROM " + personTableName + " WHERE " + id + " = ?",
                new DbUtils.StatementCallback()
                {
                    public void setParameters(PreparedStatement stmt) throws Exception
                    {
                        stmt.setInt(1, PersonData.getId());
                    }

                    public void processResult(ResultSet res) throws Exception
                    {
                        if (res.next())
                        {
                            assertEquals("JoeJoe", new StringType().pullFromDatabase(entityManager, res, String.class, getFieldName(Person.class, "getFirstName")));
                            assertEquals(123, new IntegerType().pullFromDatabase(entityManager, res, int.class, getFieldName(Person.class, "getAge")).intValue());
                            assertEquals(new URL("http://www.google.com"), new URLType().pullFromDatabase(entityManager, res, URL.class, getFieldName(Person.class, "getURL")));
                        }
                    }
                });
    }
}
