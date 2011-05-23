package net.java.ao.types;

import net.java.ao.it.DatabaseProcessor;
import net.java.ao.it.model.Person;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
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
public class DatabaseTypeIntegrationTest extends ActiveObjectsIntegrationTest
{
    @Test
    public void testPutToDatabase() throws Exception
    {
        final String personTableName = getTableName(Person.class);
        final String firstName = escapeFieldName(Person.class, "getFirstName");
        final String age = escapeFieldName(Person.class, "getAge");
        final String url = escapeFieldName(Person.class, "getURL");
        final String favoriteClass = escapeFieldName(Person.class, "getFavoriteClass");
        final String id = escapeFieldName(Person.class, "getID");

        executeUpdate("UPDATE " + personTableName + " SET "
                + firstName + " = ?, "
                + age + " = ?, "
                + url + " = ?, "
                + favoriteClass + " = ? "
                + "WHERE " + id + " = ?", new UpdateCallback()
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
        final String firstName = escapeFieldName(Person.class, "getFirstName");
        final String age = escapeFieldName(Person.class, "getAge");
        final String url = escapeFieldName(Person.class, "getURL");
        final String favoriteClass = escapeFieldName(Person.class, "getFavoriteClass");
        final String id = escapeFieldName(Person.class, "getID");

        executeUpdate("UPDATE " + personTableName + " SET "
                + firstName + " = ?, "
                + age + " = ?, "
                + url + " = ?, "
                + favoriteClass + " = ? "
                + "WHERE " + id + " = ?",
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

        executeStatement("SELECT " + firstName + ","
                + age + ","
                + url + ","
                + favoriteClass
                + " FROM " + personTableName + " WHERE " + id + " = ?",
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
                            assertEquals("JoeJoe", new VarcharType().pullFromDatabase(entityManager, res, String.class, getFieldName(Person.class, "getFirstName")));
                            assertEquals(123, new IntegerType().pullFromDatabase(entityManager, res, int.class, getFieldName(Person.class, "getAge")).intValue());
                            assertEquals(new URL("http://www.google.com"), new URLType().pullFromDatabase(entityManager, res, URL.class, getFieldName(Person.class, "getURL")));
                            assertEquals(DatabaseTypeIntegrationTest.class, new ClassType().pullFromDatabase(entityManager, res, (Class) Class.class, getFieldName(Person.class, "getFavoriteClass")));
                        }
                    }
                });
    }
}
