package net.java.ao.it.query;

import net.java.ao.Entity;
import net.java.ao.Preload;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.DbUtils;
import net.java.ao.test.jdbc.NonTransactional;
import org.junit.Test;

import java.sql.PreparedStatement;

import static org.junit.Assert.assertEquals;

public final class PreloadTest extends ActiveObjectsIntegrationTest {
    /**
     * Test get with listed preload values
     */
    @Test
    @NonTransactional
    public void testGetPreloadListed() throws Exception {
        entityManager.migrate(PreloadListed.class);

        int id = insertEntry(PreloadListed.class, 1000l, "Toto", 50);

        // load object
        PreloadListed ao = entityManager.get(PreloadListed.class, id);

        // change db
        updateEntry(PreloadListed.class, id, 100l, "Tata", 5);

        // ensure we stored the old values
        assertEquals(1000l, (long) ao.getAge());
        assertEquals("Toto", ao.getName());
        assertEquals(50, (int) ao.getHello());
    }

    /**
     * Test get with listed preload values
     */
    @Test
    @NonTransactional
    public void testGetPreloadAll() throws Exception {
        entityManager.migrate(PreloadAll.class);

        int id = insertEntry(PreloadAll.class, 1000l, "Toto", 50);

        // load object
        PreloadAll ao = entityManager.get(PreloadAll.class, id);

        // change db
        updateEntry(PreloadAll.class, id, 100l, "Tata", 5);

        // ensure we stored the old values
        assertEquals(1000l, (long) ao.getAge());
        assertEquals("Toto", ao.getName());
        assertEquals(50, (int) ao.getHello());
    }

    /**
     * Test find with listed preload
     */
    @Test
    @NonTransactional
    public void testFindPreloadListed() throws Exception {
        entityManager.migrate(PreloadListed.class);

        int id = insertEntry(PreloadListed.class, 1000l, "Toto", 50);

        // load object
        PreloadListed ao = entityManager.find(PreloadListed.class, "ID = ?", id)[0];

        // change db
        updateEntry(PreloadListed.class, id, 100l, "Tata", 5);

        // ensure we stored the old values
        assertEquals(1000l, (long) ao.getAge());
        assertEquals("Toto", ao.getName());
        assertEquals(50, (int) ao.getHello());
    }

    /**
     * Test find with star preload
     */
    @Test
    @NonTransactional
    public void testFindPreloadAll() throws Exception {
        entityManager.migrate(PreloadAll.class);

        int id = insertEntry(PreloadAll.class, 1000l, "Toto", 50);

        // load object
        PreloadAll ao = entityManager.find(PreloadAll.class, "ID = ?", id)[0];

        // change db
        updateEntry(PreloadAll.class, id, 100l, "Tata", 5);

        // ensure we stored the old values
        assertEquals(1000l, (long) ao.getAge());
        assertEquals("Toto", ao.getName());
        assertEquals(50, (int) ao.getHello());
    }

    /**
     * Insert an entry into the db
     */
    private Integer insertEntry(Class aoClass, final Long age, final String name, final Integer hello) throws Exception {
        // get count, which will be the last existing id
        int id = entityManager.count(aoClass);

        final String insert = "INSERT INTO " +
                getTableName(aoClass) + " (" +
                escapeFieldName(aoClass, "getAge") + ", " +
                escapeFieldName(aoClass, "getName") + ", " +
                escapeFieldName(aoClass, "getHello") + ") VALUES (?,?,?)";

        // insert one entry
        DbUtils.executeUpdate(entityManager, insert,
                new DbUtils.UpdateCallback() {
                    public void setParameters(PreparedStatement statement) throws Exception {
                        statement.setLong(1, age);
                        statement.setString(2, name);
                        statement.setInt(3, hello);
                    }
                }
        );
        return id + 1;
    }

    /**
     * Update the entry with new values
     */
    private void updateEntry(Class aoClass, final Integer id, final Long age, final String name, final Integer hello) throws Exception {
        final String insert = "UPDATE " +
                getTableName(aoClass) + " SET " +
                escapeFieldName(aoClass, "getAge") + " = ?, " +
                escapeFieldName(aoClass, "getName") + " = ?, " +
                escapeFieldName(aoClass, "getHello") + " = ? " +
                "WHERE " + escapeFieldName(aoClass, "getID") + " = ?";

        // insert one entry
        DbUtils.executeUpdate(entityManager, insert,
                new DbUtils.UpdateCallback() {
                    public void setParameters(PreparedStatement statement) throws Exception {
                        statement.setLong(1, age);
                        statement.setString(2, name);
                        statement.setInt(3, hello);
                        statement.setInt(4, id);
                    }
                }
        );
    }

    /**
     * Object with preload values
     */
    @Preload({"age", "name", "hello"})
    public static interface PreloadListed extends Entity {
        public Long getAge();

        public void setAge(Long age);

        public String getName();

        public void setName(String name);

        public Integer getHello();

        public void setHello(Integer i);
    }

    /**
     * Object with star preload value
     */
    @Preload("*")
    public static interface PreloadAll extends Entity {
        public Long getAge();

        public void setAge(Long age);

        public String getName();

        public void setName(String name);

        public Integer getHello();

        public void setHello(Integer i);
    }
}
