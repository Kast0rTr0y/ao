package net.java.ao.it.datatypes;

import net.java.ao.ActiveObjectsConfigurationException;
import net.java.ao.DBParam;
import net.java.ao.Entity;
import net.java.ao.schema.Default;
import net.java.ao.schema.NotNull;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.NonTransactional;
import org.apache.commons.io.IOUtils;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;

import static org.junit.Assert.*;

/**
 * Tests for Blob data type
 */
public final class BlobTypeTest extends ActiveObjectsIntegrationTest
{
    private static byte[] SMALL_BLOB = "Some small sample".getBytes();

    // over 4000 bytes, as Oracle has issues with that.
    private static byte[] LARGE_BLOB;

    static
    {
        String s = "123456789#"; // 10 chars
        StringBuilder sb = new StringBuilder(s.length() * 600);
        for (int i = 0; i < 600; i++)
        {
            sb.append(s);
        }
        sb.append(sb.length() + 4);
        LARGE_BLOB = sb.toString().getBytes();
    }

    /**
     * Test ByteArray representation of a blob
     */
    @Test
    @NonTransactional
    public void testByteArrayStore() throws Exception
    {
        entityManager.migrate(ByteArrayBlobColumn.class);

        // create
        ByteArrayBlobColumn e = entityManager.create(ByteArrayBlobColumn.class);
        assertNull(e.getData());

        // set data
        e.setData(SMALL_BLOB);
        e.save();

        entityManager.flushAll();
        assertByteArraysEquals(SMALL_BLOB, e.getData());
        // TODO: check how this looks in the db!

        // clear data
        e.setData(null);
        e.save();

        entityManager.flushAll();
        assertNull(e.getData());
        // TODO: check how this looks in the db!
    }

    /**
     * Test null value
     */
    @Test
    @NonTransactional
    public void testByteArrayNullColumnWithCreate() throws Exception
    {
        entityManager.migrate(ByteArrayBlobColumn.class);

        // create
        ByteArrayBlobColumn e = entityManager.create(ByteArrayBlobColumn.class, new DBParam(getFieldName(ByteArrayBlobColumn.class, "getData"), null));

        entityManager.flushAll();
        assertNull(e.getData());
        // TODO: check how this looks in the db!
    }

    /**
     * Test null value
     */
    @Test
    @NonTransactional
    public void testByteArrayNullColumnWithSet() throws Exception
    {
        entityManager.migrate(ByteArrayBlobColumn.class);

        // create
        ByteArrayBlobColumn e = entityManager.create(ByteArrayBlobColumn.class, new DBParam(getFieldName(ByteArrayBlobColumn.class, "getData"), LARGE_BLOB));
        e.setData(null);
        e.save();

        entityManager.flushAll();
        assertNull(e.getData());
        // TODO: check how this looks in the db!
    }

    @Test
    @NonTransactional
    public void testNullValueWithPullFromDatabase() throws Exception
    {
        entityManager.migrate(ByteArrayBlobColumn.class);

        // create
        ByteArrayBlobColumn newEntity = entityManager.create(ByteArrayBlobColumn.class, new DBParam(getFieldName(ByteArrayBlobColumn.class, 
                "setData"), LARGE_BLOB));
        newEntity.setData(null);
        newEntity.save();

        //Use PullFromDatabase of EnumType
        ByteArrayBlobColumn loadedEntity = entityManager.get(ByteArrayBlobColumn.class, newEntity.getID());
        assertNull(loadedEntity.getData());
    }

    /**
     * Test null value
     */
    @Test
    // Transactional, as Oracle requires an active DB connection while reading an Entity's InputStream fields
    public void testInputStreamNullColumnWithCreate() throws Exception
    {
        entityManager.migrateDestructively(InputStreamBlobColumn.class);

        // create
        InputStreamBlobColumn e = entityManager.create(InputStreamBlobColumn.class, new DBParam(getFieldName(InputStreamBlobColumn.class, "getData"), null));

        entityManager.flushAll();
        assertNull(e.getData());
        // TODO: check how this looks in the db!
    }

    /**
     * Test null value
     */
    @Test
    // Transactional, as Oracle requires an active DB connection while reading an Entity's InputStream fields
    public void testInputStreamNullColumnWithSet() throws Exception
    {
        entityManager.migrateDestructively(InputStreamBlobColumn.class);

        // create
        InputStreamBlobColumn e = entityManager.create(InputStreamBlobColumn.class, new DBParam(getFieldName(InputStreamBlobColumn.class, "getData"), LARGE_BLOB));
        e.setData(null);
        e.save();

        entityManager.flushAll();
        assertNull(e.getData());
        // TODO: check how this looks in the db!
    }


    /**
     * Test InputStream representation of a blob
     */
    @Test
    // Transactional, as Oracle requires an active DB connection while reading an Entity's InputStream fields
    public void testInputStreamStore() throws Exception
    {
        entityManager.migrateDestructively(InputStreamBlobColumn.class);

        // create
        InputStreamBlobColumn e = entityManager.create(InputStreamBlobColumn.class);
        assertNull(e.getData());

        // set data
        e.setData(new ByteArrayInputStream(SMALL_BLOB));
        e.save();

        entityManager.flushAll();
        byte[] data = IOUtils.toByteArray(e.getData());
        assertByteArraysEquals(SMALL_BLOB, data);
        // TODO: check how this looks in the db!

        // clear data
        e.setData(null);
        e.save();

        entityManager.flushAll();
        assertNull(e.getData());
        // TODO: check how this looks in the db!
    }

    /**
     * Test ByteArray representation of a blob
     */
    @Test
    @NonTransactional
    public void testNotNullByteArrayStore() throws Exception
    {
        entityManager.migrate(ByteArrayBlobColumn.class);

        // create
        ByteArrayBlobColumn e = entityManager.create(ByteArrayBlobColumn.class, new DBParam(getFieldName(ByteArrayBlobColumn.class, "getData"), LARGE_BLOB));

        entityManager.flushAll();
        assertByteArraysEquals(LARGE_BLOB, e.getData());
        // TODO: check how this looks in the db!
    }

    /**
     * Test InputStream representation of a blob
     */
    @Test
    // Transactional, as Oracle requires an active DB connection while reading an Entity's InputStream fields
    public void testNotNullInputStreamStore() throws Exception
    {
        entityManager.migrateDestructively(InputStreamBlobColumn.class);

        // create
        InputStreamBlobColumn e = entityManager.create(InputStreamBlobColumn.class, new DBParam(getFieldName(InputStreamBlobColumn.class, "getData"), new ByteArrayInputStream(LARGE_BLOB)));

        entityManager.flushAll();
        assertByteArraysEquals(LARGE_BLOB, IOUtils.toByteArray(e.getData()));
        // TODO: check how this looks in the db!
    }

    /**
     * Test NotNull blob column
     */
    @Test(expected = IllegalArgumentException.class)
    @NonTransactional
    public void testNotNullColumnCreatingWithoutValue() throws Exception
    {
        entityManager.migrate(NotNullByteArrayBlobColumn.class);

        // create
        entityManager.create(NotNullByteArrayBlobColumn.class);
    }

    /**
     * Test NotNull blob column
     */
    @Test(expected = IllegalArgumentException.class)
    @NonTransactional
    public void testNotNullColumnSetNull() throws Exception
    {
        entityManager.migrate(NotNullByteArrayBlobColumn.class);

        // create
        NotNullByteArrayBlobColumn e = entityManager.create(NotNullByteArrayBlobColumn.class, new DBParam(getFieldName(NotNullByteArrayBlobColumn.class, "getData"), SMALL_BLOB));

        // set value to null should fail
        e.setData(null);
    }

    /**
     * Default value not supported
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testDefaultColumn() throws Exception
    {
        entityManager.migrate(DefaultColumn.class);
    }

    /**
     * Empty String default value not supported
     */
    @Test(expected = ActiveObjectsConfigurationException.class)
    @NonTransactional
    public void testEmptyDefaultColumn() throws Exception
    {
        entityManager.migrate(EmptyDefaultColumn.class);
    }

    /**
     * Test deletion
     */
    @Test
    @NonTransactional
    public void testDeletion() throws Exception
    {
        entityManager.migrate(ByteArrayBlobColumn.class);

        // create
        ByteArrayBlobColumn e = entityManager.create(ByteArrayBlobColumn.class);
        assertNull(e.getData());

        // set data
        e.setData(LARGE_BLOB);
        e.save();

        entityManager.delete(e);

        // TODO: check that blob got deleted
    }

    private void assertByteArraysEquals(byte[] a, byte[] b)
    {
        assertEquals(new String(a), new String(b));
    }

    /**
     * Accessing the blob by getting the string
     */
    public static interface ByteArrayBlobColumn extends Entity
    {
        public byte[] getData();

        public void setData(byte[] data);
    }

    /**
     * Accessing the blob by an input stream
     */
    public static interface InputStreamBlobColumn extends Entity
    {
        public InputStream getData();

        public void setData(InputStream data);
    }

    /**
     * Blob with not null constraint
     */
    public static interface NotNullByteArrayBlobColumn extends Entity
    {
        @NotNull
        public byte[] getData();

        public void setData(byte[] data);
    }

    /**
     * Accessing the blob by an input stream
     */
    public static interface NotNullInputStreamBlobColumn extends Entity
    {
        @NotNull
        public InputStream getData();

        public void setData(InputStream data);
    }

    /**
     * Default value - not supported
     */
    public static interface DefaultColumn extends Entity
    {
        @Default("This is a blob!")
        public InputStream getData();

        public void setData(InputStream data);
    }

    /**
     * Empty default column - not supported
     */
    public static interface EmptyDefaultColumn extends Entity
    {
        @Default("")
        public InputStream getData();

        public void setData(InputStream data);
    }
}
