package net.java.ao.it;

import com.google.common.base.Supplier;
import com.google.common.base.Suppliers;
import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.schema.StringLength;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import org.junit.Test;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

@Data(TestClobDataType.TestBigClobDatabaseUpdater.class)
public final class TestClobDataType extends ActiveObjectsIntegrationTest {
    @Test
    public void setSmallClobAndRetrieve() throws Exception {
        setSomeClobAndRetrieve("small", SMALL_CLOB);
    }

    @Test
    public void setBigClobAndRetrieve() throws Exception {
        setSomeClobAndRetrieve("big", BIG_CLOB.get());
    }

    private void setSomeClobAndRetrieve(String name, String text) throws SQLException {
        final TestEntity o = entityManager.create(TestEntity.class);
        o.setName(name);
        o.setText(text);
        o.save();

        entityManager.flushAll();

        final TestEntity retrieved = entityManager.get(TestEntity.class, o.getID());
        assertEquals(text, retrieved.getText());
    }

    static interface TestEntity extends Entity {
        String getName();

        void setName(String name);

        @StringLength(StringLength.UNLIMITED)
        String getText();

        @StringLength(StringLength.UNLIMITED)
        void setText(String text);
    }

    public static final class TestBigClobDatabaseUpdater implements DatabaseUpdater {
        @Override
        public void update(EntityManager entityManager) throws Exception {
            entityManager.migrate(TestEntity.class);
        }
    }

    private static final String SMALL_CLOB = "Some small sample";

    // over 4000 bytes, as Oracle has issues with that.
    private static final Supplier<String> BIG_CLOB = Suppliers.memoize(new Supplier<String>() {
        @Override
        public String get() {
            final int size = 8100;
            final StringBuilder sb = new StringBuilder(size);
            for (int i = 0; i < size / 10; i++) {
                sb.append("0123456789#");
            }
            return sb.append(size).toString();
        }
    });
}
