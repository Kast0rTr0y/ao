package net.java.ao.it;

import net.java.ao.DBParam;
import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.Preload;
import net.java.ao.Query;
import net.java.ao.schema.StringLength;
import net.java.ao.schema.Table;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@Data(TestClobError.TestClobErrorDatabaseUpdater.class)
public final class TestClobError extends ActiveObjectsIntegrationTest {
    public static final class TestClobErrorDatabaseUpdater implements DatabaseUpdater {
        @Override
        public void update(EntityManager entityManager) throws Exception {
            entityManager.migrate(AoProjectActivity.class);
        }
    }

    @Preload
    @Table("ProjectActivity")
    public interface AoProjectActivity extends Entity {
        @StringLength(StringLength.UNLIMITED)
        String getCommentText();
    }

    @Test
    public void testVarbinaryNtext() throws Exception {
        entityManager.create(AoProjectActivity.class,
                new DBParam("COMMENT_TEXT", null)
        );
        assertThat(entityManager.count(AoProjectActivity.class, Query.select("COMMENT_TEXT").where("COMMENT_TEXT is null")), is(1));
    }
}
