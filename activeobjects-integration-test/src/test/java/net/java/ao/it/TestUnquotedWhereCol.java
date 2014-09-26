package net.java.ao.it;

import net.java.ao.Entity;
import net.java.ao.EntityManager;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.test.jdbc.DatabaseUpdater;
import org.junit.Test;

import java.sql.SQLException;

/**
 * Attempts to reproduce https://ecosystem.atlassian.net/browse/AO-560
 */
@Data (TestUnquotedWhereCol.TestUnquotedWhereColDatabaseUpdater.class)
public class TestUnquotedWhereCol extends ActiveObjectsIntegrationTest
{
    @Test
    public void deleteWithSQL() throws SQLException
    {
        entityManager.deleteWithSQL(HistoryEvent.class, "KARMA_USER_ID = ?", 1);
    }

    static interface HistoryEvent extends Entity
    {
        Integer getKarmaUserId();
        void setKarmaUserId(Integer karmaUserId);
    }

    public static final class TestUnquotedWhereColDatabaseUpdater implements DatabaseUpdater
    {
        @Override
        public void update(final EntityManager entityManager) throws Exception
        {
            //noinspection unchecked
            entityManager.migrate(HistoryEvent.class);
        }
    }
}
