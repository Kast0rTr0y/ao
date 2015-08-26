package net.java.ao.it;

import net.java.ao.Transaction;
import net.java.ao.it.model.Author;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.NonTransactional;
import net.java.ao.test.junit.ActiveObjectsJUnitRunner;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.sql.SQLException;

import static org.junit.Assert.assertEquals;

@RunWith(ActiveObjectsJUnitRunner.class)
public class TransactionTest extends ActiveObjectsIntegrationTest {

    @Test
    @NonTransactional
    public void testRollbackUnsaved() throws Exception {
        testRollback(false);
    }

    @Test
    @NonTransactional
    public void testRollbackSaved() throws Exception {
        testRollback(true);
    }

    private void testRollback(final boolean save) throws SQLException {
        entityManager.migrate(Author.class);
        final int id = new Transaction<Integer>(entityManager) {

            @Override
            protected Integer run() throws SQLException {
                final Author author = entityManager.create(Author.class);
                author.setName("George Orwell");
                author.save();
                return author.getID();
            }

        }.execute();
        try {
            new Transaction<Void>(entityManager) {

                @Override
                protected Void run() throws SQLException {
                    final Author author = entityManager.get(Author.class, id);
                    author.setName("Emily Bronte");
                    if (save) {
                        author.save();
                    }
                    throw new RuntimeException();
                }

            }.execute();
        } catch (final RuntimeException exception) {
        }
        final String name = new Transaction<String>(entityManager) {

            @Override
            protected String run() throws SQLException {
                final Author author = entityManager.get(Author.class, id);
                return author.getName();
            }

        }.execute();
        assertEquals("George Orwell", name);
    }

}