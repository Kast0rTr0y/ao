package net.java.ao.schema;

import net.java.ao.Entity;
import net.java.ao.RawEntity;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.DbUtils;
import org.junit.Test;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public final class ColumnNamesTest extends ActiveObjectsIntegrationTest {
    /**
     * Test a long table and column name
     */
    @Test
    public void testCreateLongName() throws Exception {
        entityManager.migrate(ThisIsAVeryLongTableNameLikeInTheOldTimes.class);

        ThisIsAVeryLongTableNameLikeInTheOldTimes e = entityManager.create(ThisIsAVeryLongTableNameLikeInTheOldTimes.class);

        e.setThisIsAVeryLongColumnNameLikeInTheOldTimes("Test");
        e.save();

        entityManager.flushAll();
        assertEquals("Test", e.getThisIsAVeryLongColumnNameLikeInTheOldTimes());
        checkFieldValue(ThisIsAVeryLongTableNameLikeInTheOldTimes.class, "getID", e.getID(), "getThisIsAVeryLongColumnNameLikeInTheOldTimes", "Test");
    }

    /**
     * Test a column called "index"
     */
    @Test
    public void testIndexColumn() throws Exception {
        entityManager.migrate(IndexColumn.class);

        IndexColumn e = entityManager.create(IndexColumn.class);
        e.setIndex("Test");
        e.save();

        entityManager.flushAll();
        assertEquals("Test", e.getIndex());
        checkFieldValue(IndexColumn.class, "getID", e.getID(), "getIndex", "Test");
    }

    /**
     * Test a column called min
     */
    @Test
    public void testMinColumn() throws Exception {
        entityManager.migrate(MinColumn.class);

        MinColumn e = entityManager.create(MinColumn.class);
        e.setMin("Test");
        e.save();

        entityManager.flushAll();
        assertEquals("Test", e.getMin());
        checkFieldValue(MinColumn.class, "getID", e.getID(), "getMin", "Test");
    }

    /**
     * Test a column called "max
     */
    @Test
    public void testMaxColumn() throws Exception {
        entityManager.migrate(MaxColumn.class);

        MaxColumn e = entityManager.create(MaxColumn.class);
        e.setMax("Test");
        e.save();

        entityManager.flushAll();
        assertEquals("Test", e.getMax());
        checkFieldValue(MaxColumn.class, "getID", e.getID(), "getMax", "Test");
    }

    /**
     * Test a column called position
     */
    @Test
    public void testPositionColumn() throws Exception {
        entityManager.migrate(PositionColumn.class);

        PositionColumn e = entityManager.create(PositionColumn.class);
        e.setPosition("Test");
        e.save();

        entityManager.flushAll();
        assertEquals("Test", e.getPosition());
        checkFieldValue(PositionColumn.class, "getID", e.getID(), "getPosition", "Test");
    }

    /**
     * Test various column names
     */
    @Test
    public void testVarious() throws Exception {
        entityManager.migrate(
                TableColumn.class,
                IsColumn.class,
                DropColumn.class,
                WhereColumn.class,
                SelectColumn.class,
                LabelColumn.class,
                ColumnColumn.class);
    }

    private <T extends RawEntity<?>> void checkFieldValue(final Class<T> entityType, String idGetterName, final Integer id, final String getterName, final String fieldValue) throws Exception {
        executeStatement("SELECT " + escapeFieldName(entityType, getterName) + " FROM " + getTableName(entityType) + " WHERE " + escapeFieldName(entityType, idGetterName) + " = ?",
                new DbUtils.StatementCallback() {
                    public void setParameters(PreparedStatement statement) throws Exception {
                        statement.setObject(1, id);
                    }

                    public void processResult(ResultSet resultSet) throws Exception {
                        if (resultSet.next()) {
                            assertEquals(fieldValue, resultSet.getString(getFieldName(entityType, getterName)));
                        } else {
                            fail("No entry found in database with ID " + id);
                        }
                    }
                }
        );
    }

    public static interface ThisIsAVeryLongTableNameLikeInTheOldTimes extends Entity {
        public String getThisIsAVeryLongColumnNameLikeInTheOldTimes();

        public void setThisIsAVeryLongColumnNameLikeInTheOldTimes(String thisIsAVeryLongNameLikeInTheOldTimes);
    }

    public static interface IndexColumn extends Entity {
        public String getIndex();

        public void setIndex(String index);
    }

    public static interface MinColumn extends Entity {
        public String getMin();

        public void setMin(String min);
    }

    public static interface MaxColumn extends Entity {
        public String getMax();

        public void setMax(String max);
    }

    public static interface TableColumn extends Entity {
        public String getTable();

        public void setTable(String table);
    }

    public static interface DropColumn extends Entity {
        public String getDrop();

        public void setDrop(String drop);
    }

    public static interface IsColumn extends Entity {
        public String getIs();

        public void setIs(String is);
    }

    public static interface WhereColumn extends Entity {
        public String getWhere();

        public void setWhere(String where);
    }

    public static interface PositionColumn extends Entity {
        public String getPosition();

        public void setPosition(String position);
    }

    public static interface SelectColumn extends Entity {
        public String getSelect();

        public void setSelect(String select);
    }

    public static interface LabelColumn extends Entity {
        public String getLabel();

        public void setLabel(String label);
    }

    public static interface ColumnColumn extends Entity {
        public String getColumn();

        public void setColumn(String column);
    }
}
