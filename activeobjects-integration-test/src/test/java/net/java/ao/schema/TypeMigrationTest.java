package net.java.ao.schema;

import net.java.ao.Entity;
import net.java.ao.it.model.Address;
import net.java.ao.it.model.Profession;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.DbUtils;
import org.junit.Test;

import java.util.concurrent.Callable;

/**
 * Ensure that SQL is run to migrate column types when the DB physical type has changed,
 * but not when the only difference is the logical type.
 *
 * See https://ecosystem.atlassian.net/browse/AO-418 for details.
 */
public class TypeMigrationTest extends ActiveObjectsIntegrationTest
{
    @Test
    public void testMigrationWithUnchangedLogicalOrPhysicalTypes() throws Exception
    {
        if (!DbUtils.isOracle(entityManager))
        {
            entityManager.migrate(TestTable.class);
            checkSqlNotExecuted(new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    entityManager.migrate(TestTable.class);
                    return null;
                }
            });
        }
    }

    @Test
    public void testMigrationWithUnchangedPhysicalTypes() throws Exception
    {
        if (!DbUtils.isOracle(entityManager))
        {
            entityManager.migrate(TestTable.class);
            checkSqlNotExecuted(new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    entityManager.migrate(EquivalentTable.class);
                    return null;
                }
            });
            checkSqlNotExecuted(new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    entityManager.migrate(TestTable.class);
                    return null;
                }
            });
        }
    }

    @Test
    public void testMigrationThatChangesPhysicalTypeForEntityColumn() throws Exception
    {
        if (!DbUtils.isOracle(entityManager))
        {
            entityManager.migrate(TestTable.class);
            checkSqlExecuted(new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    entityManager.migrate(DifferentEntityColumnType.class);
                    return null;
                }
            });
        }
    }

    @Test
    public void testMigrationThatChangesPhysicalTypeForEnumColumn() throws Exception
    {
        if (!DbUtils.isOracle(entityManager))
        {
            entityManager.migrate(TestTable.class);
            checkSqlExecuted(new Callable<Object>()
            {
                @Override
                public Object call() throws Exception
                {
                    entityManager.migrate(DifferentEnumColumnType.class);
                    return null;
                }
            });
        }
    }

    @Table(value = "TYPE_MIGRATION_TEST_TABLE")
    private interface TestTable extends Entity
    {
        public Address getEntityVal();
        public void setEntityVal(Address entityVal);

        public Profession getEnumVal();
        public void setEnumVal(Profession enumVal);
    }

    @Table(value = "TYPE_MIGRATION_TEST_TABLE")
    private interface EquivalentTable extends Entity
    {
        @Indexed
        public Integer getEntityValId();
        public void setEntityValId(Integer entityVal);

        public String getEntityValType();
        public void setEntityValType(String entityValType);

        public String getEnumVal();
        public void setEnumVal(String enumVal);
    }

    @Table(value = "TYPE_MIGRATION_TEST_TABLE")
    private interface DifferentEntityColumnType extends Entity
    {
        @Indexed
        public Long getEntityValId();
        public void setEntityValId(Long entityVal);

        public String getEntityValType();
        public void setEntityValType(String entityValType);

        public Profession getEnumVal();
        public void setEnumVal(Profession enumVal);
    }

    @Table(value = "TYPE_MIGRATION_TEST_TABLE")
    private interface DifferentEnumColumnType extends Entity
    {
        public Address getEntityVal();
        public void setEntityVal(Address entityVal);

        @StringLength(value = StringLength.UNLIMITED)
        public String getEnumVal();
        public void setEnumVal(String enumVal);
    }
}
