package net.java.ao.db;

import net.java.ao.DatabaseProvider;
import org.junit.Test;

import java.io.IOException;

import static net.java.ao.DatabaseProviders.getEmbeddedDerbyDatabaseProvider;

public final class DerbyDatabaseProviderTest extends DatabaseProviderTest
{
    @Override
    protected String getDatabase()
    {
        return "derby";
    }

    @Override
    protected DatabaseProvider getDatabaseProvider()
    {
        return getEmbeddedDerbyDatabaseProvider();
    }

    @Test
    public void testRenderActionAlterColumn() throws IOException
    {
        testRenderAction(new String[0], createActionAlterColumn, getDatabaseProvider());
    }

    @Test
    public void testRenderActionAlterStringColumn() throws IOException
    {
        testRenderAction(new String[0], createActionAlterStringColumn, getDatabaseProvider());
    }

    @Test
    public void testRenderActionAlterStringLengthWithinBoundsColumn() throws IOException
    {
        testRenderAction(new String[0], createActionAlterStringLengthColumn, getDatabaseProvider());
    }

    @Test
    public void testRenderActionAlterStringGreaterThanMaxLengthToUnlimitedColumn() throws IOException
    {
        testRenderAction(new String[0], createActionAlterStringMaxLengthColumn, getDatabaseProvider());
    }

    @Test
    public void testRenderActionAlterNumericColumn() throws IOException
    {
        testRenderAction(new String[0], createActionAlterNumericColumn, getDatabaseProvider());
    }

    @Test
    public void testRenderActionDropColumn() throws IOException
    {
        testRenderAction(new String[0], createActionDropColumn, getDatabaseProvider());
    }
}
