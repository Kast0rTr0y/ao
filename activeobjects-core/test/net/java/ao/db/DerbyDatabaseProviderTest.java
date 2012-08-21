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
    public void testRenderActionDropColumn() throws IOException
    {
        testRenderAction(new String[0], createActionDropColumn, getDatabaseProvider());
    }
}
