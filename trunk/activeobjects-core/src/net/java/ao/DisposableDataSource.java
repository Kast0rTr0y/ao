package net.java.ao;

import javax.sql.DataSource;

/**
 * This is a simple data source with one more method {@link #dispose()} to clean up any resources associated with this
 * {@link DataSource data source}.
 */
public interface DisposableDataSource extends DataSource
{
    void dispose();
}
