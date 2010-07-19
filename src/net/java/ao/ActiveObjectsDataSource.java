package net.java.ao;

import javax.sql.DataSource;
import java.sql.Driver;

/**
 * This is a simple data source with a few more methods, accessors to connection information, and a {@link #dispose()}
 * method to clean up any resources associated with this {@link DataSource data source}.
 */
public interface ActiveObjectsDataSource extends DataSource
{
    /**
     * The URL of the connection, can be {@code null}
     * @return
     */
    String getUrl();

    String getUsername();

    String getPassword();

    Class<? extends Driver> getDriverClass();

    void dispose();
}
