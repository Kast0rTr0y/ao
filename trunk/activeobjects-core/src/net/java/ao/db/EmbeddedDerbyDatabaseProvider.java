/*
 * Copyright 2007 Daniel Spiewak
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.java.ao.db;

import net.java.ao.DisposableDataSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.java.ao.Common.closeQuietly;

/**
 * @author Daniel Spiewak
 */
public class EmbeddedDerbyDatabaseProvider extends DerbyDatabaseProvider
{
    private final String uri;

    public EmbeddedDerbyDatabaseProvider(DisposableDataSource dataSource, String uri)
    {
        super(dataSource);
        this.uri = checkNotNull(uri);
    }

    @Override
    public void dispose()
    {
        Connection conn = null;
        try
        {
            conn = DriverManager.getConnection(uri + ";shutdown=true");
        }
        catch (SQLException e)
        {
            // ignored
        }
        finally
        {
            closeQuietly(conn);
        }
        super.dispose();
    }
}
