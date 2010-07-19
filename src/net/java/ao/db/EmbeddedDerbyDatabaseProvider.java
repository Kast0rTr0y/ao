/*
 * Copyright 2007 Daniel Spiewak
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 *	    http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.java.ao.db;

import net.java.ao.ActiveObjectsDataSource;
import net.java.ao.Database;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

/**
 * @author Daniel Spiewak
 */
public class EmbeddedDerbyDatabaseProvider extends DerbyDatabaseProvider
{
    public EmbeddedDerbyDatabaseProvider(Database database, ActiveObjectsDataSource dataSource)
    {
        super(database, dataSource);
    }

    @Override
	public Class<? extends Driver> getDriverClass() throws ClassNotFoundException {
		return (Class<? extends Driver>) Class.forName("org.apache.derby.jdbc.EmbeddedDriver");
	}
	

	@Override
	public void dispose() {
		Connection conn = null;
		try {
			getDriverClass();
			conn = DriverManager.getConnection(/* TODO: getURI() + */";shutdown=true");
		} catch (SQLException e) {
		} catch (ClassNotFoundException e) {
		} finally {
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException t) {
				}
			}
		}
		
		super.dispose();
	}
}
