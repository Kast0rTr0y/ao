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

import java.sql.Connection;
import java.sql.Driver;
import java.sql.SQLException;
import java.sql.Types;

import net.java.ao.DatabaseProvider;

/**
 * @author Daniel Spiewak
 */
public class MySQLDatabaseProvider extends DatabaseProvider {

	public MySQLDatabaseProvider(String uri, String username, String password) {
		super(uri, username, password);
	}

	public Class<? extends Driver> getDriverClass() throws ClassNotFoundException {
		return (Class<? extends Driver>) Class.forName("com.mysql.jdbc.Driver");
	}
	
	@Override
	protected void setPostConnectionProperties(Connection conn) throws SQLException {
	}
	
	@Override
	protected String convertTypeToString(int type) {
		switch (type) {
			case Types.CLOB:
				return "TEXT";
		}
		
		return super.convertTypeToString(type);
	}

	@Override
	protected String renderAutoIncrement() {
		return "AUTO_INCREMENT";
	}
	
	@Override
	protected String renderAppend() {
		return "ENGINE=InnoDB";
	}
}
