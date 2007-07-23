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

import java.sql.Driver;
import java.sql.Types;

import net.java.ao.DatabaseFunction;
import net.java.ao.DatabaseProvider;

/**
 * FIXME	UNTESTED!!!!!
 * 
 * @author Daniel Spiewak
 */
public class SQLServerDatabaseProvider extends DatabaseProvider {

	public SQLServerDatabaseProvider(String uri, String username, String password) {
		super(uri, username, password);
		
		System.err.println("ActiveObjects: Warning, you are using an untested database provider.  Please report any problems.");
	}

	@Override
	public Class<? extends Driver> getDriverClass() throws ClassNotFoundException {
		return (Class<? extends Driver>) Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
	}

	@Override
	protected String renderAutoIncrement() {
		return "IDENTITY(1,1)";
	}

	@Override
	protected String convertTypeToString(int type) {
		switch (type) {
			case Types.TIMESTAMP:
				return "DATETIME";
				
			case Types.DATE:
				return "SMALLDATETIME";
		}
		
		return super.convertTypeToString(type);
	}
	
	@Override
	protected String renderFunction(DatabaseFunction func) {
		switch (func) {
			case CURRENT_DATE:
				return "GetDate()";
				
			case CURRENT_TIMESTAMP:
				return "GetDate()";
		}
		
		return super.renderFunction(func);
	}
}
