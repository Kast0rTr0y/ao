/*
 * Created on May 2, 2007
 */
package net.java.ao;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;

import net.java.ao.db.SupportedProvider;

/**
 * @author Daniel Spiewak
 */
public abstract class DatabaseProvider {
	private String uri, username, password;
	
	protected DatabaseProvider(String uri, String username, String password) {
		this.uri = uri;
		
		this.username = username;
		this.password = password;
	}
	
	public String getURI() {
		return uri;
	}
	
	public String getUsername() {
		return username;
	}
	
	public String getPassword() {
		return password;
	}
	
	protected Connection getConnection() throws SQLException {
		try {
			getDriverClass();
		} catch (ClassNotFoundException e) {
			return null;
		}
		
		Connection back = DriverManager.getConnection(getURI(), getUsername(), getPassword());
		
		return back;
	}

	protected abstract Class<? extends Driver> getDriverClass() throws ClassNotFoundException;
	
	public final static DatabaseProvider getInstance(String uri, String username, String password) {
		SupportedProvider provider = SupportedProvider.getProviderForURI(uri);
		if (provider == null) {
			throw new RuntimeException("Unable to locate a valid database provider for URI: " + uri);
		}
		
		DatabaseProvider back = provider.createInstance(uri, username, password);
		if (back == null) {
			throw new RuntimeException("Unable to instantiate database provider for URI: " + uri);
		}
		
		return back;
	}
}
