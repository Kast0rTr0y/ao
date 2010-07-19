package net.java.ao.builder;

import net.java.ao.ActiveObjectsDataSource;

import java.sql.Driver;

interface DataSourceFactory
{
    ActiveObjectsDataSource getDataSource(Class<? extends Driver> driverClass, String url, String username, String password);
}
