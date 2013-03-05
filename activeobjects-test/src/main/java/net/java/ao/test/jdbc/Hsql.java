package net.java.ao.test.jdbc;

import net.java.ao.test.ConfigurationProperties;

public class Hsql extends AbstractJdbcConfiguration
{
    private static final String TEST_HOME_CONSTANT = "${testHome}"; // to be replaced by replaceTestHomeConstant
    private static final String FILE_URL = "jdbc:hsqldb:${testHome}/database/ao_test";
    private static final String IN_MEMORY_URL = "jdbc:hsqldb:mem:ao_test";
    private static final String DEFAULT_USERNAME = "sa";
    private static final String DEFAULT_PASSWORD = "";
    private static final String DEFAULT_SCHEMA = "PUBLIC";

    public Hsql()
    {
//        super(getFileUrl(), DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_SCHEMA);
        super(IN_MEMORY_URL, DEFAULT_USERNAME, DEFAULT_PASSWORD, DEFAULT_SCHEMA);
    }

    public Hsql(String url, String username, String password, String schema)
    {
        super(url, username, password, schema);
    }

    @Override
    protected String getDefaultUsername()
    {
        return DEFAULT_USERNAME;
    }

    @Override
    protected String getDefaultPassword()
    {
        return DEFAULT_PASSWORD;
    }

    @Override
    protected String getDefaultSchema()
    {
        return DEFAULT_SCHEMA;
    }

    @Override
    protected String getDefaultUrl()
    {
//        return getFileUrl();
        return IN_MEMORY_URL;
    }

    private static String getFileUrl()
    {
        return replaceTestHomeConstant(FILE_URL, getTestHome());
    }

    private static String replaceTestHomeConstant(String in, String testHome)
    {
        if (in == null)
        {
            return null;
        }

        StringBuilder prop = new StringBuilder(in);

        int length = TEST_HOME_CONSTANT.length();
        int index = prop.indexOf(TEST_HOME_CONSTANT);

        while (index != -1)
        {
            prop.replace(index, index + length, testHome);
            index = prop.indexOf(TEST_HOME_CONSTANT);
        }
        return prop.toString();
    }

    private static String getTestHome()
    {
        return ConfigurationProperties.get("test.home", "target/testHome");
    }
}
