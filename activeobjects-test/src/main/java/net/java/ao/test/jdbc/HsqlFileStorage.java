package net.java.ao.test.jdbc;

import net.java.ao.test.ConfigurationProperties;

public class HsqlFileStorage extends Hsql
{
    private static final String TEST_HOME_CONSTANT = "${testHome}"; // to be replaced by replaceTestHomeConstant
    private static final String FILE_URL = "jdbc:hsqldb:${testHome}/database/ao_test";
    private static final String DEFAULT_TEST_HOME = "target/testHome";

    public HsqlFileStorage()
    {
        super(getFileUrl());
    }

    public HsqlFileStorage(String url, String username, String password, String schema)
    {
        super(url, username, password, schema);
    }

    @Override
    protected String getDefaultUrl()
    {
        return getFileUrl();
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
        return ConfigurationProperties.get("test.home", DEFAULT_TEST_HOME);
    }
}
