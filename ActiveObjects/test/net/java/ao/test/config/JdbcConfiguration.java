package net.java.ao.test.config;

public final class JdbcConfiguration
{
    private static JdbcConfiguration config = null;

    private final String url;
    private final String username;
    private final String password;

    private JdbcConfiguration()
    {
        url = ConfigurationProperties.get("jdbc.url", "jdbc:hsqldb:mem:ao_test");
        username = ConfigurationProperties.get("jdbc.username", "sa");
        password = ConfigurationProperties.get("jdbc.password", "");
    }

    public String getUrl()
    {
        return url;
    }

    public String getUsername()
    {
        return username;
    }

    public String getPassword()
    {
        return password;
    }

    public static JdbcConfiguration get()
    {
        if (config == null)
        {
            config = new JdbcConfiguration();
        }
        return config;
    }
}
