package net.java.ao.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * This the class that holds the configuration for testing.
 *
 * @author Samuel Le Berrigaud
 */
public final class JdbcConfiguration
{
    private static JdbcConfiguration config = null;

    private final String url;
    private final String username;
    private final String password;

    private JdbcConfiguration(String configFileName)
    {
        final Properties properties = loadProperties(configFileName);
        url = getProperty(properties, "jdbc.url", "jdbc:hsqldb:mem:ao_test");
        username = getProperty(properties, "jdbc.username", "sa");
        password = getProperty(properties, "jdbc.password", "");
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
            config = new JdbcConfiguration(getFileName());
        }
        return config;
    }

    /**
     * Gets the file name for JDBC configuration
     *
     * @return the value of the system property {@code jdbc.file}, or {@code jdbc.properties} if it doesn't exists
     */
    private static String getFileName()
    {
        final String fileName = System.getProperty("jdbc.file", "jdbc.properties");
        log("Using file <" + fileName + "> for JDBC configuration.");
        return fileName;
    }

    private static Properties loadProperties(String configFile)
    {
        final Properties properties = new Properties();
        final InputStream configFileStream = JdbcConfiguration.class.getResourceAsStream("/" + configFile);
        if (configFileStream != null)
        {
            try
            {
                properties.load(configFileStream);
            }
            catch (IOException e)
            {
                throw new IllegalStateException("Error loading properties from resource <" + configFile + ">", e);
            }
            finally
            {
                close(configFileStream);
            }
        }
        else
        {
            log("Didn't not find config file <" + configFile + "> will be using default properties");
        }
        return properties;
    }

    private static void log(String msg)
    {
        System.out.println(msg);
    }

    private static String getProperty(Properties properties, String key, final String defaultValue)
    {
        String value = properties.getProperty(key, defaultValue);
        if (value.startsWith("${")) // we're using an unfiltered jdbc.properties
        {
            value = defaultValue;
        }
        log("<" + key + "> is defined as <" + value + ">");
        return value;
    }

    private static void close(InputStream stream)
    {
        try
        {
            stream.close();
        }
        catch (IOException e)
        {
            // ignore
        }
    }
}
