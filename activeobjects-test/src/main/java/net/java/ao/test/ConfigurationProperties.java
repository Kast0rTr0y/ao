package net.java.ao.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public final class ConfigurationProperties
{
    private static Properties props = null;

    public static String get(String key, final String defaultValue)
    {
        String value = load().getProperty(key, defaultValue);
        if (value.startsWith("${")) // we're using an unfiltered test.properties
        {
            value = defaultValue;
        }
        return value;
    }

    private static Properties load()
    {
        if (props == null)
        {
            props = load(getFileName());
        }
        return props;
    }

    private static Properties load(String configFile)
    {
        final Properties properties = new Properties();
        final InputStream configFileStream = ConfigurationProperties.class.getResourceAsStream("/" + configFile);
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

        // adding all system properties
        properties.putAll(System.getProperties());
        return properties;
    }

    /**
     * Gets the file name for JDBC configuration
     *
     * @return the value of the system property {@code jdbc.file}, or {@code jdbc.properties} if it doesn't exists
     */
    private static String getFileName()
    {
        return System.getProperty("test.file", "test.properties");
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
