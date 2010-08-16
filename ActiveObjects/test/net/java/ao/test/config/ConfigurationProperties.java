package net.java.ao.test.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

final class ConfigurationProperties
{
    private static Properties props = null;

    static String get(String key, final String defaultValue)
    {
        String value = load().getProperty(key, defaultValue);
        if (value.startsWith("${")) // we're using an unfiltered test.properties
        {
            value = defaultValue;
        }
        log("<" + key + "> is defined as <" + value + ">");
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
        else
        {
            log("Didn't not find config file <" + configFile + "> will be using default properties");
        }
        return properties;
    }

    /**
     * Gets the file name for JDBC configuration
     *
     * @return the value of the system property {@code jdbc.file}, or {@code jdbc.properties} if it doesn't exists
     */
    private static String getFileName()
    {
        final String fileName = System.getProperty("test.file", "test.properties");
        log("Using file <" + fileName + "> for configuration properties.");
        return fileName;
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

    private static void log(String msg)
    {
        System.out.println(msg);
    }
}
