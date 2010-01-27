package net.java.ao.test;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * This the class that holds the configuration for testing. By default configuration properties are looked up, in order:
 * <ol>
 * <li>In the {@link System#getProperty(String) system properties}</li>
 * <li>In the resource denoted by {@link #CONFIGURATION_PROPERTIES}, loaded as a {link Properties properties object}</li>
 * </ol>
 * @author Samuel Le Berrigaud
 * @see #getProperty(String, String)
 */
public final class Configuration
{
    private static final String CONFIGURATION_PROPERTIES = "test-configuration.properties";

    private static final Configuration CONFIG = new Configuration(CONFIGURATION_PROPERTIES);

    private final Properties properties = new Properties();

    public Configuration(String configurationPropertiesResource)
    {
        final InputStream configurationPropertiesResourceStream = this.getClass().getResourceAsStream("/" + configurationPropertiesResource);
        if (configurationPropertiesResourceStream != null)
        {
            try
            {
                properties.load(configurationPropertiesResourceStream);
            }
            catch (IOException e)
            {
                throw new IllegalStateException("Error loading properties from resource <" + configurationPropertiesResource + ">", e);
            }
            finally
            {
                close(configurationPropertiesResourceStream);
            }
        }
        else
        {
            throw new IllegalStateException("Could not load resource at <" + configurationPropertiesResource + ">");
        }
    }

    public static Configuration get()
    {
        return CONFIG;
    }

    public String getUriPrefix()
    {
        return getProperty("db.uri.prefix", null);
    }

    public String getUriSuffix()
    {
        return getProperty("db.uri.suffix", "");
    }

    public String getUserName()
    {
        return getProperty("db.user", null);
    }

    public String getPassword()
    {
        return getProperty("db.pass", "");
    }

    /**
     * Gets a property by looking it up in the system property and then in the properties file if not found. Will return
     * {@code defaultValue} if still not found.
     * @param key the name of the property to look up
     * @param defaultValue the default value for the property when otherwise not found.
     * @return the value associated to the property
     */
    private String getProperty(String key, String defaultValue)
    {
        String value = System.getProperty(key);
        if (value != null)
        {
            return value;
        }
        value = properties.getProperty(key);
        if (value != null)
        {
            return value;
        }
        return defaultValue;
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
