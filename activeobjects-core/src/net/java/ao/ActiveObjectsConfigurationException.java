package net.java.ao;

/**
 * Thrown in case ActiveObjects discovers invalid configuration options, such as empty String default values (not
 * supported by all databases).
 */
public class ActiveObjectsConfigurationException extends ActiveObjectsException
{
    public ActiveObjectsConfigurationException()
    {
    }

    public ActiveObjectsConfigurationException(String message)
    {
        super(message);
    }

    public ActiveObjectsConfigurationException(String message, Throwable cause)
    {
        super(message, cause);
    }

    public ActiveObjectsConfigurationException(Throwable cause)
    {
        super(cause);
    }
}
