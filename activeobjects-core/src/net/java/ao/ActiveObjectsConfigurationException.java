package net.java.ao;

import java.lang.reflect.Method;

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
    
    public ActiveObjectsConfigurationException forMethod(Method method) {
        return new ActiveObjectsConfigurationException(getMessage() + " (" +
                method.getDeclaringClass().getName() + "." + method.getName() + ")");
    }
}
