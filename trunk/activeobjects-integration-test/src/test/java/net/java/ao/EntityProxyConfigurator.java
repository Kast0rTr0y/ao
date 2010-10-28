package net.java.ao;

/**
 *
 */
public final class EntityProxyConfigurator
{
    public static void setIgnorePreload(boolean ignore)
    {
        EntityProxy.ignorePreload = ignore;
    }
}
