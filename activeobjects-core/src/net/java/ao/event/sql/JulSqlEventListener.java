package net.java.ao.event.sql;

import net.java.ao.event.EventListener;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * A simple event listener that logs SQL events to the {@code net.java.ao} JUL logger
 */
public class JulSqlEventListener
{
    private static final Logger logger = Logger.getLogger("net.java.ao");

    @EventListener
    public void sql(SqlEvent event)
    {
        logger.log(Level.INFO, event.toString());
    }
}
