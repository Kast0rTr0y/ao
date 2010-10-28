package net.java.ao.test.junit;

import net.java.ao.test.jdbc.Hsql;
import net.java.ao.test.jdbc.Jdbc;
import net.java.ao.test.jdbc.JdbcConfiguration;
import net.java.ao.test.lucene.WithIndex;
import org.junit.rules.MethodRule;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.InitializationError;

import java.util.LinkedList;
import java.util.List;

/**
 *
 */
public class ActiveObjectsJUnitRunner extends BlockJUnit4ClassRunner
{
    private final JdbcConfiguration jdbcConfiguration;
    private final boolean withIndex;

    public ActiveObjectsJUnitRunner(Class<?> klass) throws InitializationError
    {
        super(klass);
        jdbcConfiguration = resolveJdbcConfiguration(klass);
        withIndex = withIndex(klass);
    }

    @Override
    protected List<MethodRule> rules(Object test)
    {
        final LinkedList<MethodRule> methodRules = new LinkedList<MethodRule>(super.rules(test));
        methodRules.add(new ActiveObjectTransactionMethodRule(test, jdbcConfiguration, withIndex));
        return methodRules;
    }

    private boolean withIndex(Class<?> klass)
    {
        return klass.isAnnotationPresent(WithIndex.class);
    }

    private JdbcConfiguration resolveJdbcConfiguration(Class<?> klass)
    {
        if (klass.isAnnotationPresent(Jdbc.class))
        {
            try
            {
                return klass.getAnnotation(Jdbc.class).value().newInstance();
            }
            catch (InstantiationException e)
            {
                throw new RuntimeException(e);
            }
            catch (IllegalAccessException e)
            {
                throw new RuntimeException(e);
            }
        }
        return getDefaultJdbcConfiguration();
    }

    private JdbcConfiguration getDefaultJdbcConfiguration()
    {
        return new Hsql();
    }
}
