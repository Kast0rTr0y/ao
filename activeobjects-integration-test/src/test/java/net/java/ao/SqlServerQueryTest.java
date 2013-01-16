package net.java.ao;

import net.java.ao.it.model.Company;
import net.java.ao.it.model.CompanyAddressInfo;
import net.java.ao.it.model.Person;
import net.java.ao.test.junit.SqlServerIntegrationTest;
import org.junit.experimental.categories.Category;

import static java.lang.String.*;

@Category(SqlServerIntegrationTest.class)
public final class SqlServerQueryTest extends QueryTest
{
    @Override
    protected DatabaseProvider getDatabaseProvider()
    {
        return DatabaseProviders.getMsSqlDatabaseProvider();
    }

    @Override
    protected String getExpectedSqlForSimpleSelect()
    {
        return format("SELECT %s FROM %s", getPersonId(), getExpectedTableName(Person.class));
    }

    @Override
    protected String getExpectedSqlForSimpleCount()
    {
        return format("SELECT COUNT(*) FROM %s", getExpectedTableName(Person.class));
    }

    @Override
    protected String getExpectSqlForSelectSomeFields()
    {
        return format("SELECT %s,%s,%s FROM %s", getPersonId(), getPersonFirstName(), getPersonLastName(), getExpectedTableName(Person.class));
    }

    @Override
    protected String getExpectSqlForCountSomeFields()
    {
        return getExpectedSqlForSimpleCount();
    }

    @Override
    protected String getExpectedSqlForSelectWithWhereClause()
    {
        return format("SELECT %s FROM %s WHERE %s IS NULL AND %s = 3", getPersonId(), getExpectedTableName(Person.class), getPersonLastName(), getPersonAge());
    }

    @Override
    protected String getExpectedSqlForCountWithWhereClause()
    {
        return format("SELECT COUNT(*) FROM %s WHERE %s IS NULL AND %s = 3", getExpectedTableName(Person.class), getPersonLastName(), getPersonAge());
    }

    @Override
    protected String getExpectedSqlForSelectWithOrderClause()
    {
        return format("SELECT %s FROM %s ORDER BY %s DESC", getPersonId(), getExpectedTableName(Person.class), getPersonLastName());
    }

    @Override
    protected String getExpectedSqlForCountWithOrderClause()
    {
        return format("SELECT COUNT(*) FROM %s ORDER BY %s DESC", getExpectedTableName(Person.class), getPersonLastName());
    }

    @Override
    protected String getExpectedSqlForSelectWithLimit()
    {
        return format("SELECT TOP 10 %s FROM %s WHERE %s IS NULL AND %s = 3", getPersonId(), getExpectedTableName(Person.class), getPersonLastName(), getPersonAge());
    }

    @Override
    protected String getExpectedSqlForCountWithLimit()
    {
        return format("SELECT TOP 10 COUNT(*) FROM %s WHERE %s IS NULL AND %s = 3", getExpectedTableName(Person.class), getPersonLastName(), getPersonAge());
    }

    @Override
    protected String getExpectedSqlForSelectWithLimitAndOffset()
    {
        return format("SELECT TOP 14 %s FROM %s WHERE %s IS NULL AND %s = 3", getPersonId(), getExpectedTableName(Person.class), getPersonLastName(), getPersonAge());
    }

    @Override
    protected String getExpectedSqlForCountWithLimitAndOffset()
    {
        return format("SELECT TOP 14 COUNT(*) FROM %s WHERE %s IS NULL AND %s = 3", getExpectedTableName(Person.class), getPersonLastName(), getPersonAge());
    }

    @Override
    protected String getExpectedSqlForSelectWithGroupBy()
    {
        return format("SELECT TOP 4 %s FROM %s WHERE %s IS NULL AND %s = 3 GROUP BY %s", getPersonId(), getExpectedTableName(Person.class), getPersonLastName(), getPersonAge(), getPersonAge());
    }

    @Override
    protected String getExpectedSqlForCountWithGroupBy()
    {
        return format("SELECT TOP 4 COUNT(*) FROM %s WHERE %s IS NULL AND %s = 3 GROUP BY %s", getExpectedTableName(Person.class), getPersonLastName(), getPersonAge(), getPersonAge());
    }

    @Override
    protected String getExpectedSqlForSelectWithExplicitJoin()
    {
        return format("SELECT %s FROM %s JOIN %s ON %s.%s = %s.%s WHERE %s IS NULL AND %s = 3 GROUP BY %s",
                getPersonId(), getExpectedTableName(Person.class),
                getExpectedTableName(Company.class),
                getExpectedTableName(Person.class), getPersonCompany(),
                getExpectedTableName(Company.class), getCompanyId(),
                getPersonLastName(), getPersonAge(), getPersonUrl());
    }

    @Override
    protected String getExpectedSqlForCountWithExplicitJoin()
    {
        return format("SELECT COUNT(*) FROM %s JOIN %s ON %s.%s = %s.%s WHERE %s IS NULL AND %s = 3 GROUP BY %s",
                getExpectedTableName(Person.class),
                getExpectedTableName(Company.class),
                getExpectedTableName(Person.class), getPersonCompany(),
                getExpectedTableName(Company.class), getCompanyId(),
                getPersonLastName(), getPersonAge(), getPersonUrl());
    }

    @Override
    protected String getExpectedSqlForSelectWithDefaultJoin()
    {
        return format("SELECT %s FROM %s JOIN %s JOIN %s WHERE %s IS NULL", getPersonId(), getExpectedTableName(Person.class), getExpectedTableName(Company.class), getExpectedTableName(CompanyAddressInfo.class), getCompanyAddressInfoLine1());
    }

    @Override
    protected String getExpectedSqlForCountWithDefaultJoin()
    {
        return format("SELECT COUNT(*) FROM %s JOIN %s JOIN %s WHERE %s IS NULL", getExpectedTableName(Person.class), getExpectedTableName(Company.class), getExpectedTableName(CompanyAddressInfo.class), getCompanyAddressInfoLine1());
    }

    @Override
    protected String getExpectedSqlForSelectWithAliasedJoin()
    {
        return format("SELECT p.%s FROM %s p JOIN %s c JOIN %s ca WHERE ca.%s IS NULL", getPersonId(), getExpectedTableName(Person.class), getExpectedTableName(Company.class), getExpectedTableName(CompanyAddressInfo.class), getCompanyAddressInfoLine1());
    }

    @Override
    protected String getExpectedSqlForSelectWithAliasedJoinAndSomeFields()
    {
        return format("SELECT p.%s,p.%s,p.%s FROM %s p JOIN %s c JOIN %s ca WHERE ca.%s IS NULL", getPersonId(), getPersonFirstName(), getPersonLastName(), getExpectedTableName(Person.class), getExpectedTableName(Company.class), getExpectedTableName(CompanyAddressInfo.class), getCompanyAddressInfoLine1());
    }

    @Override
    protected String getExpectedSqlForSelectWithAliasedExplicitJoin()
    {
        return format("SELECT p.%s FROM %s p JOIN %s c ON p.%s = c.%s WHERE p.%s IS NULL AND p.%s = 3 GROUP BY p.%s",
                getPersonId(), getExpectedTableName(Person.class),
                getExpectedTableName(Company.class),
                getPersonCompany(), getCompanyId(),
                getPersonLastName(), getPersonAge(), getPersonUrl());
    }
}
