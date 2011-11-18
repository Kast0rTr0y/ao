package net.java.ao;

import net.java.ao.it.model.Company;
import net.java.ao.it.model.CompanyAddressInfo;
import net.java.ao.it.model.Person;

import static java.lang.String.format;

public final class PostgresQueryTest extends QueryTest
{
    @Override
    protected DatabaseProvider getDatabaseProvider()
    {
        return DatabaseProviders.getPostgreSqlDatabaseProvider();
    }

    @Override
    protected String getExpectedSqlForSimpleSelect()
    {
        return format("SELECT '%s' FROM %s", getPersonId(), getExpectedTableName(Person.class));
    }

    @Override
    protected String getExpectedSqlForSimpleCount()
    {
        return format("SELECT COUNT(*) FROM %s", getExpectedTableName(Person.class));
    }

    @Override
    protected String getExpectSqlForSelectSomeFields()
    {
        return format("SELECT '%s','%s','%s' FROM %s", getPersonId(), getPersonFirstName(), getPersonLastName(), getExpectedTableName(Person.class));
    }

    @Override
    protected String getExpectSqlForCountSomeFields()
    {
        return getExpectedSqlForSimpleCount();
    }

    @Override
    protected String getExpectedSqlForSelectWithWhereClause()
    {
        return format("SELECT '%s' FROM %s WHERE '%s' IS NULL AND '%s' = 3", getPersonId(), getExpectedTableName(Person.class), getPersonLastName(), getPersonAge());
    }

    @Override
    protected String getExpectedSqlForCountWithWhereClause()
    {
        return format("SELECT COUNT(*) FROM %s WHERE '%s' IS NULL AND '%s' = 3", getExpectedTableName(Person.class), getPersonLastName(), getPersonAge());
    }

    @Override
    protected String getExpectedSqlForSelectWithOrderClause()
    {
        return format("SELECT '%s' FROM %s ORDER BY '%s' DESC", getPersonId(), getExpectedTableName(Person.class), getPersonLastName());
    }

    @Override
    protected String getExpectedSqlForCountWithOrderClause()
    {
        return format("SELECT COUNT(*) FROM %s ORDER BY '%s' DESC", getExpectedTableName(Person.class), getPersonLastName());
    }

    @Override
    protected String getExpectedSqlForSelectWithLimit()
    {
        return format("SELECT '%s' FROM %s WHERE '%s' IS NULL AND '%s' = 3 LIMIT 10", getPersonId(), getExpectedTableName(Person.class), getPersonLastName(), getPersonAge());
    }

    @Override
    protected String getExpectedSqlForCountWithLimit()
    {
        return format("SELECT COUNT(*) FROM %s WHERE '%s' IS NULL AND '%s' = 3 LIMIT 10", getExpectedTableName(Person.class), getPersonLastName(), getPersonAge());
    }

    @Override
    protected String getExpectedSqlForSelectWithLimitAndOffset()
    {
        return format("SELECT '%s' FROM %s WHERE '%s' IS NULL AND '%s' = 3 LIMIT 10 OFFSET 4", getPersonId(), getExpectedTableName(Person.class), getPersonLastName(), getPersonAge());
    }

    @Override
    protected String getExpectedSqlForCountWithLimitAndOffset()
    {
        return format("SELECT COUNT(*) FROM %s WHERE '%s' IS NULL AND '%s' = 3 LIMIT 10 OFFSET 4", getExpectedTableName(Person.class), getPersonLastName(), getPersonAge());
    }

    @Override
    protected String getExpectedSqlForSelectWithGroupBy()
    {
        return format("SELECT '%s' FROM %s WHERE '%s' IS NULL AND '%s' = 3 GROUP BY '%s' LIMIT 4", getPersonId(), getExpectedTableName(Person.class), getPersonLastName(), getPersonAge(), getPersonAge());
    }

    @Override
    protected String getExpectedSqlForCountWithGroupBy()
    {
        return format("SELECT COUNT(*) FROM %s WHERE '%s' IS NULL AND '%s' = 3 GROUP BY '%s' LIMIT 4", getExpectedTableName(Person.class), getPersonLastName(), getPersonAge(), getPersonAge());
    }

    @Override
    protected String getExpectedSqlForSelectWithExplicitJoin()
    {
        return format("SELECT '%s' FROM %s JOIN %s ON %s.'%s' = %s.'%s' WHERE '%s' IS NULL AND '%s' = 3 GROUP BY '%s'",
                getPersonId(), getExpectedTableName(Person.class),
                getExpectedTableName(Company.class),
                getExpectedTableName(Person.class), getPersonCompany(),
                getExpectedTableName(Company.class), getCompanyId(),
                getPersonLastName(), getPersonAge(), getPersonUrl());
    }

    @Override
    protected String getExpectedSqlForCountWithExplicitJoin()
    {
        return format("SELECT COUNT(*) FROM %s JOIN %s ON %s.'%s' = %s.'%s' WHERE '%s' IS NULL AND '%s' = 3 GROUP BY '%s'",
                getExpectedTableName(Person.class),
                getExpectedTableName(Company.class),
                getExpectedTableName(Person.class), getPersonCompany(),
                getExpectedTableName(Company.class), getCompanyId(),
                getPersonLastName(), getPersonAge(), getPersonUrl());
    }

    @Override
    protected String getExpectedSqlForSelectWithDefaultJoin()
    {
        return format("SELECT '%s' FROM %s JOIN %s JOIN %s WHERE '%s' IS NULL", getPersonId(), getExpectedTableName(Person.class), getExpectedTableName(Company.class), getExpectedTableName(CompanyAddressInfo.class), getCompanyAddressInfoLine1());
    }

    @Override
    protected String getExpectedSqlForCountWithDefaultJoin()
    {
        return format("SELECT COUNT(*) FROM %s JOIN %s JOIN %s WHERE '%s' IS NULL", getExpectedTableName(Person.class), getExpectedTableName(Company.class), getExpectedTableName(CompanyAddressInfo.class), getCompanyAddressInfoLine1());
    }

    @Override
    protected String getExpectedSqlForSelectWithAliasedJoin()
    {
        return format("SELECT p.'%s' FROM %s AS p JOIN %s AS c JOIN %s AS ca WHERE ca.'%s' IS NULL", getPersonId(), getExpectedTableName(Person.class), getExpectedTableName(Company.class), getExpectedTableName(CompanyAddressInfo.class), getCompanyAddressInfoLine1());
    }

    @Override
    protected String getExpectedSqlForSelectWithAliasedJoinAndSomeFields()
    {
        return format("SELECT p.'%s',p.'%s',p.'%s' FROM %s AS p JOIN %s AS c JOIN %s AS ca WHERE ca.'%s' IS NULL", getPersonId(), getPersonFirstName(), getPersonLastName(), getExpectedTableName(Person.class), getExpectedTableName(Company.class), getExpectedTableName(CompanyAddressInfo.class), getCompanyAddressInfoLine1());
    }

    @Override
    protected String getExpectedSqlForSelectWithAliasedExplicitJoin()
    {
        return format("SELECT p.'%s' FROM %s AS p JOIN %s AS c ON p.'%s' = c.'%s' WHERE p.'%s' IS NULL AND p.'%s' = 3 GROUP BY p.'%s'",
                getPersonId(), getExpectedTableName(Person.class),
                getExpectedTableName(Company.class),
                getPersonCompany(), getCompanyId(),
                getPersonLastName(), getPersonAge(), getPersonUrl());
    }
}
