package net.java.ao.schema;

import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import net.java.ao.Common;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * <p>Imposes an underscore word-separation convention on table
 * names.  This will convert entity names in the following way:</p>
 *
 * <table border="1">
 * <tr>
 * <td><b>Entity Name</b></td>
 * <td><b>Table Name</b></td>
 * </tr>
 *
 * <tr>
 * <td><code>Person</code></td>
 * <td>person</td>
 * </tr>
 *
 * <tr>
 * <td><code>LicenseRegistration</code></td>
 * <td>license_registration</td>
 * </tr>
 *
 * <tr>
 * <td><code>Volume4</code></td>
 * <td>volume_4</td>
 * </tr>
 *
 * <tr>
 * <td><code>Company</code></td>
 * <td>company</td>
 * </tr>
 * </table>
 *
 * <p>This converter allows for both all-lowercase and all-uppercase
 * table name conventions.  For example, depending on the configuration,
 * <code>LicenseRegistration</code> may convert to "LICENSE_REGISTRATION".</p>
 *
 * <p>This converter, coupled with {@link PluralizedTableNameConverter} is
 * all that is required to emulate the ActiveRecord table name conversion.</p>
 */
public final class UnderscoreTableNameConverter extends CanonicalClassNameTableNameConverter
{
    private static final Pattern WORD_PATTERN = Pattern.compile("([a-z\\d])([A-Z\\d])");

    private final Case tableNamesCase;

    public UnderscoreTableNameConverter(Case tableNamesCase)
    {
        this.tableNamesCase = checkNotNull(tableNamesCase);
    }

    @Override
    protected String getName(String entityClassCanonicalName)
    {
        final Matcher matcher = WORD_PATTERN.matcher(Common.convertSimpleClassName(entityClassCanonicalName));
        return tableNamesCase.apply(matcher.replaceAll("$1_$2"));
    }

    public static enum Case
    {
        UPPER
                {
                    String apply(String tableName)
                    {
                        return tableName.toUpperCase(Locale.ENGLISH);
                    }
                },
        LOWER
                {
                    @Override
                    String apply(String tableName)
                    {
                        return tableName.toLowerCase(Locale.ENGLISH);
                    }
                };

        abstract String apply(String tableName);
    }
}