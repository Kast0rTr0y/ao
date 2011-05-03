package net.java.ao.schema;

import static com.google.common.base.Preconditions.checkNotNull;
import static net.java.ao.Common.convertSimpleClassName;
import static net.java.ao.schema.UnderScoreUtils.*;

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
    private final Case tableNameCase;

    public UnderscoreTableNameConverter(Case tableNameCase)
    {
        this.tableNameCase = checkNotNull(tableNameCase);
    }

    @Override
    protected String getName(String entityClassCanonicalName)
    {
        return tableNameCase.apply(camelCaseToUnderScore(convertSimpleClassName(entityClassCanonicalName)));
    }
}