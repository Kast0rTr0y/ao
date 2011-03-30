package net.java.ao.schema;

import net.java.ao.Common;

/**
 * <p>Imposes a standard camelCase convention upon table names. This will
 * convert entity names in the following way:</p>
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
 * <td>licenseRegistration</td>
 * </tr>
 *
 * <tr>
 * <td><code>Volume4</code></td>
 * <td>volume4</td>
 * </tr>
 *
 * <tr>
 * <td><code>Company</code></td>
 * <td>company</td>
 * </tr>
 * </table>
 */
public final class CamelCaseTableNameConverter extends CanonicalClassNameTableNameConverter
{
    @Override
    protected String getName(String entityClassCanonicalName)
    {
        return Common.convertDowncaseName(Common.convertSimpleClassName(entityClassCanonicalName));
    }
}
