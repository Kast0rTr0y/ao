/*
 * Copyright 2007 Daniel Spiewak
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 *	    http://www.apache.org/licenses/LICENSE-2.0 
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package net.java.ao.schema;

import java.util.List;

import static com.google.common.base.Preconditions.*;
import static net.java.ao.schema.UnderScoreUtils.*;

/**
 * <p>Imposes an underscore word-separation convention upon field names.
 * This will convert field in the following way:</p>
 *
 * <table border="1">
 * <tr>
 * <td><b>Method Name</b></td>
 * <td><b>Returns Entity?</b></td>
 * <td><b>Field Name</b></td>
 * </tr>
 *
 * <tr>
 * <td>getFirstName</td>
 * <td><code>false</code></td>
 * <td>first_name</td>
 * </tr>
 *
 * <tr>
 * <td>setLastName</td>
 * <td><code>false</code></td>
 * <td>last_name</td>
 * </tr>
 *
 * <tr>
 * <td>getCompany</td>
 * <td><code>true</code></td>
 * <td>company_id</td>
 * </tr>
 *
 * <tr>
 * <td>isCool</td>
 * <td><code>false</code></td>
 * <td>cool</td>
 * </tr>
 * </table>
 *
 * <p>This converter allows for both all-lowercase and all-uppercase
 * field name conventions.  For example, depending on the configuration,
 * <code>getLastName</code> may convert to "LAST_NAME".</p>
 *
 * <p>This converter is all that is required to emulate the ActiveRecord
 * field name conversion.</p>
 *
 * @author Daniel Spiewak
 */
public final class UnderscoreFieldNameConverter extends AbstractFieldNameConverter
{
    private final Case fieldNameCase;

    /**
     * Creates a new field name converter in which all field names will
     * be either fully uppercase or fully lowercase.
     *
     * @param fieldNameCase the case to use for field names
     */
    public UnderscoreFieldNameConverter(Case fieldNameCase)
    {
        this.fieldNameCase = checkNotNull(fieldNameCase);
    }

    public UnderscoreFieldNameConverter(Case fieldNameCase, List<FieldNameResolver> fieldNameResolvers)
    {
        super(fieldNameResolvers);
        this.fieldNameCase = checkNotNull(fieldNameCase);
    }

    @Override
    public String convertName(String name)
    {
        return fieldNameCase.apply(camelCaseToUnderScore(name));
    }
}
