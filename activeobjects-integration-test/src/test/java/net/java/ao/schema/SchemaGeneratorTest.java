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

import com.google.common.collect.ImmutableList;
import net.java.ao.Entity;
import net.java.ao.RawEntity;
import net.java.ao.it.DatabaseProcessor;
import net.java.ao.it.model.Company;
import net.java.ao.it.model.Pen;
import net.java.ao.it.model.Person;
import net.java.ao.it.model.PersonSuit;
import net.java.ao.schema.ddl.DDLField;
import net.java.ao.schema.ddl.DDLForeignKey;
import net.java.ao.schema.ddl.DDLIndex;
import net.java.ao.schema.ddl.DDLIndexField;
import net.java.ao.schema.ddl.DDLTable;
import net.java.ao.test.ActiveObjectsIntegrationTest;
import net.java.ao.test.jdbc.Data;
import net.java.ao.types.LogicalTypes;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.sql.Types;
import java.util.Arrays;
import java.util.List;

import static net.java.ao.types.LogicalTypes.integerType;
import static net.java.ao.types.LogicalTypes.stringType;
import static net.java.ao.types.LogicalTypes.urlType;
import static org.hamcrest.Matchers.arrayWithSize;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * @author Daniel Spiewak
 */
@Data(DatabaseProcessor.class)
public final class SchemaGeneratorTest extends ActiveObjectsIntegrationTest {
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @SuppressWarnings("null")
    @Test
    public void testParseDDL() {
        String[] expectedFields = {
                getFieldName(Person.class, "getID"),
                getFieldName(Person.class, "getFirstName"),
                getFieldName(Person.class, "getLastName"),
                getFieldName(Person.class, "getProfession"),
                getFieldName(Person.class, "getAge"),
                getFieldName(Person.class, "getURL"),
                getFieldName(Person.class, "getCompany"),
                getFieldName(Person.class, "getImage"),
                getFieldName(Person.class, "isActive"),
                getFieldName(Person.class, "getModified")};

        final TableNameConverter tableNameConverter = entityManager.getNameConverters().getTableNameConverter();
        final String tableName = tableNameConverter.getName(Person.class);

        List<DDLIndex> expectedIndexes = ImmutableList.<DDLIndex>builder()
                .add(DDLIndex.builder()
                        .field(DDLIndexField.builder()
                                .fieldName(getFieldName(Person.class, "getAge"))
                                .type(entityManager.getProvider().getTypeManager().getType(Integer.class))
                                .build())
                        .table(tableName)
                        .indexName(indexName(tableName, getFieldName(Person.class, "getAge")))
                        .build())
                .add(DDLIndex.builder()
                        .field(DDLIndexField.builder()
                                .fieldName(getFieldName(Person.class, "getCompany"))
                                .type(entityManager.getProvider().getTypeManager().getType(Integer.class))
                                .build())
                        .indexName(indexName(tableName, getFieldName(Person.class, "getCompany")))
                        .table(tableName)
                        .build())
                .add(DDLIndex.builder()
                        .fields(
                                DDLIndexField.builder()
                                        .fieldName(getFieldName(Person.class, "getFirstName"))
                                        .type(entityManager.getProvider().getTypeManager().getType(String.class))
                                        .build(),
                                DDLIndexField.builder()
                                        .fieldName(getFieldName(Person.class, "getLastName"))
                                        .type(entityManager.getProvider().getTypeManager().getType(String.class))
                                        .build()
                        )
                        .indexName(indexName(tableName, "names"))
                        .table(tableName)
                        .build())
                .build();

        DDLTable[] parsedTables = SchemaGenerator.parseDDL(entityManager.getProvider(), entityManager.getNameConverters(), PersonSuit.class, Pen.class);

        assertEquals(6, parsedTables.length);

        DDLTable personDDL = null;
        for (DDLTable table : parsedTables) {
            if (table.getName().equals(tableName)) {
                personDDL = table;
                break;
            }
        }

        assertNotNull(personDDL);
        assertEquals(expectedFields.length, personDDL.getFields().length);
        assertEquals(1, personDDL.getForeignKeys().length);

        for (DDLField field : personDDL.getFields()) {
            boolean found = false;
            for (String expectedField : expectedFields) {
                if (expectedField.equals(field.getName())) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                fail("Field " + field.getName() + " was unexpected");
            }
        }

        DDLField urlField = findField(personDDL, Person.class, "getURL");
        DDLField ageField = findField(personDDL, Person.class, "getAge");
        DDLField lastNameField = findField(personDDL, Person.class, "getLastName");
        DDLField idField = findField(personDDL, Person.class, "getID");
        DDLField cidField = findField(personDDL, Person.class, "getCompany");

        assertEquals(Types.VARCHAR, urlField.getJdbcType());
        assertEquals(urlType(), urlField.getType().getLogicalType());

        assertFalse(urlField.isAutoIncrement());
        assertTrue(urlField.isNotNull());
        assertFalse(urlField.isPrimaryKey());
        assertTrue(urlField.isUnique());

        assertNotNull(urlField.getDefaultValue());

        assertEquals(Types.INTEGER, ageField.getJdbcType());
        assertEquals(integerType(), ageField.getType().getLogicalType());

        assertFalse(ageField.isAutoIncrement());
        assertFalse(ageField.isNotNull());
        assertFalse(ageField.isPrimaryKey());
        assertFalse(ageField.isUnique());

        assertEquals(Types.VARCHAR, lastNameField.getJdbcType());
        assertEquals(stringType(), lastNameField.getType().getLogicalType());
        assertEquals(new Integer(127), lastNameField.getType().getQualifiers().getStringLength());

        assertFalse(lastNameField.isAutoIncrement());
        assertFalse(lastNameField.isNotNull());
        assertFalse(lastNameField.isPrimaryKey());
        assertFalse(lastNameField.isUnique());

        assertEquals(Types.INTEGER, idField.getJdbcType());
        assertEquals(integerType(), idField.getType().getLogicalType());

        assertTrue(idField.isAutoIncrement());
        assertTrue(idField.isNotNull());
        assertTrue(idField.isPrimaryKey());
        assertFalse(idField.isUnique());

        assertNull(idField.getDefaultValue());

        assertEquals(LogicalTypes.entityType(Company.class, entityManager.getProvider().getTypeManager().getType(long.class), long.class),
                cidField.getType().getLogicalType());
        assertEquals(Types.BIGINT, cidField.getJdbcType());

        assertFalse(cidField.isAutoIncrement());
        assertFalse(cidField.isNotNull());
        assertFalse(cidField.isPrimaryKey());
        assertFalse(cidField.isUnique());

        assertNull(cidField.getDefaultValue());

        DDLForeignKey cidKey = null;
        for (DDLForeignKey key : personDDL.getForeignKeys()) {
            if (key.getField().equals(getFieldName(Person.class, "getCompany"))) {
                cidKey = key;
                break;
            }
        }

        assertNotNull(cidKey);

        assertEquals(tableName, cidKey.getDomesticTable());
        assertEquals(getFieldName(Person.class, "getCompany"), cidKey.getField());
        assertEquals(getFieldName(Company.class, "getCompanyID"), cidKey.getForeignField());
        assertEquals(tableNameConverter.getName(Company.class), cidKey.getTable());

        assertEquals(expectedIndexes.size(), personDDL.getIndexes().length);
        assertThat(Arrays.asList(personDDL.getIndexes()), containsInAnyOrder(expectedIndexes.toArray()));
    }

    @Test
    public void testParseIndexIgnoresUnrelatedInterfaces() throws Exception {
        TableNameConverter tableNameConverter = spy(entityManager.getTableNameConverter());
        FieldNameConverter fieldNameConverter = spy(entityManager.getFieldNameConverter());
        DDLIndex[] indexes = SchemaGenerator.parseIndexes(
                entityManager.getProvider(),
                entityManager.getNameConverters(),
                AoEntity.class
        );
        assertNotNull(indexes);
        verify(tableNameConverter, never()).getName((Class) RandomInterface.class);
    }

    @Test
    public void testParseDDLRejectsUnrelatedFields() throws Exception {
        expectedException.expect(RuntimeException.class);
        SchemaGenerator.parseDDL(entityManager.getProvider(), entityManager.getNameConverters(), AoValueHolder.class);
    }

    @Test
    public void testParseDDLIgnoresUnrelatedInterfaces() throws Exception {
        DDLTable[] tables = SchemaGenerator.parseDDL(entityManager.getProvider(), entityManager.getNameConverters(),
                AoChildEntity.class);
        assertThat(tables, arrayWithSize(1));
    }

    public interface AoEntity extends Entity, OtherAoEntity<Integer>, RandomInterface {
    }

    public interface OtherAoEntity<T> extends RawEntity<T> {
    }

    public interface RandomInterface {
    }

    public interface AoValueHolder extends Entity {
        RandomInterface getName();
    }

    public interface AnotherInterface {
        String getName();
    }

    public interface AoChildEntity extends Entity, AnotherInterface {
        String getDescription();
    }
}
