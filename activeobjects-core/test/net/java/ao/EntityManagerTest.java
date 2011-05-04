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
package net.java.ao;

import net.java.ao.schema.FieldNameConverter;
import net.java.ao.schema.TableNameConverter;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Mockito.*;

/**
 * @author Daniel Spiewak
 */
@RunWith(MockitoJUnitRunner.class)
public class EntityManagerTest
{
    @Mock
    private DatabaseProvider databaseProvider;

    private EntityManager entityManager;

    @Before
    public void setUp()
    {
        entityManager = new EntityManager(databaseProvider, getEntityManagerConfiguration());
    }

    @After
    public void tearDown()
    {
        entityManager = null;
    }

    @Test(expected = RuntimeException.class)
    public void testNullTypeMapper()
    {
        entityManager.setPolymorphicTypeMapper(null);
        entityManager.getPolymorphicTypeMapper();
    }

    private EntityManagerConfiguration getEntityManagerConfiguration()
    {
        final TableNameConverter tableNameConverter = mock(TableNameConverter.class);
        final FieldNameConverter fieldNameConverter = mock(FieldNameConverter.class);
        final SchemaConfiguration schemaConfiguration = mock(SchemaConfiguration.class);

        final EntityManagerConfiguration configuration = mock(EntityManagerConfiguration.class);
        when(configuration.getTableNameConverter()).thenReturn(tableNameConverter);
        when(configuration.getFieldNameConverter()).thenReturn(fieldNameConverter);
        when(configuration.getSchemaConfiguration()).thenReturn(schemaConfiguration);
        return configuration;
    }
}
