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

import net.java.ao.schema.NameConverters;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.info.EntityInfoResolver;
import net.java.ao.schema.info.EntityInfoResolverFactory;
import net.java.ao.types.TypeManager;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.mockito.Matchers.eq;
import static org.mockito.Matchers.isA;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Daniel Spiewak
 */
@RunWith(MockitoJUnitRunner.class)
public class EntityManagerTest
{
    @Mock
    private DatabaseProvider databaseProvider;
    @Mock
    private TypeManager typeManager;

    private EntityManager entityManager;

    @Before
    public void setUp()
    {
        when(databaseProvider.getTypeManager()).thenReturn(typeManager);
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
        final NameConverters nameConverters = mock(NameConverters.class);
        final TableNameConverter tableNameConverter = mock(TableNameConverter.class);
        final SchemaConfiguration schemaConfiguration = mock(SchemaConfiguration.class);

        final EntityInfoResolverFactory entityInfoResolverFactory = mock(EntityInfoResolverFactory.class);
        final EntityInfoResolver entityInfoResolver = mock(EntityInfoResolver.class);
        when(entityInfoResolverFactory.create(isA(NameConverters.class), isA(TypeManager.class))).thenReturn(entityInfoResolver);

        final EntityManagerConfiguration configuration = mock(EntityManagerConfiguration.class);
        when(configuration.getNameConverters()).thenReturn(nameConverters);
        when(nameConverters.getTableNameConverter()).thenReturn(tableNameConverter);
        when(configuration.getSchemaConfiguration()).thenReturn(schemaConfiguration);
        when(configuration.getEntityInfoResolverFactory()).thenReturn(entityInfoResolverFactory);
        return configuration;
    }
}
