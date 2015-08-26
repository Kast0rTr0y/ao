package net.java.ao.schema.info;

import com.google.common.collect.Sets;
import net.java.ao.Accessor;
import net.java.ao.Entity;
import net.java.ao.Mutator;
import net.java.ao.RawEntity;
import net.java.ao.schema.Case;
import net.java.ao.schema.NameConverters;
import net.java.ao.schema.NotNull;
import net.java.ao.schema.PrimaryKey;
import net.java.ao.schema.Table;
import net.java.ao.schema.TableAnnotationTableNameConverter;
import net.java.ao.schema.UnderscoreFieldNameConverter;
import net.java.ao.schema.UnderscoreTableNameConverter;
import net.java.ao.types.TypeManager;
import org.junit.Before;
import org.junit.Test;

import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SimpleEntityInfoResolverTest {

    private SimpleEntityInfoResolver resolver;

    @Before
    public void setUp() throws Exception {
        NameConverters nameConverters = mock(NameConverters.class);
        when(nameConverters.getTableNameConverter()).thenReturn(new TableAnnotationTableNameConverter(new UnderscoreTableNameConverter(Case.UPPER)));
        when(nameConverters.getFieldNameConverter()).thenReturn(new UnderscoreFieldNameConverter(Case.UPPER));
        resolver = new SimpleEntityInfoResolver(nameConverters, TypeManager.hsql());
    }

    @Test
    public void testSimpleEntity() throws Exception {
        assertEntityInfo(resolver.resolve(SimpleEntity.class), "SIMPLE_ENTITY", Sets.newHashSet("ID", "NAME"));
    }

    @Test
    public void testMixedInEntity() throws Exception {
        assertEntityInfo(resolver.resolve(MixedInEntity.class), "TABLE_ENTITY", Sets.newHashSet("ENTITY_KEY", "DESC", "NAME", "USER_ID"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidEntity1() throws Exception {
        resolver.resolve(InvalidEntity1.class);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidEntity2() throws Exception {
        resolver.resolve(InvalidEntity2.class);
    }

    private <E extends RawEntity<K>, K> void assertEntityInfo(EntityInfo<E, K> entityInfo, String tableName, Set<String> fieldNames) {
        assertNotNull(entityInfo);
        assertEquals(tableName, entityInfo.getName());
        assertEquals(fieldNames, entityInfo.getFieldNames());
    }

    public static interface SimpleEntity extends Entity {
        String getName();

        void setName(String name);
    }

    @Table("TABLE_ENTITY")
    public static interface MixedInEntity extends RawEntity<String>, MixinA, MixinB {
        @PrimaryKey
        String getEntityKey();

        @NotNull
        void setEntityKey(String entityKey);

        @Accessor("DESC")
        String getDescription();

        @Mutator("DESC")
        void setDescription(String description);

    }

    public static interface MixinA {

        String getDescription();

        void setDescription(String description);

    }

    public static interface MixinB extends AbstractMixinB {

        String getName();

        void setName(String name);

    }

    public static interface AbstractMixinB {

        @Accessor("M_NAME")
        String getName();

        @Mutator("M_NAME")
        void setName(String name);

        int getUserId();

        void setUserId(int userId);

    }

    public static interface InvalidEntity1 extends Entity {
        @Accessor("SAME_COL")
        String getMethodOne();

        @Mutator("SAME_COL")
        void setMethodOne(String s);

        @Accessor("SAME_COL")
        String getMethodTwo();

        @Mutator("SAME_COL")
        void setMethodTwo(String s);
    }

    public static interface InvalidEntity2 extends Entity {
        void setName(String name);

        void setName(int name);

        void setName(boolean name);
    }


}
