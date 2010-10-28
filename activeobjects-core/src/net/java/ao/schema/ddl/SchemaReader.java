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
package net.java.ao.schema.ddl;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import net.java.ao.Common;
import net.java.ao.DatabaseProvider;
import net.java.ao.SchemaConfiguration;
import net.java.ao.schema.helper.DatabaseMetaDataReader;
import net.java.ao.schema.helper.DatabaseMetaDataReaderImpl;
import net.java.ao.schema.helper.Field;
import net.java.ao.schema.helper.ForeignKey;
import net.java.ao.sql.SqlUtils;

import java.sql.*;
import java.util.*;

import static com.google.common.collect.Lists.newArrayList;

/**
 * WARNING: <i>Not</i> part of the public API.  This class is public only
 * to allow its use within other packages in the ActiveObjects library.
 *
 * @author Daniel Spiewak
 */
public final class SchemaReader
{
    /**
     * Currently doesn't account for:
     *
     * <ul>
     * <li>setOnUpdate</li>
     * <li>setUnique</li>
     * </ul>
     */
    public static DDLTable[] readSchema(DatabaseProvider provider, SchemaConfiguration schemaConfiguration) throws SQLException
    {
        return readSchema(provider, schemaConfiguration, true);
    }

    public static DDLTable[] readSchema(DatabaseProvider provider, SchemaConfiguration schemaConfiguration, final boolean includeForeignKeys) throws SQLException
    {
        final DatabaseMetaDataReader databaseMetaDataReader = new DatabaseMetaDataReaderImpl(provider, schemaConfiguration);
        Connection connection = null;
        try
        {
            connection = provider.getConnection();

            final DatabaseMetaData databaseMetaData = connection.getMetaData();
            final List<DDLTable> tables = newArrayList(Iterables.transform(databaseMetaDataReader.getTableNames(databaseMetaData), new Function<String, DDLTable>()
            {
                public DDLTable apply(String tableName)
                {
                    return readTable(databaseMetaDataReader, databaseMetaData, tableName, includeForeignKeys);
                }
            }));

            return tables.toArray(new DDLTable[tables.size()]);
        }
        finally
        {
            SqlUtils.closeQuietly(connection);
        }
    }

    private static DDLTable readTable(DatabaseMetaDataReader databaseMetaDataReader, DatabaseMetaData databaseMetaData, String tableName, boolean includeForeignKeys)
    {
        DDLTable table = new DDLTable();
        table.setName(tableName);

        final List<DDLField> fields = readFields(databaseMetaDataReader, databaseMetaData, tableName);
        table.setFields(fields.toArray(new DDLField[fields.size()]));

        if (includeForeignKeys)
        {
            final List<DDLForeignKey> foreignKeys = readForeignKeys(databaseMetaDataReader, databaseMetaData, tableName);
            table.setForeignKeys(foreignKeys.toArray(new DDLForeignKey[foreignKeys.size()]));
        }

        return table;
    }
    
    private static List<DDLField> readFields(DatabaseMetaDataReader databaseMetaDataReader, DatabaseMetaData databaseMetaData, String tableName)
    {
        return newArrayList(Iterables.transform(databaseMetaDataReader.getFields(databaseMetaData, tableName), new Function<Field, DDLField>()
        {
            public DDLField apply(Field from)
            {
                DDLField field = new DDLField();
                field.setAutoIncrement(from.isAutoIncrement());
                field.setDefaultValue(from.getDefaultValue());
                field.setName(from.getName());
                field.setNotNull(from.isNotNull());
                field.setPrecision(from.getPrecision());
                field.setPrimaryKey(from.isPrimaryKey());
                field.setScale(from.getScale());
                field.setType(from.getDatabaseType());
                return field;
            }
        }));
    }

    private static List<DDLForeignKey> readForeignKeys(DatabaseMetaDataReader databaseMetaDataReader, DatabaseMetaData databaseMetaData, String tableName)
    {
        return newArrayList(Iterables.transform(databaseMetaDataReader.getForeignKeys(databaseMetaData, tableName), new Function<ForeignKey, DDLForeignKey>()
        {
            public DDLForeignKey apply(ForeignKey from)
            {
                DDLForeignKey key = new DDLForeignKey();
                key.setForeignField(from.getForeignFieldName());
                key.setField(from.getLocalFieldName());
                key.setTable(from.getForeignTableName());
                key.setDomesticTable(from.getLocalTableName());
                return key;
            }
        }));
    }

    /**
     * Returns the difference between <code>from</code> and
     * <code>onto</code> with a bias toward <code>from</code>.
     * Thus, if a table is present in <code>from</code> but not
     * <code>onto</code>, a <code>CREATE TABLE</code>
     * statement will be generated.
     */
    public static DDLAction[] diffSchema(DDLTable[] fromArray, DDLTable[] ontoArray, boolean caseSensetive)
    {
        Set<DDLAction> actions = new HashSet<DDLAction>();

        List<DDLTable> createTables = new ArrayList<DDLTable>();
        List<DDLTable> dropTables = new ArrayList<DDLTable>();
        List<DDLTable> alterTables = new ArrayList<DDLTable>();

        Map<String, DDLTable> from = new HashMap<String, DDLTable>();
        Map<String, DDLTable> onto = new HashMap<String, DDLTable>();

        for (DDLTable table : fromArray)
        {
            String tableName = table.getName();
            if (!caseSensetive)
            {
                tableName = tableName.toLowerCase();
            }

            from.put(tableName, table);
        }
        for (DDLTable table : ontoArray)
        {
            String tableName = table.getName();
            if (!caseSensetive)
            {
                tableName = tableName.toLowerCase();
            }

            onto.put(tableName, table);
        }

        for (DDLTable table : fromArray)
        {
            String tableName = transform(table.getName(), caseSensetive);

            if (onto.containsKey(tableName))
            {
                alterTables.add(table);
            }
            else
            {
                createTables.add(table);
            }
        }

        for (DDLTable table : ontoArray)
        {
            String tableName = transform(table.getName(), caseSensetive);

            if (!from.containsKey(tableName))
            {
                dropTables.add(table);
            }
        }

        for (DDLTable table : createTables)
        {
            DDLAction action = new DDLAction(DDLActionType.CREATE);
            action.setTable(table);
            actions.add(action);
        }


        List<DDLForeignKey> dropKeys = new ArrayList<DDLForeignKey>();

        for (DDLTable table : dropTables)
        {
            DDLAction action = new DDLAction(DDLActionType.DROP);
            action.setTable(table);
            actions.add(action);

            // remove all foreign keys on that table
            dropKeys.addAll(Arrays.asList(table.getForeignKeys()));

            // remove all foreign to that table
            for (DDLTable alterTable : alterTables)
            {
                for (DDLForeignKey fKey : alterTable.getForeignKeys())
                {
                    if (equals(fKey.getTable(), table.getName(), caseSensetive))
                    {
                        dropKeys.add(fKey);
                    }
                }
            }
        }

        for (DDLTable fromTable : alterTables)
        {
            final String s = fromTable.getName();
            String tableName = transform(s, caseSensetive);

            DDLTable ontoTable = onto.get(tableName);

            List<DDLField> createFields = new ArrayList<DDLField>();
            List<DDLField> dropFields = new ArrayList<DDLField>();
            List<DDLField> alterFields = new ArrayList<DDLField>();

            Map<String, DDLField> fromFields = new HashMap<String, DDLField>();
            Map<String, DDLField> ontoFields = new HashMap<String, DDLField>();

            for (DDLField field : fromTable.getFields())
            {
                String fieldName = transform(field.getName(), caseSensetive);

                fromFields.put(fieldName, field);
            }
            for (DDLField field : ontoTable.getFields())
            {
                String fieldName = transform(field.getName(), caseSensetive);

                ontoFields.put(fieldName, field);
            }

            for (DDLField field : fromTable.getFields())
            {
                String fieldName = transform(field.getName(), caseSensetive);

                if (ontoFields.containsKey(fieldName))
                {
                    alterFields.add(field);
                }
                else
                {
                    createFields.add(field);
                }
            }

            for (DDLField field : ontoTable.getFields())
            {
                String fieldName = transform(field.getName(), caseSensetive);

                if (!fromFields.containsKey(fieldName))
                {
                    dropFields.add(field);
                }
            }

            for (DDLField field : createFields)
            {
                DDLAction action = new DDLAction(DDLActionType.ALTER_ADD_COLUMN);
                action.setTable(fromTable);
                action.setField(field);
                actions.add(action);
            }

            for (DDLField field : dropFields)
            {
                DDLAction action = new DDLAction(DDLActionType.ALTER_DROP_COLUMN);
                action.setTable(fromTable);
                action.setField(field);
                actions.add(action);
            }

            for (DDLField fromField : alterFields)
            {
                String fieldName = transform(fromField.getName(), caseSensetive);

                DDLField ontoField = ontoFields.get(fieldName);

                if (fromField.getDefaultValue() == null && ontoField.getDefaultValue() != null)
                {
                    if (!ontoField.getDefaultValue().toString().equals("CURRENT_TIMESTAMP"))
                    {        // super-hack for MySQL
                        actions.add(createColumnAlterAction(fromTable, ontoField, fromField));
                    }
                }
                else if (fromField.getDefaultValue() != null
                        && !Common.fuzzyCompare(fromField.getDefaultValue(), ontoField.getDefaultValue()))
                {
                    actions.add(createColumnAlterAction(fromTable, ontoField, fromField));
                } /*else if (!fromField.getOnUpdate().equals(ontoField.getOnUpdate())) {
					actions.add(createColumnAlterAction(fromTable, fromField));
				} else if (fromField.getPrecision() != ontoField.getPrecision()) {
					actions.add(createColumnAlterAction(fromTable, ontoField, fromField));
				} else if (fromField.getScale() != ontoField.getScale()) {
					actions.add(createColumnAlterAction(fromTable, ontoField, fromField));
				}*/
                else if (!Common.fuzzyTypeCompare(fromField.getType().getType(), ontoField.getType().getType()))
                {
                    actions.add(createColumnAlterAction(fromTable, ontoField, fromField));
                } /*else if (fromField.isAutoIncrement() != ontoField.isAutoIncrement()) {
					actions.add(createColumnAlterAction(fromTable, ontoField, fromField));
				} else if (fromField.isNotNull() != ontoField.isNotNull()) {
					actions.add(createColumnAlterAction(fromTable, ontoField, fromField));
				} else if (fromField.isUnique() != ontoField.isUnique()) {
					actions.add(createColumnAlterAction(fromTable, fromField));
				}*/
            }

            // foreign keys
            List<DDLForeignKey> addKeys = new ArrayList<DDLForeignKey>();

            for (DDLForeignKey fromKey : fromTable.getForeignKeys())
            {
                for (DDLForeignKey ontoKey : ontoTable.getForeignKeys())
                {
                    if (!(fromKey.getTable().equalsIgnoreCase(ontoKey.getTable())
                            && fromKey.getForeignField().equalsIgnoreCase(ontoKey.getForeignField()))
                            && fromKey.getField().equalsIgnoreCase(ontoKey.getField())
                            && fromKey.getDomesticTable().equalsIgnoreCase(ontoKey.getDomesticTable()))
                    {
                        addKeys.add(fromKey);
                    }
                }
            }

            for (DDLForeignKey ontoKey : ontoTable.getForeignKeys())
            {
                for (DDLForeignKey fromKey : fromTable.getForeignKeys())
                {
                    if (!(ontoKey.getTable().equalsIgnoreCase(fromKey.getTable())
                            && ontoKey.getForeignField().equalsIgnoreCase(fromKey.getForeignField()))
                            && ontoKey.getField().equalsIgnoreCase(fromKey.getField())
                            && ontoKey.getDomesticTable().equalsIgnoreCase(fromKey.getDomesticTable()))
                    {
                        dropKeys.add(ontoKey);
                    }
                }
            }

            for (DDLForeignKey key : addKeys)
            {
                DDLAction action = new DDLAction(DDLActionType.ALTER_ADD_KEY);
                action.setKey(key);
                actions.add(action);
            }

            // field indexes
//			List<DDLIndex> addIndexes = new ArrayList<DDLIndex>();
//			List<DDLIndex> dropIndexes = new ArrayList<DDLIndex>();
//			
//			for (DDLIndex fromIndex : fromTable.getIndexes()) {
//				boolean found = false;
//				
//				for (DDLIndex ontoIndex : ontoTable.getIndexes()) {
//					if (fromIndex.getTable().equalsIgnoreCase(ontoIndex.getTable()) 
//							&& fromIndex.getField().equalsIgnoreCase(ontoIndex.getField())) {
//						found = true;
//						break;
//					}
//				}
//				
//				if (!found) {
//					addIndexes.add(fromIndex);
//				}
//			}
//			
//			for (DDLIndex ontoIndex : ontoTable.getIndexes()) {
//				boolean found = false;
//				
//				for (DDLIndex fromIndex : fromTable.getIndexes()) {
//					if (ontoIndex.getTable().equalsIgnoreCase(fromIndex.getTable()) 
//							&& ontoIndex.getField().equalsIgnoreCase(fromIndex.getField())) {
//						found = true;
//						break;
//					}
//				}
//				
//				if (!found) {
//					dropIndexes.add(ontoIndex);
//				}
//			}
//			
//			for (DDLIndex index : addIndexes) {
//				DDLAction action = new DDLAction(DDLActionType.CREATE_INDEX);
//				action.setIndex(index);
//				actions.add(action);
//			}
//
//			for (DDLIndex index : dropIndexes) {
//				DDLAction action = new DDLAction(DDLActionType.DROP_INDEX);
//				action.setIndex(index);
//				actions.add(action);
//			}
        }

        for (DDLForeignKey key : dropKeys)
        {
            DDLAction action = new DDLAction(DDLActionType.ALTER_DROP_KEY);
            action.setKey(key);
            actions.add(action);
        }

        return actions.toArray(new DDLAction[actions.size()]);
    }

    private static boolean equals(String s, String s1, boolean caseSensitive)
    {
        return transform(s, caseSensitive).equals(transform(s1, caseSensitive));
    }

    private static String transform(String s, boolean caseSensitive)
    {
        String tableName = s;
        if (!caseSensitive)
        {
            tableName = tableName.toLowerCase();
        }
        return tableName;
    }

    public static DDLAction[] sortTopologically(DDLAction[] actions)
    {
        List<DDLAction> back = new LinkedList<DDLAction>();
        Map<DDLAction, Set<DDLAction>> deps = new HashMap<DDLAction, Set<DDLAction>>();
        List<DDLAction> roots = new LinkedList<DDLAction>();
        Set<DDLAction> covered = new HashSet<DDLAction>();

        performSort(actions, deps, roots);

        while (!roots.isEmpty())
        {
            DDLAction[] rootsArray = roots.toArray(new DDLAction[roots.size()]);
            roots.remove(rootsArray[0]);

            if (covered.contains(rootsArray[0]))
            {
                throw new RuntimeException("Circular dependency detected in or below " + rootsArray[0].getTable().getName());
            }
            covered.add(rootsArray[0]);

            back.add(rootsArray[0]);

            List<DDLAction> toRemove = new LinkedList<DDLAction>();
            Iterator<DDLAction> depIterator = deps.keySet().iterator();
            while (depIterator.hasNext())
            {
                DDLAction depAction = depIterator.next();

                Set<DDLAction> individualDeps = deps.get(depAction);
                individualDeps.remove(rootsArray[0]);

                if (individualDeps.isEmpty())
                {
                    roots.add(depAction);
                    toRemove.add(depAction);
                }
            }

            for (DDLAction action : toRemove)
            {
                deps.remove(action);
            }
        }

        return back.toArray(new DDLAction[back.size()]);
    }

    /*
      * DROP_KEY
      * DROP_INDEX
      * DROP_COLUMN
      * CHANGE_COLUMN
      * DROP
      * CREATE
      * ADD_COLUMN
      * ADD_KEY
      * CREATE_INDEX
      */

    private static void performSort(DDLAction[] actions, Map<DDLAction, Set<DDLAction>> deps, List<DDLAction> roots)
    {
        List<DDLAction> dropKeys = new LinkedList<DDLAction>();
        List<DDLAction> dropIndexes = new LinkedList<DDLAction>();
        List<DDLAction> dropColumns = new LinkedList<DDLAction>();
        List<DDLAction> changeColumns = new LinkedList<DDLAction>();
        List<DDLAction> drops = new LinkedList<DDLAction>();
        List<DDLAction> creates = new LinkedList<DDLAction>();
        List<DDLAction> addColumns = new LinkedList<DDLAction>();
        List<DDLAction> addKeys = new LinkedList<DDLAction>();
        List<DDLAction> createIndexes = new LinkedList<DDLAction>();

        for (DDLAction action : actions)
        {
            switch (action.getActionType())
            {
                case ALTER_DROP_KEY:
                    dropKeys.add(action);
                    break;

                case DROP_INDEX:
                    dropIndexes.add(action);
                    break;

                case ALTER_DROP_COLUMN:
                    dropColumns.add(action);
                    break;

                case ALTER_CHANGE_COLUMN:
                    changeColumns.add(action);
                    break;

                case DROP:
                    drops.add(action);
                    break;

                case CREATE:
                    creates.add(action);
                    break;

                case ALTER_ADD_COLUMN:
                    addColumns.add(action);
                    break;

                case ALTER_ADD_KEY:
                    addKeys.add(action);
                    break;

                case CREATE_INDEX:
                    createIndexes.add(action);
                    break;
            }
        }

        roots.addAll(dropKeys);
        roots.addAll(dropIndexes);

        for (DDLAction action : dropColumns)
        {
            Set<DDLAction> dependencies = new HashSet<DDLAction>();

            for (DDLAction depAction : dropKeys)
            {
                DDLForeignKey key = depAction.getKey();

                if ((key.getTable().equals(action.getTable().getName()) && key.getForeignField().equals(action.getField().getName()))
                        || (key.getDomesticTable().equals(action.getTable().getName()) && key.getField().equals(action.getField().getName())))
                {
                    dependencies.add(depAction);
                }
            }

            if (dependencies.size() == 0)
            {
                roots.add(action);
            }
            else
            {
                deps.put(action, dependencies);
            }
        }

        for (DDLAction action : changeColumns)
        {
            Set<DDLAction> dependencies = new HashSet<DDLAction>();

            for (DDLAction depAction : dropKeys)
            {
                DDLForeignKey key = depAction.getKey();

                if ((key.getTable().equals(action.getTable().getName()) && key.getForeignField().equals(action.getField().getName()))
                        || (key.getDomesticTable().equals(action.getTable().getName()) && key.getField().equals(action.getField().getName())))
                {
                    dependencies.add(depAction);
                }
            }

            for (DDLAction depAction : dropColumns)
            {
                if ((depAction.getTable().equals(action.getTable()) && depAction.getField().equals(action.getField()))
                        || (depAction.getTable().equals(action.getTable()) && depAction.getField().equals(action.getOldField())))
                {
                    dependencies.add(depAction);
                }
            }

            if (dependencies.size() == 0)
            {
                roots.add(action);
            }
            else
            {
                deps.put(action, dependencies);
            }
        }

        for (DDLAction action : drops)
        {
            Set<DDLAction> dependencies = new HashSet<DDLAction>();

            for (DDLAction depAction : dropKeys)
            {
                DDLForeignKey key = depAction.getKey();

                if (key.getTable().equals(action.getTable().getName()) || key.getDomesticTable().equals(action.getTable().getName()))
                {
                    dependencies.add(depAction);
                }
            }

            for (DDLAction depAction : dropColumns)
            {
                if (depAction.getTable().equals(action.getTable()))
                {
                    dependencies.add(depAction);
                }
            }

            for (DDLAction depAction : changeColumns)
            {
                if (depAction.getTable().equals(action.getTable()))
                {
                    dependencies.add(depAction);
                }
            }

            if (dependencies.size() == 0)
            {
                roots.add(action);
            }
            else
            {
                deps.put(action, dependencies);
            }
        }

        for (DDLAction action : creates)
        {
            Set<DDLAction> dependencies = new HashSet<DDLAction>();

            for (DDLForeignKey key : action.getTable().getForeignKeys())
            {
                for (DDLAction depAction : creates)
                {
                    if (depAction != action && depAction.getTable().getName().equals(key.getTable()))
                    {
                        dependencies.add(depAction);
                    }
                }

                for (DDLAction depAction : addColumns)
                {
                    if (depAction.getTable().getName().equals(key.getTable())
                            && depAction.getField().getName().equals(key.getForeignField()))
                    {
                        dependencies.add(depAction);
                    }
                }

                for (DDLAction depAction : changeColumns)
                {
                    if (depAction.getTable().getName().equals(key.getTable())
                            && depAction.getField().getName().equals(key.getForeignField()))
                    {
                        dependencies.add(depAction);
                    }
                }
            }

            if (dependencies.size() == 0)
            {
                roots.add(action);
            }
            else
            {
                deps.put(action, dependencies);
            }
        }

        for (DDLAction action : addColumns)
        {
            Set<DDLAction> dependencies = new HashSet<DDLAction>();

            for (DDLAction depAction : creates)
            {
                if (depAction.getTable().equals(action.getTable()))
                {
                    dependencies.add(depAction);
                }
            }

            if (dependencies.size() == 0)
            {
                roots.add(action);
            }
            else
            {
                deps.put(action, dependencies);
            }
        }

        for (DDLAction action : addKeys)
        {
            Set<DDLAction> dependencies = new HashSet<DDLAction>();
            DDLForeignKey key = action.getKey();

            for (DDLAction depAction : creates)
            {
                if (depAction.getTable().getName().equals(key.getTable())
                        || depAction.getTable().getName().equals(key.getDomesticTable()))
                {
                    dependencies.add(depAction);
                }
            }

            for (DDLAction depAction : addColumns)
            {
                if ((depAction.getTable().getName().equals(key.getTable()) && depAction.getField().getName().equals(key.getForeignField()))
                        || (depAction.getTable().getName().equals(key.getDomesticTable())) && depAction.getField().getName().equals(key.getField()))
                {
                    dependencies.add(depAction);
                }
            }

            for (DDLAction depAction : changeColumns)
            {
                if ((depAction.getTable().getName().equals(key.getTable()) && depAction.getField().getName().equals(key.getForeignField()))
                        || (depAction.getTable().getName().equals(key.getDomesticTable())) && depAction.getField().getName().equals(key.getField()))
                {
                    dependencies.add(depAction);
                }
            }

            if (dependencies.size() == 0)
            {
                roots.add(action);
            }
            else
            {
                deps.put(action, dependencies);
            }
        }

        for (DDLAction action : createIndexes)
        {
            Set<DDLAction> dependencies = new HashSet<DDLAction>();
            DDLIndex index = action.getIndex();

            for (DDLAction depAction : creates)
            {
                if (depAction.getTable().getName().equals(index.getTable()))
                {
                    dependencies.add(depAction);
                }
            }

            for (DDLAction depAction : addColumns)
            {
                if (depAction.getTable().getName().equals(index.getTable()) || depAction.getField().getName().equals(index.getField()))
                {
                    dependencies.add(depAction);
                }
            }

            for (DDLAction depAction : changeColumns)
            {
                if (depAction.getTable().getName().equals(index.getTable()) || depAction.getField().getName().equals(index.getField()))
                {
                    dependencies.add(depAction);
                }
            }

            if (dependencies.size() == 0)
            {
                roots.add(action);
            }
            else
            {
                deps.put(action, dependencies);
            }
        }
    }

    private static DDLAction createColumnAlterAction(DDLTable table, DDLField oldField, DDLField field)
    {
        DDLAction action = new DDLAction(DDLActionType.ALTER_CHANGE_COLUMN);
        action.setTable(table);
        action.setField(field);
        action.setOldField(oldField);
        return action;
    }
}