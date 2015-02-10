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

import com.google.common.base.Function;
import com.google.common.collect.ImmutableList;
import net.java.ao.schema.TableNameConverter;
import net.java.ao.schema.info.EntityInfo;
import net.java.ao.schema.info.FieldInfo;
import net.java.ao.types.TypeInfo;
import net.java.ao.types.TypeManager;

import java.io.Serializable;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.collect.Iterables.transform;
import static com.google.common.collect.Lists.*;
import static com.google.common.collect.Maps.*;

/**
 * A cross-database representation of a SELECT query, independent of database provider type.
 * <p>
 * Use with {@link net.java.ao.DatabaseProvider#renderQuery(Query, net.java.ao.schema.TableNameConverter, boolean)}
 * to produce the database-specific SQL.
 *
 * @author Daniel Spiewak
 */
public class Query implements Serializable
{
    public enum QueryType
    {
        SELECT
    }

    private final static String PRIMARY_KEY_FIELD = "''''primary_key_field''''";
	
	private final QueryType type;
	private String fields;
	
	private boolean distinct = false;
	
	private Class<? extends RawEntity<?>> tableType;
	private String table;
	
	private String whereClause;
	private Object[] whereParams;
	
	private String orderClause;
	private String groupClause;
	private int limit = -1;
	private int offset = -1;
	
	private Map<Class<? extends RawEntity<?>>, String> joins;
    private Map<Class<? extends RawEntity<?>>, String> aliases = newHashMap();

    /**
     * Create a {@link Query} and set the field list in the {@code SELECT} clause.
     *
     * @param fields The fields to select, as comma-delimited field names. Spaces are OK. Must not contain {@code "*"}.
     * @throws java.lang.IllegalArgumentException if fields contains {@code "*"}
     */
    public Query(QueryType type, String fields) {
        validateSelectFields(fields);

		this.type = type;
		this.fields = fields;
		
		joins = new LinkedHashMap<Class<? extends RawEntity<?>>, String>();
	}

    public Iterable<String> getFields()
    {
        if (fields.contains(PRIMARY_KEY_FIELD))
        {
            return Collections.emptyList();
        }
        else
        {
            return ImmutableList.copyOf(transform(newArrayList(fields.split(",")), new Function<String, String>()
            {
                @Override
                public String apply(String field)
                {
                    return field.trim();
                }
            }));
        }
    }

    void setFields(String[] fields) {
		if (fields.length == 0) {
			return;
		}
		
		StringBuilder builder = new StringBuilder();
		for (String field : fields) {
			builder.append(field).append(',');
		}
		if (fields.length > 1) {
			builder.setLength(builder.length() - 1);
		}
		
		this.fields = builder.toString();
	}
	
	<K> void resolvePrimaryKey(FieldInfo<K> fieldInfo) {
		fields = fields.replaceAll(PRIMARY_KEY_FIELD, fieldInfo.getName());
	}
	
	public Query distinct() {
		distinct = true;
		
		return this;
	}
	
	public Query from(Class<? extends RawEntity<?>> tableType) {
		table = null;
		this.tableType = tableType;
		
		return this;
	}
	
	public Query from(String table) {
		tableType = null;
		this.table = table;
		
		return this;
	}
	
	public Query where(String clause, Object... params) {
		whereClause = clause;
		setWhereParams(params);
		
		return this;
	}
	
	public Query order(String clause) {
		orderClause = clause;
		
		return this;
	}
	
	public Query group(String clause) {
		groupClause = clause;
		
		return this;
	}
	
	public Query limit(int limit) {
		this.limit = limit;
		
		return this;
	}
	
	public Query offset(int offset) {
		this.offset = offset;
		
		return this;
	}

    public Query alias(Class<? extends RawEntity<?>> table, String alias)
    {
        if (aliases.containsValue(alias))
        {
            throw new ActiveObjectsException("There is already a table aliased '" + alias + "' for this query!");
        }
        aliases.put(table, alias);
        return this;
    }

    public String getAlias(Class<? extends RawEntity<?>> table)
    {
        return aliases.get(table);
    }

	public Query join(Class<? extends RawEntity<?>> join, String on) {
		joins.put(join, on);
		
		return this;
	}
	
	public Query join(Class<? extends RawEntity<?>> join) {
		joins.put(join, null);
		
		return this;
	}
	
	public boolean isDistinct() {
		return distinct;
	}

	public void setDistinct(boolean distinct) {
		this.distinct = distinct;
	}

	public Class<? extends RawEntity<?>> getTableType() {
		return tableType;
	}

	public void setTableType(Class<? extends RawEntity<?>> tableType) {
		this.tableType = tableType;
	}

	public String getTable() {
		return table;
	}

	public void setTable(String table) {
		this.table = table;
	}

	public String getWhereClause() {
		return whereClause;
	}

	public void setWhereClause(String whereClause) {
		this.whereClause = whereClause;
	}

	public Object[] getWhereParams() {
		return whereParams;
	}

	public void setWhereParams(Object[] whereParams) {
		this.whereParams = whereParams;
		
		if (whereParams != null) {
			for (int i = 0; i < whereParams.length; i++) {
				if (whereParams[i] instanceof RawEntity<?>) {
					whereParams[i] = Common.getPrimaryKeyValue((RawEntity<?>) whereParams[i]);
				}
			}
		}
	}

	public String getOrderClause() {
		return orderClause;
	}

	public void setOrderClause(String orderClause) {
		this.orderClause = orderClause;
	}

	public String getGroupClause() {
		return groupClause;
	}

	public void setGroupClause(String groupClause) {
		this.groupClause = groupClause;
	}

	public int getLimit() {
		return limit;
	}

	public void setLimit(int limit) {
		this.limit = limit;
	}

	public int getOffset() {
		return offset;
	}
	
	public void setOffset(int offset) {
		this.offset = offset;
	}

	public Map<Class<? extends RawEntity<?>>, String> getJoins() {
		return Collections.unmodifiableMap(joins);
	}

	public void setJoins(Map<Class<? extends RawEntity<?>>, String> joins) {
		this.joins = joins;
	}

	public QueryType getType() {
		return type;
	}
	
	public String[] getCanonicalFields(EntityInfo<?, ?> entityInfo) {
        String[] back = fields.split(",");
               
        List<String> result = new ArrayList<String>();
		for(String fieldName : back) {
            if (fieldName.trim().equals("*")) {
                for (FieldInfo<?> fieldInfo : entityInfo.getFields()) {
					result.add(fieldInfo.getName());
				}
			}  else {
                result.add(fieldName.trim());
            }
        }

		return result.toArray(new String[result.size()]);
	}

	protected <K> String toSQL(EntityInfo<? extends RawEntity<K>, K> entityInfo, DatabaseProvider provider, TableNameConverter converter, boolean count) {
		if (this.tableType == null && table == null) {
			this.tableType = entityInfo.getEntityType();
		}
		
		resolvePrimaryKey(entityInfo.getPrimaryKey());
		
		return provider.renderQuery(this, converter, count);
	}

	@SuppressWarnings("unchecked")
	protected void setParameters(EntityManager manager, PreparedStatement stmt) throws SQLException {
		if (whereParams != null) {
			final TypeManager typeManager = manager.getProvider().getTypeManager();
			
			for (int i = 0; i < whereParams.length; i++) {
				if (whereParams[i] == null) {
					manager.getProvider().putNull(stmt, i + 1);
				} else {
					Class javaType = whereParams[i].getClass();
					
					if (whereParams[i] instanceof RawEntity) {
						javaType = ((RawEntity) whereParams[i]).getEntityType();
					}
					
					TypeInfo<Object> typeInfo = typeManager.getType(javaType);
					typeInfo.getLogicalType().putToDatabase(manager, stmt, i + 1, whereParams[i], typeInfo.getJdbcWriteType());
				}
			}
		}
	}

    /**
     * Create a {@link Query} which will select the primary key field of the entity.
     *
     * @return non-null Query
     */
	public static Query select() {
		return select(PRIMARY_KEY_FIELD);
	}

    /**
     * Create a {@link Query} and set the field list in the {@code SELECT} clause.
     *
     * @param fields The fields to select, as comma-delimited field names. Spaces are OK. Must not contain {@code "*"}.
     * @return non-null Query
     * @throws java.lang.IllegalArgumentException if fields contains {@code "*"}
     */
	public static Query select(String fields) {
        return new Query(QueryType.SELECT, fields);
	}

    /*
     * @throws java.lang.IllegalArgumentException if fields contains {@code "*"}
     */
    private static void validateSelectFields(String fields) {
        if (fields == null) {
            return;
        }

        // Assuming we won't have a "legitimate" '*' in a quoted field name
        if (fields.contains("*")) {
            throw new IllegalArgumentException("fields must not contain '*' - got '" + fields + "'");
        }
    }
}
