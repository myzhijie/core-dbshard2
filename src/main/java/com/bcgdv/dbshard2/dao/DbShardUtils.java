/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.bcgdv.dbshard2.dao;

import java.lang.reflect.Field;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import javax.persistence.Column;
import javax.persistence.Id;

import com.bcgdv.dbshard2.util.reflection.AnnotatedFieldCallback;
import com.bcgdv.dbshard2.util.reflection.ReflectionUtil;

public class DbShardUtils {
	public static List<String> getSqls(final Class<?> beanCls, DbDialet dbdialet) {
		List<String> sqls = new ArrayList<String>();
		ShardedTable tableDefinition = (ShardedTable)beanCls.getAnnotation(ShardedTable.class);
		if(tableDefinition == null)
			return sqls;
		
		final StringBuilder sbb = new StringBuilder( );
		if(DbDialet.Mysql.equals(dbdialet)) {
		    sbb.append("create table if not exists `" + beanCls.getSimpleName() + "` (`id` varchar(64) primary key, `created` bigint, `updated` bigint, `version` integer, `json` text CHARACTER SET utf8mb4 COLLATE utf8mb4_bin");
		}
		else if(DbDialet.H2.equals(dbdialet)) {
            sbb.append("create table if not exists `" + beanCls.getSimpleName() + "` (`id` varchar(64) primary key, `created` bigint, `updated` bigint, `version` integer, `json` text");
		}
		else {
		    throw new RuntimeException("unsupported db dialet");
		}
		
		try {
			ReflectionUtil.iterateAnnotatedFields(beanCls.newInstance(), Column.class, new AnnotatedFieldCallback() {
				@Override
				public void field(Object o, Field field) throws Exception {
					Id anno = field.getAnnotation(Id.class);
					if(anno != null) return;
					sbb.append(",").append(field.getName()).append(" ").append(getColumnType(beanCls, field.getName()));
				}
			});
		} catch (Throwable e) {
			System.err.println("============ " + e.getMessage());
		}
		sbb.append(")");
		sqls.add(sbb.toString());
		
		for(Index index : tableDefinition.indexes()) {
			ClassIndex ti = new ClassIndex();
			ti.forClass = beanCls;
			ti.index = index;
			String[] columns = index.value();
			String tableName = ti.getTableName();
			StringBuilder sb = new StringBuilder();
			StringBuilder indexTable = new StringBuilder();
			sb.append("create table if not exists ").append(tableName).append("(`id` varchar(64)");
			indexTable.append("alter table ").append(tableName).append(" add ").append(index.unique() ? " unique " : "").append(" index `i").append(tableName.substring(1)).append(" (");
			boolean first = true;
			boolean hasCreated = false;
			for(String c : columns) {
				ColumnPath cp = new ColumnPath(c);
				String columnName = cp.getColumnName();
				if("created".equals(columnName))
					hasCreated = true;
				sb.append(",");
				sb.append("`").append(columnName).append("`").append(" ").append(getColumnType(beanCls, c));
				
				if(first) {
					first = false;
				}
				else {
					indexTable.append(",");
				}
				indexTable.append("`").append(columnName).append("`");
			}
			if(!hasCreated)
				sb.append(", `created` bigint)");
			else 
				sb.append(")");
			indexTable.append(")");
			sqls.add(sb.toString());
			sqls.add(indexTable.toString());
			int random = new Random().nextInt();
            sqls.add("alter table " + tableName + " add index `idindex" + random + "` (`id`)");
		}
		
		for(Mapping mapping : tableDefinition.mappings()) {
			ClassMapping cm = new ClassMapping(beanCls, mapping);
			StringBuilder table = new StringBuilder();
			table.append("create table if not exists ").append(cm.getTableName()).append("(`pid` varchar(64), `sid` varchar(64), `created` bigint");
			for(String c : mapping.otherColumns()) {
				ColumnPath cp = new ColumnPath(c);
				String columnName = cp.getColumnName();
				if("created".equals(columnName))
					continue;
				table.append(",");
				table.append("`").append(columnName).append("`").append(" ").append(getColumnType(beanCls, c));
			}
			table.append(")");
			sqls.add(table.toString());
			int random = new Random().nextInt();
            sqls.add("alter table " + cm.getTableName() + " add index `sidindex" + random + "` (`sid`)");
            sqls.add("alter table " + cm.getTableName() + " add index `pidindex" + random + "` (`pid`)");
		}
		
		return sqls;
	}

	private static String getColumnType(Class<?> beanCls, String columnName) {
		try {
			int pos = columnName.indexOf(".");
			if(pos != -1) {
				String fieldName = columnName.substring(0, pos);
				Field field = ReflectionUtil.getField(beanCls, fieldName);
				Class<?> fieldType = field.getType();
				if(fieldType.isAssignableFrom(List.class)) {
					Class<?> listItemType = ReflectionUtil.getFieldGenericType(field);
					return getColumnType(listItemType, columnName.substring(pos+1));
				}
				else {
					return getColumnType(fieldType, columnName.substring(pos+1));
				}
			}
			
			Field field = ReflectionUtil.getField(beanCls, columnName);
			Class<?> type = field.getType();
			if(type.isAssignableFrom(List.class)) {
				// handle List<String> use case
				type = ReflectionUtil.getFieldGenericType(field);
			}
			
	        if (type.equals(String.class)) {
				try {
					Column colAnno = (Column)field.getAnnotation(Column.class);
					if(colAnno != null) {
						String definition = colAnno.columnDefinition();
						if(definition != null && definition.length() > 0)
							return definition;
					    int length = colAnno.length();
					    if(length > 0)
							return "varchar(" + length + ")";
					}
				} catch (Throwable e) {
					System.err.println(e.getMessage());
				}
				return "varchar(64)";
	        }
	        
	        if (type.equals(Integer.class) || type.equals(int.class)) {
	            return "integer";
	        }
	        if (type.equals(Float.class) || type.equals(float.class)) {
	            return "float";
	        }
	        if (type.equals(Double.class) || type.equals(double.class)) {
	            return "float";
	        }
	        if (type.equals(Long.class) || type.equals(long.class)) {
	            return "bigint";
	        }
	        if (type.equals(Boolean.class) || type.equals(boolean.class)) {
	            return "integer";
	        }
	        if (type.equals(Date.class)) {
	            return "bigint";
	        }
	        if (type.equals(Calendar.class)) {
	            return "bigint";
	        }
	        if (type.equals(BigInteger.class)) {
	            return "bigint";
	        }
	        if (type.equals(BigDecimal.class)) {
	            return "bigint";
	        }
	        if(type.isEnum()) {
	            return "varchar(64)";
	        }
			
			return null;
		} catch (Exception e) {
			throw new RuntimeException(beanCls + ":" + columnName, e);
		}
	}
	
	public static Map getMap(Object... keyValues) {
		Map map = new HashMap();
		if(keyValues == null)
			return map;
		for(int i=0; i<keyValues.length; i+=2) {
			map.put(keyValues[i], keyValues[i+1]);
		}
		return map;
	}
}
