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

package com.jingsky.dbshard2.dao;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;

public class ClassIndex {
	public Index index;
	public Class forClass;
	private String tableName;
	
	public ClassIndex() {
	}
	
	public ClassIndex(Class forClass, Index index) {
		this.forClass = forClass;
		this.index = index;
	}
	
	public String getTableName() {
		if(tableName != null)
			return tableName;
		
		StringBuilder sb = new StringBuilder();
		sb.append("i_");
		sb.append(forClass.getSimpleName());
		for(String column : index.value()) {
			sb.append("_");
			String columnName = new ColumnPath(column).getColumnName();
			sb.append(columnName);
		}
		tableName = "`"+(sb.length()<=64?sb.toString():sb.substring(0,64))+"`";
		return tableName;
	}
	
	public final String getIndexedLookupKey(Map<String, Object>values) {
		StringBuilder sb = new StringBuilder();
		sb.append(getTableName());
		for(String s : index.value()) {
			Object object = values.get(s);
			if(object != null && object instanceof Collection) {
				List list = new ArrayList((Collection) object);
				Collections.sort(list, new Comparator<Object>() {
					@Override
					public int compare(Object o1, Object o2) {
						if(o1==null && o2 != null)
							return 1;
						else if(o1!=null && o2 == null)
							return -1;
						else if(o1==null && o2 == null)
							return 0;
						else
							return o1.toString().compareTo(o2.toString());
					}
				});
				for(Object o : list) {
					sb.append("|"+ o);
				}
			}
			else {
				sb.append("|"+object);
			}
		}
		return sb.toString();
	}
}
