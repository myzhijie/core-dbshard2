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

public class ClassMapping {
	public Mapping mapping;
	public Class forClass;
	
	private String tableName;
	
	public ClassMapping() {
	}
	
	public ClassMapping(Class forClass, Mapping mapping) {
		this.forClass = forClass;
		this.mapping = mapping;
	}
	
	public String getTableName() {
		if(tableName != null)
			return tableName;
		
		StringBuilder sb = new StringBuilder();
		sb.append("`m_");
		sb.append(mapping.map2cls().getSimpleName());
		sb.append("_");
		sb.append(forClass.getSimpleName());
		sb.append("`");
		tableName = sb.toString();
		
		return tableName;
	}
}
