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

package com.jingsky.dbshard2.dao.impl;

import java.sql.ResultSet;
import java.sql.SQLException;

import com.jingsky.dbshard2.dao.entity.IndexedData;
import org.springframework.jdbc.core.RowMapper;

public class IndexedDataRowMapper implements RowMapper<IndexedData>{
	
	@Override
	public IndexedData mapRow(ResultSet arg0, int arg1)
			throws SQLException {
		IndexedData row = new IndexedData();
		row.setId(arg0.getString("id"));
		for(int i=0; i<arg0.getMetaData().getColumnCount(); i++) {
			String columnName = arg0.getMetaData().getColumnName(i+1);
			row.put(columnName, arg0.getObject(i+1));
		}
		return row;
	}
}
