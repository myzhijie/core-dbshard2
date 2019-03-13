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

import java.io.OutputStream;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import org.springframework.jdbc.core.RowMapper;

import com.bcgdv.dbshard2.dao.entity.IndexedData;
import com.bcgdv.dbshard2.dao.entity.ObjectData;

public interface BaseDao extends Dao {
	ShardResolver getShardResolver();
	TableManager getTableManager();
	ShardedDataSource getShardedDataSource();
	int getDataSourceIdForObjectId(String id);
    int getDataSourceIdForObject(ObjectData od);
	
	int create(ObjectData obj);
	int update(ObjectData obj);
	ObjectData objectLookup(String id);
	List<ObjectData> objectLookup(Collection<String> ids);
	int delete(String id);
	int deleteAll(Collection<String> ids);
	
	List<IndexedData> indexLookup(final String sql, final Map<String, Object> values);
	int indexCountLookup(final String sql, final Map<String, Object> values);
	List<IndexedData> indexLookup(int dataSourceId, final String sql, final Map<String, Object> values);
	int indexCountLookup(int dataSourceId, final String sql, final Map<String, Object> values);
	int deleteIndexData(ClassIndex ind, String id) ;
	<T>List<T> indexQuery(int dataSourceId, String sql,
			Map<String, Object> params, RowMapper<T> rowMapper);
	<T>List<T> indexQuery(String sql,
			Map<String, Object> params, RowMapper<T> rowMapper);
	
	int updateAll(String sql, Object...objects );
	int updateAll(String sql, Map<String, ?> params);

	<T extends ObjectData> List<T> objectLookup(final Class<T> cls);
	<T extends ObjectData> List<T> objectLookup(final Class<T> cls, int offset, int size);

	boolean isType(String id, Class clss);
	void dumpTable(OutputStream output, String tableName, String fields);
}
