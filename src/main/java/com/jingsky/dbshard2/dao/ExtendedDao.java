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

import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.jingsky.dbshard2.dao.entity.MappedData;
import com.jingsky.dbshard2.dao.entity.ObjectData;
import com.jingsky.dbshard2.dao.impl.BeanHandler;
import org.springframework.jdbc.core.RowMapper;

public interface ExtendedDao extends BaseDao {
	List<Class> listManagedClasses();
	void createBean(ObjectData obj);
	void createBeans(List<? extends ObjectData> objs);
    void updateBean(ObjectData obj);
    void updateBeanOnly(ObjectData obj);
	int deleteBean(ObjectData od);
	<Z> Z getBean(String id);
	int delete(String id);
	<T>List<T> listBeans(Collection<String> ids);
	<T>Map<String, T> mapBeans(Collection<String> ids);
	void removeBeans(List<? extends ObjectData> list);
	<T> List<T> query(final String sql, final RowMapper<T> mapper);
	<T> List<T> queryBeans(final String sql, Class<T> cls);

	List<ObjectData> indexLookup(Class cls, String field, Object value);
	List<ObjectData> indexLookup(Class cls, Map<String, Object> keyValues);
	List<ObjectData> indexLookup(Class cls, String field, Object value, int dataSourceId);
	List<ObjectData> indexLookup(Class cls, Map<String, Object> keyValues, int dataSourceId);
	int indexCountLookup(Class cls, String field, Object value, int dataSourceId);
	int indexCountLookup(Class cls, Map<String, Object> keyValues, int dataSourceId);
	int indexCountLookup(Class cls, String field, Object value);
	int indexCountLookup(Class cls, Map<String, Object> keyValues);
	<Z>List<Z> indexBeanLookup(Class<Z> cls, String field, Object value);
	<Z>List<Z> indexBeanLookup(Class<Z> cls, Map<String, Object> keyValues);
	List<ObjectData> indexLookup(Class cls, String field, String id, int offset, int size);
	<Z>List<Z> indexBeanLookup(Class<Z> cls, String field, String id, int offset, int size);
	<Z>List<Z> indexBeanLookup(Class<Z> cls, Map<String, Object> keyValues, int offset, int size, int dataSourceId);
	<Z>List<Z> indexBeanLookup(Class<Z> cls, Map<String, Object> keyValues, int dataSourceId);
	<Z>List<Z> indexBeanLikeLookup(Class<Z> cls, String field, String id, int offset, int size);
	<Z> List<Z> indexBeanLookup(String sql, Map<String, Object> keyValues, int dataSourceId);
	<Z> List<Z> indexBeanLookup(String sql, Map<String, Object> keyValues);
	<Z> List<Z> indexBeanLookup(String sql);
	
	String generateIdForBean(ObjectData bean);
	String generateSameShardId(String id, Class forClass);
	String sameIdExceptType(String id, Class forClass);
	List<String> sameIdsExceptType(List<String> ids, Class forClass);
	
	List<MappedData> mappedLookup(Class pclass, Class sclass, String pid);
	List<String> mappedIdLookup(Class pclass, Class sclass, String pid);
	List<MappedData> mappedLookup(Class pclass, Class sclass, String pid, int offset, int size);
	List<String> mappedIdLookup(Class pclass, Class sclass, String pid, int offset, int size);
	int mappedCountLookup(Class pclass, Class sclass, String pid);

	<T extends ObjectData> List<T> beanLookup(final Class<T> cls, int offset, int size);

	<T> void forEachBean(Class<T> cls, BeanHandler<T> handler);
	void forEachRawBean(Class cls, BeanHandler<ObjectData> handler);
	void touchAllBean(Class cls);
	ClassIndex indexByKeys(Class forClass, Collection<String> keys);
	ClassIndex indexByKeys(Class forClass, String... keys);
	
	int count(Class cls);

	List<MappedData> mappedLookup(Class pclass, Class sclass, String sql, Map<String, Object> params);
	List<String> mappedIdLookup(Class pclass, Class sclass, String sql, Map<String, Object> params);
	int mappedCountLookup(Class pclass, Class sclass, String sql, Map<String, Object> params);
}
