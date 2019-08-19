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

import java.lang.reflect.Field;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

import com.jingsky.dbshard2.dao.entity.IndexedData;
import com.jingsky.dbshard2.dao.entity.MappedData;
import com.jingsky.dbshard2.dao.entity.ObjectData;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import com.jingsky.dbshard2.dao.ClassIndex;
import com.jingsky.dbshard2.dao.ClassMapping;
import com.jingsky.dbshard2.dao.ClassTable;
import com.jingsky.dbshard2.dao.ColumnPath;
import com.jingsky.dbshard2.dao.ColumnValues;
import com.jingsky.dbshard2.dao.DaoManager;
import com.jingsky.dbshard2.dao.DbDialet;
import com.jingsky.dbshard2.dao.DbShardUtils;
import com.jingsky.dbshard2.dao.ExtendedDao;
import com.jingsky.dbshard2.dao.ExtendedDataSource;
import com.jingsky.dbshard2.dao.Index;
import com.jingsky.dbshard2.dao.Mapping;
import com.jingsky.dbshard2.dao.ObjectId;
import com.jingsky.dbshard2.dao.ShardedTable;
import com.jingsky.dbshard2.util.reflection.ReflectionUtil;
import com.jingsky.dbshard2.util.DateUtil;
import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.gson.Gson;

public class ExtendedDaoImpl extends BaseDaoImpl implements ExtendedDao {
	private static Logger logger = Logger.getLogger(ExtendedDaoImpl.class);
	
	private static ObjectMapper objectMapper;
	static {
		objectMapper = new ObjectMapper();
		objectMapper.setSerializationInclusion(Include.NON_NULL);
		objectMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
		objectMapper.configure(DeserializationFeature.ACCEPT_EMPTY_STRING_AS_NULL_OBJECT, true);
	}
	
	private static Gson gson = new Gson();
	
	private List<Class> forClasses;
	
	public ExtendedDaoImpl(Class... forClass){
		forClasses = new ArrayList<Class>();
		for(Class cls : forClass)
			addClass(cls);
	}
	
	public void addClass(Class forcls) {
		forClasses.add(forcls);
		DaoManager.getInstance().add(forcls, this);
	}
	
	@Override
	public String generateIdForBean(ObjectData obj) {
		int shardId = getShardResolver().getShardId(obj);
		ObjectId oi = new ObjectId(obj.getClass(), shardId);
		String id = oi.toString();
		return id;
	}	
	
	@Override
	public String generateSameShardId(String id, Class forClass) {
		ObjectId oi = new ObjectId(id);
		oi.setUuid(UUID.randomUUID().toString());
		oi.setType(forClass);
		return oi.toString();
	}
	
	@Override
	public String sameIdExceptType(String id, Class forClass) {
		ObjectId oi = new ObjectId(id);
		oi.setType(forClass);
		return oi.toString();
	}

	@Override
	public List<String> sameIdsExceptType(List<String> ids, Class forClass) {
		List<String> result = new ArrayList<String>();
		for(String id : ids) {
			ObjectId oi = new ObjectId(id);
			oi.setType(forClass);
			result.add(oi.toString());
		}
		return result;
	}
	
	@Override
	public void createBeans(List<? extends ObjectData> objs) {
		for(ObjectData od : objs) {
			createBean(od);
		}
	}

	@Override
	public void createBean(ObjectData obj) {
		if(obj.getClass().equals(ObjectData.class))
			throw new RuntimeException("use BaseDao.create method");
		
		if(!forClasses.contains(obj.getClass()))
			throw new RuntimeException("cannot use " + this.getClass().getSimpleName() + " to create " + obj.getClass());

		obj.json = null;
		if(obj.created == 0)
			obj.created = obj.updated = DateUtil.currentTimeMillis();
		else if (obj.updated == 0)
			obj.updated = DateUtil.currentTimeMillis();
		
		ObjectData od = new ObjectData();
		od.created = obj.created;
		od.updated = obj.updated;
		
		String id = obj.id;
		if(id == null) {
			id = generateIdForBean(obj);
			obj.id = id;
		}
		od.id = id;
		od.version = obj.version;
		
		try {
			String json = objectMapper.writeValueAsString(obj);
			od.json = json ;
		} catch (JsonProcessingException e) {
			throw new RuntimeException("json error", e);
		}
		super.create(od);
		addIndexesForBean(obj);
		addMappingsForBean(obj);
	}
	
	@Override
	public void touchAllBean(Class cls) {
		forEachBean(cls, new BeanHandler() {
			@Override
			public void processBean(Object bean) {
				updateBean((ObjectData) bean);
			}
		});
	}

    @Override
    public void updateBean(ObjectData obj) {
        ObjectData od = new ObjectData();
        String id = obj.id;
        
        obj.json = null;
        if(obj.created == 0)
            obj.created = obj.updated = DateUtil.currentTimeMillis();
        else
            obj.updated = DateUtil.currentTimeMillis();
        
        ObjectId oi = new ObjectId(id);
        Class<?> cls = tableManager.getCls(oi.getType());
        if(!forClasses.contains(cls))
            throw new RuntimeException("cannot use " + this.getClass().getSimpleName() + " update " + cls.getSimpleName());
        
        ObjectData indb = getBean(id);
        removeIndexesForBean(indb);
        removeMappingsForBean(indb);
        
        od.id = id;
        try {
            String json = objectMapper.writeValueAsString(obj);
            od.json = json ;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("json error", e);
        }
        super.update(od);
        addIndexesForBean(obj);
        addMappingsForBean(obj);
    }

    @Override
    public void updateBeanOnly(ObjectData obj) {
        ObjectData od = new ObjectData();
        String id = obj.id;
        
        obj.json = null;
        if(obj.created == 0)
            obj.created = obj.updated = DateUtil.currentTimeMillis();
        else
            obj.updated = DateUtil.currentTimeMillis();
        
        ObjectId oi = new ObjectId(id);
        Class<?> cls = tableManager.getCls(oi.getType());
        if(!forClasses.contains(cls))
            throw new RuntimeException("cannot use " + this.getClass().getSimpleName() + " update " + cls.getSimpleName());
        
        od.id = id;
        try {
            String json = objectMapper.writeValueAsString(obj);
            od.json = json ;
        } catch (JsonProcessingException e) {
            throw new RuntimeException("json error", e);
        }
        super.update(od);
    }
	
	protected int addIndexesForBean(ObjectData obj){
		int ret = 0;
		ShardedTable annotation = obj.getClass().getAnnotation(ShardedTable.class);
		for(Index index : annotation.indexes()) {
			ret += addIndexForBean(index, obj);
		}
		return ret;
	}
	
	protected int addMappingsForBean(ObjectData obj){
		int ret = 0;
		ShardedTable annotation = obj.getClass().getAnnotation(ShardedTable.class);
		for(Mapping mapping : annotation.mappings()) {
			Class primaryCls = mapping.map2cls();
			ExtendedDao dao = getDaoForClass(primaryCls);
			
			ClassMapping cm = new ClassMapping(obj.getClass(), mapping);
			String table = cm.getTableName();
			StringBuilder sql = new StringBuilder().append("insert into ").append(table).append(" (`pid`, `sid`, `created`");
			for(String s : mapping.otherColumns()) {
				String columnName = new ColumnPath(s).getColumnName();
				sql.append(",").append("`").append(columnName).append("`");
			}
			sql.append(")").append(" values (?, ?, ?");
			for(String s : mapping.otherColumns()) {
				sql.append(", ?");
			}
			sql.append(")");

			Object[] values = new Object[mapping.otherColumns().length + 3];
			try {
				values[0] = ReflectionUtil.getFieldValue(obj, mapping.column());
				if(values[0] == null)
					continue;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
			values[1] = obj.id;
			values[2] = obj.created;
			
			for(int i = 0; i< mapping.otherColumns().length; i++) {
				try {
					values[i+3] = ReflectionUtil.getFieldValue(obj, mapping.otherColumns()[i]);
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}

			ExtendedDataSource dataSource = dao.getShardedDataSource().getDataSourceByObjectId(threadContext.get(), (String)values[0]);
			JdbcTemplate jt = getJdbcTemplate(dataSource);
			ret += jt.update(sql.toString(), values);
		}
		return ret;
	}

	public static ExtendedDao getDaoForClass(Class cls) {
		return DaoManager.getInstance().get(cls);
	}
	
	protected int addIndexForBean(Index index, ObjectData obj){
		ClassIndex ti = new ClassIndex(obj.getClass(), index);
		List<ColumnValues> columnValues = new ArrayList<ColumnValues>();
		for(String indexColumnName : index.value()) {
			try {
				List<Object> list = new ArrayList();
				getValues(obj, indexColumnName, list);
				String columnName = new ColumnPath(indexColumnName).getColumnName();
				columnValues.add(new ColumnValues(columnName, list));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		
		int ret = 0;
		List<Map> values = valueCombination(columnValues);
		for(Map m : values) {
			m.put("id", obj.id);
			m.put("created", obj.created);
			ret += super.createIndex(ti, m);
		}
		
		return ret;
	}
	
	public static List<Map> valueCombination(List<ColumnValues> columnValues) {
		if(columnValues.size()==1) {
			List<Map> list = new ArrayList<Map>();
			ColumnValues cv = columnValues.get(0);
			for(Object v : cv.values) {
				Map m = new HashMap();
				m.put(cv.name, v);
				list.add(m);
			}
			return list;
		}
		
		List<Map> ret = new ArrayList<Map>();
		ColumnValues cv = columnValues.get(0);
		for(Object v : cv.values) {
			List<Map> list = valueCombination(columnValues.subList(1, columnValues.size()));
			for(Map m : list) {
				m.put(cv.name, v);
			}
			ret.addAll(list);
		}
		return ret;
	}
	
	private static void getValues(Object obj, String indexColumnName, List<Object>values) {
		try {
			int pos = indexColumnName.indexOf('.');
			if(pos == -1) {
				Object value = ReflectionUtil.getFieldValue(obj, indexColumnName);
				if(value instanceof List) {
					for(Object o : (List)value) {
		                if(!values.contains(value))
		                    values.add(o);
					}
				}
				else {
                    if(!values.contains(value))
                        values.add(value);
				}
				return;
			}
			
			String fieldName = indexColumnName.substring(0, pos);
			Field field = ReflectionUtil.getField(obj.getClass(), fieldName);
			Object value = ReflectionUtil.getFieldValue(obj, fieldName);
			if(List.class.isAssignableFrom(field.getType())) {
				if(value != null) {
					List list = (List)value;
					for(Object item : list) {
						if(item != null) {
							getValues(item, indexColumnName.substring(pos+1), values);
						}
					}
				}
			}
			else {
				if(value != null) {
					getValues(value, indexColumnName.substring(pos+1), values);
				}
			}
		} catch (Exception e) {
		}
	}
	
	public static ShardedTable getTableForBean(ObjectData obj) {
		return obj.getClass().getAnnotation(ShardedTable.class);
	}
	
	protected int removeIndexesForBean(ObjectData obj){
		if(obj == null) return 0;
		int ret = 0;
		for(Index index : getTableForBean(obj).indexes()) {
			ClassIndex ti = new ClassIndex();
			ti.index = index;
			ti.forClass = obj.getClass();
			deleteIndexData(ti, obj.id);
		}
		return ret;
	}
	
	@Override
	public int delete(String id) {
		ObjectData od = getBean(id);
		removeIndexesForBean(od);
		removeMappingsForBean(od);
		return super.delete(id);
	}
	
	@Override
	public int deleteBean(ObjectData od) {
		if(od.getClass().equals(ObjectData.class))
			throw new RuntimeException(od.getClass() + " is not a subclass of ObjectData");
		removeIndexesForBean(od);
		removeMappingsForBean(od);
		return super.delete(od.id);
	}
	
	@Override
	public int deleteAll(Collection<String> ids) {
		int total = 0;
		for(String id : ids) {
			total += delete(id);
		}
		return total;
	}
	
	protected int removeMappingsForBean(ObjectData obj){
		if(obj == null) return 0;
		int ret = 0;
		for(Mapping mapping : getTableForBean(obj).mappings()) {
			ClassMapping cm = new ClassMapping(obj.getClass(), mapping);
			String table = cm.getTableName();
			List<Object> list = new ArrayList();
			getValues(obj, mapping.column(), list);
			for(Object sid : list) {
				if(sid == null) continue;
				ExtendedDao dao = getDaoForClass(mapping.map2cls());
				ExtendedDataSource dataSource = dao.getShardedDataSource().getDataSourceByObjectId(threadContext.get(), (String)sid);
				JdbcTemplate jt = getJdbcTemplate(dataSource);
				String sql = "delete from " + table + " where `sid`='" + obj.id + "'";
				ret += jt.update(sql);
			}
		}
		return ret;
	}
	
	@Override
	public <Z>Z getBean(String id) {
	    long t0 = DateUtil.currentTimeMillis();
		Z value = null;
		ObjectData data = super.objectLookup(id);
		if(data != null){
			Class cls = getTableManager().getObjectTypeFromId(id);
			try {
			    value = (Z) gson.fromJson(data.json, cls);
//				value = (Z) objectMapper.readValue(data.json, cls);
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
        long t1 = DateUtil.currentTimeMillis() - t0;
        if(t1>1000) {
            System.out.println(threadContext.get() + " getBean " + id + " costs " + t1);
        }
		return value;
	}

	@Override
	public <T>List<T> listBeans(Collection<String> ids) {
		List result = new ArrayList();
		List<ObjectData> list = super.objectLookup(ids);
		HashMap<String, T> map = new HashMap<String, T>();
		if(list.size() > 0){
			Class cls = getTableManager().getObjectTypeFromId(list.get(0).id);
			for (ObjectData data : list) {
				try {
					map.put(data.id, (T)objectMapper.readValue(data.json, cls));
				} catch (Exception e) {
					throw new RuntimeException(e);
				}
			}
			for(ObjectData od : list) {
				result.add(map.get(od.id));
			}
		}
		return result;
	}
	
	@Override
	public void removeBeans(List<? extends ObjectData> list) {
		for(ObjectData od : list) {
		    if(od == null)
		        continue;
			if(od.getClass().equals(ObjectData.class))
				throw new RuntimeException(od.getClass() + " is not a subclass of ObjectData.");
			removeIndexesForBean(od);
			removeMappingsForBean(od);
			super.delete(od.id);
		}
	}

	@Override
	public List<ObjectData> indexLookup(Class cls, String field, Object value, int dataSourceId){
		ClassIndex index = indexByKeys(cls, Arrays.asList(field));
		List<String> ids = new ArrayList<>();
		for(IndexedData id: super.indexedLookup(dataSourceId, index,Collections.singletonMap(field, value))){
			ids.add(id.getId());
		}
		return objectLookup(ids);
	}

	@Override
	public List<ObjectData> indexLookup(Class cls, String field, Object value){
		ClassIndex index = indexByKeys(cls, Arrays.asList(field));
		List<String> ids = new ArrayList<>();
		for(IndexedData id: super.indexedLookup(null, index,Collections.singletonMap(field, value))){
			ids.add(id.getId());
		}
		return objectLookup(ids);
	}

	@Override
	public int indexCountLookup(Class cls, String field, Object value, int dataSourceId){
		ClassIndex index = indexByKeys(cls, Arrays.asList(field));
		return super.indexedCountLookup(index,Collections.singletonMap(field, value), dataSourceId);
	}

	@Override
	public List<ObjectData> indexLookup(Class cls,
			Map<String, Object> keyValues) {
		ClassIndex index = indexByKeys(cls, keyValues.keySet());
		List<String> ids = new ArrayList<>();
		if(index != null){
			for(IndexedData id: super.indexedLookup(null, index,keyValues)){
				ids.add(id.getId());
			}
		}
		return objectLookup(ids);
	}

	@Override
	public List<ObjectData> indexLookup(Class cls,
			Map<String, Object> keyValues, int dataSourceId) {
		ClassIndex index = indexByKeys(cls, keyValues.keySet());
		List<String> ids = new ArrayList<>();
		if(index != null){
			for(IndexedData id: super.indexedLookup(dataSourceId, index,keyValues)){
				ids.add(id.getId());
			}
		}
		return objectLookup(ids);
	}

	@Override
	public int indexCountLookup(Class cls,
			Map<String, Object> keyValues, int dataSourceId) {
		ClassIndex index = indexByKeys(cls, keyValues.keySet());
		return super.indexedCountLookup(index,keyValues, dataSourceId);
	}
	
	@Override
	public int indexCountLookup(Class cls, String field, Object value) {
		return indexCountLookup(cls, Collections.singletonMap(field, value));
	}
	
	@Override
	public int indexCountLookup(Class cls, Map<String, Object> keyValues) {
		ClassIndex index = indexByKeys(cls, keyValues.keySet());
		return indexedCountLookup(index, keyValues);
	}

	@Override
	public List<ObjectData> indexLookup(Class cls, String field, String indexedId, int offset, int size){
		ClassIndex index = indexByKeys(cls, Arrays.asList(field));
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByObjectId(threadContext.get(), indexedId);
		NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
		String sql = "select * from " + index.getTableName() + " where `" + field + "` = :field limit " + offset + ", " + size;
		List<IndexedData> data = namedjc.query(sql.toString(), Collections.singletonMap("field", indexedId), new IndexedDataRowMapper());
		List<String> ids = new ArrayList<>();
		for(IndexedData id: data){
			ids.add(id.getId());
		}
		return objectLookup(ids);
	}
	
	@Override
	public ClassIndex indexByKeys(Class forClass, String... keys) {
		List<String> list = new ArrayList<String>();
		for(String s : keys) {
			list.add(s);
		}
		return indexByKeys(forClass, list);
	}
	
	@Override
	public ClassIndex indexByKeys(Class forClass, Collection<String>keys) {
		ClassIndex ti = new ClassIndex();
		ti.forClass = forClass;
		ShardedTable annotation = (ShardedTable) forClass.getAnnotation(ShardedTable.class);
		int maxMatched = 0;
		for(Index i : annotation.indexes()) {
			int matched = 0;
			for(String s : i.value()) {
				if(keys.contains(s)) {
					matched++;
				}
			}
			if(matched>maxMatched) {
				ti.index = i;
				maxMatched = matched;
			}
		}
		if(maxMatched == 0)
			throw new RuntimeException("index doesn't exist for " + keys);
		return ti;
	}

	@Override
	public void createTable(DbDialet dialet) {
		for(Class cls : forClasses) {
			List<String> sqls = DbShardUtils.getSqls(cls, dialet);
			for(String sql : sqls) {
				try {
					updateAll(sql);
				} catch (Exception e) {
                                        if(logger.isDebugEnabled())logger.info("========= error create table " + sql, e);
                                        else logger.info("========= error create table " + sql);
				}
			}
		}
	}

	@Override
	public List<MappedData> mappedLookup(Class pclass, Class sclass, String pid) {
		ClassTable ct = new ClassTable(sclass);
		ClassMapping cm = ct.getClassMapping(pclass);
		final ExtendedDataSource dataSource = getShardedDataSource().getDataSourceByObjectId(threadContext.get(), pid);
		final String sql = "select * from " + cm.getTableName() + " where `pid` = :pid";
		NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
		List<MappedData> data = namedjc.query(sql, Collections.singletonMap("pid", pid), new MappedDataRowMapper());
		return data;
	}

	@Override
	public List<MappedData> mappedLookup(Class pclass, Class sclass, String sql, Map<String, Object> params) {
		if (!params.containsKey("pid")) {
			throw new RuntimeException("pid doesn't transmit");
		}

		ClassTable ct = new ClassTable(sclass);
		ClassMapping cm = ct.getClassMapping(pclass);
		sql = sql.replace("__mappedTable__", cm.getTableName());

		final ExtendedDataSource dataSource = getShardedDataSource().getDataSourceByObjectId(threadContext.get(), params.get("pid").toString());
		NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
		List<MappedData> data = namedjc.query(sql, params, new MappedDataRowMapper());

		return data;
	}
	
	@Override
	public int mappedCountLookup(Class pclass, Class sclass, String pid) {
		ClassTable ct = new ClassTable(sclass);
		ClassMapping cm = ct.getClassMapping(pclass);
		final ExtendedDataSource dataSource = getShardedDataSource().getDataSourceByObjectId(threadContext.get(), pid);
		final String sql = "select count(*) from " + cm.getTableName() + " where `pid` = :pid";
		NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
		int cnt = namedjc.queryForObject(sql, Collections.singletonMap("pid", pid), Integer.class);
		return cnt;
	}

	@Override
	public int mappedCountLookup(Class pclass, Class sclass, String sql, Map<String, Object> params) {
		if (!params.containsKey("pid")) {
			throw new RuntimeException("pid doesn't transmit");
		}

		ClassTable ct = new ClassTable(sclass);
		ClassMapping cm = ct.getClassMapping(pclass);
		sql = sql.replace("__mappedTable__", cm.getTableName());

		final ExtendedDataSource dataSource = getShardedDataSource().getDataSourceByObjectId(threadContext.get(), params.get("pid").toString());
		NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
		int cnt = namedjc.queryForObject(sql, params, Integer.class);

		return cnt;
	}
	
	@Override
	public List<MappedData> mappedLookup(final Class pclass, final Class sclass, final String pid, final int offset, final int size) {
		final ClassTable ct = new ClassTable(sclass);
		final ClassMapping cm = ct.getClassMapping(pclass);
		if(pid != null) {
			final ExtendedDataSource dataSource = getShardedDataSource().getDataSourceByObjectId(threadContext.get(), pid);
			final String sql = "select * from " + cm.getTableName() + " where `pid` = :pid order by `created` desc limit " + offset + "," + size;
			NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
			List<MappedData> data = namedjc.query(sql, Collections.singletonMap("pid", pid), new MappedDataRowMapper());
			return data;
		}
		else {
			final List<MappedData> list = new ArrayList<MappedData>();
			forEachDataSource(new RequestAwareShardRunnable(threadContext.get()) {
				@Override
				public void run(int dataSourceId) {
					ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(getTc(), dataSourceId);
					final String sql = "select * from " + cm.getTableName() + " order by `created` desc limit " + offset + "," + size;
					NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
					List<MappedData> data = namedjc.query(sql, new MappedDataRowMapper());
					synchronized (list) {
						list.addAll(data);
					}
				}
			});
			return list;
		}
	}

	@Override
	public List<String> mappedIdLookup(Class pclass, Class sclass, String pid) {
		List<MappedData> mdlist = mappedLookup(pclass, sclass, pid);
		List<String> ids = new ArrayList<String>();
		for(MappedData md : mdlist) {
			ids.add(md.getSid());
		}
		return ids;
	}

	@Override
	public List<String> mappedIdLookup(Class pclass, Class sclass, String sql, Map<String, Object> params) {
		List<MappedData> mdlist = mappedLookup(pclass, sclass, sql, params);
		List<String> ids = new ArrayList<String>();
		for(MappedData md : mdlist) {
			ids.add(md.getSid());
		}
		return ids;
	}

	@Override
	public List<String> mappedIdLookup(Class pclass, Class sclass, String pid, int offset, int size) {
		List<MappedData> mdlist = mappedLookup(pclass, sclass, pid, offset, size);
		List<String> ids = new ArrayList<String>();
		for(MappedData md : mdlist) {
			ids.add(md.getSid());
		}
		return ids;
	}

	@Override
	public <Z> List<Z> indexBeanLookup(Class<Z> cls, String field, Object value) {
		ClassIndex index = indexByKeys(cls, Arrays.asList(field));
		if(index == null)
			throw new RuntimeException("index doesn't exist for " + cls + "." + field);
		List<String> ids = new ArrayList<>();
		for(IndexedData id: super.indexedLookup(null, index,Collections.singletonMap(field, value))){
			ids.add(id.getId());
		}
		return (List<Z>) listBeans(ids);
	}
	
	@Override
	public <Z> List<Z> indexBeanLookup(Class<Z> cls, String field, String indexedId, int offset, int size) {
		ClassIndex index = indexByKeys(cls, Arrays.asList(field));
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByObjectId(threadContext.get(), indexedId);
		NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
		String sql = "select * from " + index.getTableName() + " where `" + new ColumnPath(field).getColumnName() + "` = :field order by `created` desc limit " + offset + ", " + size;
		List<IndexedData> data = namedjc.query(sql.toString(), Collections.singletonMap("field", indexedId), new IndexedDataRowMapper());
		List<String> ids = new ArrayList<>();
		for(IndexedData id: data){
			ids.add(id.getId());
		}
		return listBeans(ids);
	}
	
	@Override
	public <Z> List<Z> indexBeanLikeLookup(Class<Z> cls, String field, String indexedId, int offset, int size) {
		ClassIndex index = indexByKeys(cls, Arrays.asList(field));
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByObjectId(threadContext.get(), indexedId);
		NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
		String sql = "select * from " + index.getTableName() + " where `" + new ColumnPath(field).getColumnName() + "` like :field order by `created` desc limit " + offset + ", " + size;
		List<IndexedData> data = namedjc.query(sql.toString(), Collections.singletonMap("field", indexedId), new IndexedDataRowMapper());
		List<String> ids = new ArrayList<>();
		for(IndexedData id: data){
			ids.add(id.getId());
		}
		return listBeans(ids);
	}

	@Override
	public <Z> List<Z> indexBeanLookup(Class<Z> cls,
			Map<String, Object> keyValues) {
		ClassIndex index = indexByKeys(cls, keyValues.keySet());
		List<String> ids = new ArrayList<>();
		for(IndexedData id: super.indexedLookup(null, index,keyValues)){
			ids.add(id.getId());
		}
		return (List<Z>) listBeans(ids);
	}

	@Override
	public <T extends ObjectData> List<T> beanLookup(Class<T> cls, int offset,
			int size) {
		List<T> objs = objectLookup(cls, offset, size);
		List<T> beans = new ArrayList<T>();
		for (ObjectData data : objs) {
			try {
				beans.add(objectMapper.readValue(data.json, cls));
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
		return beans;
	}

	@Override
	public <T> Map<String, T> mapBeans(Collection<String> ids) {
		Map<String, Object> map = new HashMap<String, Object>();
		List<ObjectData> list = listBeans(ids);
		for(ObjectData od : list) {
			map.put(od.id, od);
		}
		return (Map<String, T>) map;
	}

	@Override
	public <T> void forEachBean(final Class<T> cls, final BeanHandler<T> handler) {
		ShardRunnable runnable = new RequestAwareShardRunnable(threadContext.get()) {
			@Override
			public void run(int dataSourceId) {
			    ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(getTc(), dataSourceId);
				String sql = "select json from " + getTableManager().getObjectDataTable(cls);
				JdbcTemplate jt = getJdbcTemplate(dataSource);
				jt.query(sql, new RowCallbackHandler() {
					@Override
					public void processRow(ResultSet rs) throws SQLException {
						String json = rs.getString("json");
						try {
							T bean = objectMapper.readValue(json, cls);
							handler.processBean(bean);
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}
				});
			}
		};
		forEachDataSourceOneByOne(runnable );
	}

	@Override
	public void forEachRawBean(final Class cls, final BeanHandler<ObjectData> handler) {
		ShardRunnable runnable = new RequestAwareShardRunnable(threadContext.get()) {
			@Override
			public void run(int dataSourceId) {
			    ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(getTc(), dataSourceId);
				String sql = "select * from " + getTableManager().getObjectDataTable(cls);
				JdbcTemplate jt = getJdbcTemplate(dataSource);
				jt.query(sql, new RowCallbackHandler() {
					@Override
					public void processRow(ResultSet arg0) throws SQLException {
						ObjectData row = new ObjectData();
						row.id = arg0.getString("id");
						row.created = arg0.getLong("created");
						row.updated = arg0.getLong("updated");
						row.version = arg0.getInt("version");
						row.json = arg0.getString("json");
						handler.processBean(row);
					}
				});
			}
		};
		forEachDataSourceOneByOne(runnable );
	}

	@Override
	public <Z> List<Z> indexBeanLookup(Class<Z> cls,
			Map<String, Object> keyValues, int dataSourceId) {
		return indexBeanLookup(cls, keyValues, -1, -1, dataSourceId);
	}

	@Override
	public <Z> List<Z> indexBeanLookup(String sql,
			Map<String, Object> keyValues, int dataSourceId) {
	    long start = DateUtil.currentTimeMillis();
	    logger.debug("indexBeanLookup start " + sql);
		List<IndexedData> data = super.indexLookup(dataSourceId, sql.toString(), keyValues);
		
		List<String> ids = new ArrayList<>();
		for(IndexedData id: data){
			ids.add(id.getId());
		}
        logger.debug("indexBeanLookup listBeans start ");
		List<Z> beans = listBeans(ids);
		
        long end = DateUtil.currentTimeMillis();
        logger.debug("indexBeanLookup end " + (end - start));
        return beans;
	}

	@Override
	public <Z> List<Z> indexBeanLookup(String sql,
			Map<String, Object> keyValues) {
		List<IndexedData> data = super.indexLookup(sql.toString(), keyValues);
		
		List<String> ids = new ArrayList<>();
		for(IndexedData id: data){
			ids.add(id.getId());
		}
		return listBeans(ids);
	}

	@Override
	public <Z> List<Z> indexBeanLookup(String sql) {
		return indexBeanLookup(sql, null);
	}

	@Override
	public <Z> List<Z> indexBeanLookup(Class<Z> cls,
			Map<String, Object> keyValues, int offset, int size,
			int dataSourceId) {
		ClassIndex index = indexByKeys(cls, keyValues.keySet());
		final StringBuilder sql = new StringBuilder("select * from ").append(index.getTableName());
		boolean first = true;
		Boolean emptyCollection = null;
        Map<String, Object> params = new HashMap<String, Object>();
        for(String s : keyValues.keySet()) {
            String columnName = s.toString().replaceAll("\\.", "__");
            if(first) {
                sql.append(" where ");
                first = false;
            }else {
                sql.append(" and ");
            }
            Object value = keyValues.get(s);
            if(value != null && value.getClass().isEnum()) {
                value = value.toString();
            }
            params.put(columnName, value);
			sql.append("`").append(columnName).append("`");
            if(value == null || value instanceof Collection) {
                sql.append(" in (:").append(columnName).append(")");
                Collection c = (Collection) value;
                if(emptyCollection == null && (value == null || c.size() == 0))
                    emptyCollection = true;
            }
            else {
                sql.append("=:").append(columnName);
            }
        }
		sql.append(" order by `created` desc ");
		if(offset>=0)
			sql.append(" limit :offset,:size");
		
		if(offset>=0) {
			params.put("offset", offset);
			params.put("size", size);
		}

		List<IndexedData> data = null;
		if(Boolean.TRUE.equals(emptyCollection)) 
			data = new ArrayList<IndexedData>();
		else
			data = super.indexLookup(dataSourceId, sql.toString(), params);
		
		List<String> ids = new ArrayList<>();
		for(IndexedData id: data){
			ids.add(id.getId());
		}
		return listBeans(ids);
	}

	@Override
	public List<Class> listManagedClasses() {
		return forClasses;
	}
	
	@Override
	public <T> List<T> query(final String sql, final RowMapper<T> mapper) {
		final List<T> list = new ArrayList<T>();
		forEachDataSource(new RequestAwareShardRunnable(threadContext.get()) {
			@Override
			public void run(int dataSourceId) {
			    ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(getTc(), dataSourceId);
				JdbcTemplate jt = getJdbcTemplate(dataSource);
				List<T> result = jt.query(sql, mapper);
				synchronized (list) {
					list.addAll(result);
				}
			}
		});
		return list;
	}

	@Override
	public <T> List<T> queryBeans(final String sql, final Class<T>cls) {
		final List<T> list = new ArrayList<T>();
		forEachDataSource(new RequestAwareShardRunnable(threadContext.get()) {
			@Override
			public void run(int dataSourceId) {
			    ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(getTc(), dataSourceId);
				JdbcTemplate jt = getJdbcTemplate(dataSource);
				List<T> result = jt.query(sql, new RowMapper<T>(){
					@Override
					public T mapRow(ResultSet rs, int rowNum)
							throws SQLException {
						try {
							String json = rs.getString("json");
							T obj = objectMapper.readValue(json, cls);
							return obj;
						} catch (Exception e) {
							throw new RuntimeException(e);
						}
					}});
				synchronized (list) {
					list.addAll(result);
				}
			}
		});
		return list;
	}

    @Override
    public int count(Class cls) {
        String sql = "select count(*) from `" + cls.getSimpleName()+"`";
        return indexCountLookup(sql, null);
    }
}
