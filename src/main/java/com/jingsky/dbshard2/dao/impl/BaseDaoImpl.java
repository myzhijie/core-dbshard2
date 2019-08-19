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

import java.io.IOException;
import java.io.OutputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

import com.jingsky.dbshard2.cache.CacheProxy;
import com.jingsky.dbshard2.cache.DummyCacheProxy;
import com.jingsky.dbshard2.cache.NullCache;
import com.jingsky.dbshard2.dao.entity.IndexedData;
import com.jingsky.dbshard2.dao.entity.ObjectData;
import com.jingsky.dbshard2.util.DateUtil;
import com.jingsky.dbshard2.util.JacksonUtil;
import com.jingsky.dbshard2.util.MultiTask;
import com.jingsky.dbshard2.util.reflection.ReflectionUtil;
import org.apache.log4j.Logger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowCallbackHandler;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

import com.jingsky.dbshard2.dao.BaseDao;
import com.jingsky.dbshard2.dao.ClassIndex;
import com.jingsky.dbshard2.dao.DbDialet;
import com.jingsky.dbshard2.dao.ExtendedDataSource;
import com.jingsky.dbshard2.dao.InvalidIdException;
import com.jingsky.dbshard2.dao.ObjectId;
import com.jingsky.dbshard2.dao.RequestAware;
import com.jingsky.dbshard2.dao.RequestContext;
import com.jingsky.dbshard2.dao.ShardResolver;
import com.jingsky.dbshard2.dao.ShardedDataSource;
import com.jingsky.dbshard2.dao.TableManager;

@SuppressWarnings({"rawtypes","unchecked"}) 
public class BaseDaoImpl implements BaseDao, RequestAware {
	private static Logger logger = Logger.getLogger(BaseDaoImpl.class);
	
	protected ShardResolver shardResolver;
	protected ShardedDataSource shardedDataSource;
	protected ExecutorService executorService;
	protected CacheProxy cacheProxy;
	protected TableManager tableManager;
	
	protected ThreadLocal<RequestContext> threadContext = new ThreadLocal<RequestContext>();
	
	public ShardResolver getShardResolver() {
		return shardResolver;
	}
	public void setShardResolver(ShardResolver shardResolver) {
		this.shardResolver = shardResolver;
	}
	public ShardedDataSource getShardedDataSource() {
		return shardedDataSource;
	}
	public void setShardedDataSource(ShardedDataSource shardedDataSource) {
		this.shardedDataSource = shardedDataSource;
	}
	public ExecutorService getExecutorService() {
		return executorService;
	}
	public void setExecutorService(ExecutorService executorService) {
		this.executorService = executorService;
	}
	public CacheProxy getCacheProxy() {
		return cacheProxy==null ? DummyCacheProxy.getInstance() : cacheProxy;
	}
	public void setCacheProxy(CacheProxy cacheService) {
		this.cacheProxy = cacheService;
	}
	public TableManager getTableManager() {
		return tableManager;
	}
	public void setTableManager(TableManager tableManager) {
		this.tableManager = tableManager;
	}
	
    @Override
    public int getDataSourceIdForObjectId(String id) {
        return getDataSourceById(new ObjectId(id)).getDataSourceId();
    }
    
    protected ExtendedDataSource getDataSourceById(ObjectId id){
		//do not check (id.getShard() < shardResolver.getNumberOfShards()) if TimedShardResolver
		if(shardResolver instanceof TimedShardResolver){
			return shardedDataSource.getDataSourceByShardId(threadContext.get(), id.getShard());
		}
        if(id != null && id.getShard() < shardResolver.getNumberOfShards())
            return shardedDataSource.getDataSourceByShardId(threadContext.get(), id.getShard());
        else
            throw new InvalidIdException(id.toString());
    }
    
    @Override
    public int getDataSourceIdForObject(ObjectData od) {
        if(od.id != null)
            return getDataSourceIdForObjectId(od.id);
        int shardId = shardResolver.getShardId(od);
        return shardedDataSource.getDataSourceByShardId(threadContext.get(), shardId).getDataSourceId();
    }
    
    protected JdbcTemplate getJdbcTemplate(ExtendedDataSource dataSource) {
        JdbcTemplate jt = null;
        try {
            if(dataSource.getThreadContext().get() == null)
                return new JdbcTemplate(dataSource);
            jt = new JdbcTemplate(new SingleConnectionDataSource(dataSource.getConnection(), true));
            return jt;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
    protected NamedParameterJdbcTemplate getNamedParameterJdbcTemplate(ExtendedDataSource dataSource) {
        NamedParameterJdbcTemplate jt = null;
        try {
            if(dataSource.getThreadContext().get() == null)
                return new NamedParameterJdbcTemplate(dataSource);
            jt = new NamedParameterJdbcTemplate(new SingleConnectionDataSource(dataSource.getConnection(), true));
            return jt;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
    
	@Override
	public int create(final ObjectData obj) {
		if(!obj.getClass().equals(ObjectData.class))
			throw new RuntimeException("Please call createBean method");
		
		if(obj.json == null)
			throw new RuntimeException("json should not be null");
		final ObjectId oi = new ObjectId(obj.id);
		final AtomicInteger ups = new AtomicInteger();
		ExtendedDataSource dataSource = getDataSourceById(oi);
		String sql = "insert into " + getTableManager().getObjectDataTable(oi) + " (`id`, `created`, `updated`, `version`, `json`) values (?, ?, ?, ?, ?)";
		logger.debug(sql + "\n" + obj.json);
		JdbcTemplate jt = getJdbcTemplate(dataSource);
		int res = jt.update(sql, obj.id, obj.created, obj.updated, obj.version, obj.json);
		ups.getAndAdd(res);
		getCacheProxy().set(obj.id, obj);
		return ups.get();
	}

    @Override
	public int update(final ObjectData obj) {
		if(obj.json == null)
			throw new RuntimeException("json should not be null");
		final ObjectId oi = new ObjectId(obj.id);
		obj.updated = DateUtil.currentTimeMillis();
		final AtomicInteger ups = new AtomicInteger();
		ExtendedDataSource dataSource = getDataSourceById(oi);
		String sql = "update " + getTableManager().getObjectDataTable(oi) + " set `created`=?, `updated`=?, `version`=?, `json`=? where `id`=?";
		JdbcTemplate jt = getJdbcTemplate(dataSource);
		int res = jt.update(sql, obj.created, obj.updated, obj.version, obj.json, obj.id);
		logger.debug("update " + obj.id + " with json " + obj.json);
		ups.getAndAdd(res);
		getCacheProxy().set(obj.id, obj);
		return ups.get();
	}

	@Override
	public ObjectData objectLookup(String id) {
        long t0 = DateUtil.currentTimeMillis();
        long t1 = 0;
		ObjectData data = null;
		if(id != null){
            ObjectId oi = new ObjectId(id);
            if(oi.getShard() >= shardResolver.getNumberOfShards())
                throw new InvalidIdException(oi.toString());
            
			Object cached = getCacheProxy().get(oi.toString());
			if(cached == null){
                t1 = DateUtil.currentTimeMillis() - t0;
                if(t1>1000) {
                    System.out.println(threadContext.get() + " objectLookup before getDataSourceByShardId " + oi + " costs " + t1);
                }
				ExtendedDataSource dataSource = getDataSourceById(oi);
                t1 = DateUtil.currentTimeMillis() - t0;
                if(t1>1000) {
                    System.out.println(threadContext.get() + " objectLookup getDataSourceByShardId " + oi + " costs " + t1);
                }
				String sql = "select * from " + getTableManager().getObjectDataTable(oi) + " where `id`=?";
				JdbcTemplate jt = getJdbcTemplate(dataSource);
		        t1 = DateUtil.currentTimeMillis() - t0;
		        if(t1>1000) {
		            System.out.println(threadContext.get() + " objectLookup getJdbcTemplate " + oi + " costs " + t1);
		        }
				List<ObjectData> od = jt.query(sql, new Object[]{oi.toString()}, new ObjectDataRowMapper());
                t1 = DateUtil.currentTimeMillis() - t0;
                if(t1>1000) {
                    System.out.println(threadContext.get() + " objectLookup query " + oi + " costs " + t1);
                }
				
				data = od.size() > 0 ? od.get(0) : null;
				getCacheProxy().set(oi.toString(), data != null ? data : NullCache.getInstance());
			}else if(cached != null && !(cached instanceof NullCache)){
				data = (ObjectData) cached;
			}
		}
        
        t1 = DateUtil.currentTimeMillis() - t0;
        if(t1>1000) {
            System.out.println(threadContext.get() + " objectLookup " + id + " costs " + t1);
        }
		return data;
	}

	@Override
	public List<ObjectData> objectLookup(Collection<String> ids) {
		HashMap<String, ObjectData> map = new HashMap<String, ObjectData>();
		List<ObjectData> result = new ArrayList<>();
		if(ids == null || ids.size()==0){
			return result;
		}
		List<String> spilledIdList = new ArrayList<>(ids);
		for(Entry<String, Object> entry : getCacheProxy().getBulk(spilledIdList).entrySet()) {
			Object value = entry.getValue();
			if(value != null && !(value instanceof NullCache)) {
				ObjectData od = (ObjectData) value;
				map.put(od.id, od);
			}
			spilledIdList.remove(entry.getKey());
		}
		
		if(spilledIdList.size() > 0){
			ObjectId oi = new ObjectId(spilledIdList.get(0));
			final String sql = "select * from " + getTableManager().getObjectDataTable(oi) + " where `id` in (:ids)";
			final Map<Integer, List<String>> shardedIds = shardedDataSource.splitByDataSource(spilledIdList);
			final List<ObjectData> spilledResults = new ArrayList<>();
			
			forSelectDataSources(shardedIds.keySet(),new RequestAwareShardRunnable(threadContext.get()) {
				@Override
				public void run(int dataSourceId) {
				    ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(getTc(), dataSourceId);
					NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
					List<ObjectData> data = namedjc.query(sql, Collections.singletonMap("ids", shardedIds.get(dataSourceId)), new ObjectDataRowMapper());
					synchronized (spilledResults) {
						spilledResults.addAll(data);
					}
				}
			});

			HashMap<String,Object> spilledMap = new HashMap<>();
			for (ObjectData objectData : spilledResults) {
				spilledMap.put(objectData.id, objectData);
				spilledIdList.remove(objectData.id);
				map.put(objectData.id, objectData);
			}

			for(String id : spilledIdList){
				spilledMap.put(id, new NullCache());
			}
			getCacheProxy().setBulk(spilledMap);
		}
		
		for(String id : ids) {
			ObjectData data = map.get(id);
			if(data != null)
				result.add(data);
		}
		return result;
	}

	@Override
	public int delete(String id) {
		ObjectId oi = new ObjectId(id);
		ExtendedDataSource dataSource = getDataSourceById(oi);
		String sql = "delete from " + getTableManager().getObjectDataTable(oi) + " where `id`=?";
		JdbcTemplate jt = getJdbcTemplate(dataSource);
		int res = jt.update(sql, id);
		getCacheProxy().set(id, NullCache.getInstance());
		return res;
	}

	@Override
	public int deleteAll(final Collection<String> ids) {
		final AtomicInteger ups = new AtomicInteger();
		if(ids.size() > 0){
			ObjectId oi = new ObjectId(ids.iterator().next());
			final String sql = "delete from " + getTableManager().getObjectDataTable(oi) + " where `id` in (:ids)";
			final Map<Integer, List<String>> shardedIds = shardedDataSource.splitByDataSource(ids);
			
			forSelectDataSources(shardedIds.keySet(), new RequestAwareShardRunnable(threadContext.get()) {
				@Override
				public void run(int shardId) {
				    ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(getTc(), shardId);
					NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
					int update = namedjc.update(sql, Collections.singletonMap("ids", shardedIds.get(shardId)));
					synchronized (ups) {
						ups.getAndAdd(update);
					}
				}
			});
			
			HashMap<String,Object> cache = new HashMap<>();
			for(String id: ids){
				cache.put(id, NullCache.getInstance());
			}
			getCacheProxy().setBulk(cache);
		}
		return ups.intValue();
	}
	
	protected int createIndex(ClassIndex index, Map<String, Object>values) {
		String table = index.getTableName();
		StringBuilder sql = new StringBuilder("insert into ").append(table).append(" (");
		StringBuilder valueNames = new StringBuilder();
		valueNames.append(" values (");
		boolean first = true;
		for(String s : values.keySet().toArray(new String[0])) {
			if(first) {
				first = false;
			}else {
				sql.append(",");
				valueNames.append(",");
			}
			sql.append("`").append(s).append("`");
			valueNames.append(":").append(s);
			Object value = values.get(s);
			if(value != null && value instanceof Enum<?>) {
				values.put(s, value.toString());
			}
		}
		sql.append(")");
		valueNames.append(")");
		sql.append(valueNames.toString());
		
		ObjectId objectId = new ObjectId((String)values.get("id"));
		ExtendedDataSource dataSource = getDataSourceById(objectId);
		NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
		int res = namedjc.update(sql.toString(), values);
		getCacheProxy().delete(index.getIndexedLookupKey(values));
		return res;
	}

	@Override
	public List<IndexedData> indexLookup(final String sql, final Map<String, Object> values) {
		final List<IndexedData> result = new ArrayList<>();
		ShardRunnable runnable = new RequestAwareShardRunnable(threadContext.get()) {
			@Override
			public void run(int dataSourceId) {
			    ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(getTc(), dataSourceId);
				NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
				List<IndexedData> data = namedjc.query(sql.toString(), values, new IndexedDataRowMapper());
				synchronized (result) {
					result.addAll(data);
				}
			}
		};
		forEachDataSource(runnable);
		return result;
	}

	@Override
	public List<IndexedData> indexLookup(int dataSourceId, final String sql, final Map<String, Object> values) {
	    ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(threadContext.get(), dataSourceId);
		NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
		List<IndexedData> data = namedjc.query(sql.toString(), values, new IndexedDataRowMapper());
		return data;
	}

	@Override
	public int indexCountLookup(final String sql, final Map<String, Object> values) {
		final AtomicInteger ai = new AtomicInteger();
		ShardRunnable runnable = new RequestAwareShardRunnable(threadContext.get()) {
			@Override
			public void run(int dataSourceId) {
			    ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(getTc(), dataSourceId);
				NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
				int count = namedjc.queryForObject(sql.toString(), values, Integer.class);
				ai.addAndGet(count);
			}
		};
		forEachDataSource(runnable);
		return ai.intValue();
	}

	@Override
	public int indexCountLookup(int dataSourceId, final String sql, final Map<String, Object> values) {
	    ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(threadContext.get(), dataSourceId);
		NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
		int count = namedjc.queryForObject(sql.toString(), values, Integer.class);
		return count;
	}
	
	protected int indexedCountLookup(final ClassIndex index, final Map<String, Object> values) {
		String table = index.getTableName();
		final StringBuilder sql = new StringBuilder("select count(*) from ").append(table);
		boolean first = true;
		Boolean emptyCollection = null;
		Map<String, Object> params = new HashMap<String, Object>();
		for(String s : values.keySet()) {
			String columnName = s.toString().replaceAll("\\.", "__");
			if(first) {
				sql.append(" where ");
				first = false;
			}else {
				sql.append(" and ");
			}
			Object value = values.get(s);
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
		
		if(Boolean.TRUE.equals(emptyCollection)) 
			return 0;
		else
			return indexCountLookup(sql.toString(), params);
	}
	
	protected int indexedCountLookup(final ClassIndex index, final Map<String, Object> values, int dataSourceId) {
		String table = index.getTableName();
		final StringBuilder sql = new StringBuilder("select count(*) from ").append(table);
		boolean first = true;
		Boolean emptyCollection = null;
		Map<String, Object> params = new HashMap<String, Object>();
		for(String s : values.keySet()) {
			String columnName = s.toString().replaceAll("\\.", "__");
			if(first) {
				sql.append(" where ");
				first = false;
			}else {
				sql.append(" and ");
			}
			Object value = values.get(s);
            if(value != null && value.getClass().isEnum()) {
                value = value.toString();
            }
			params.put(columnName, value);
            sql.append("`").append(columnName).append("`");
			if(value == null || value instanceof Collection) {
				sql.append(" in (:").append(columnName).append(")");
				Collection c = (Collection) value;
				emptyCollection = c.size() == 0;
			}
			else {
				sql.append("=:").append(columnName);
			}
		}
		
		if(Boolean.TRUE.equals(emptyCollection))
			return 0;
		else
			return indexCountLookup(dataSourceId, sql.toString(), params);
	}
	
	public List<IndexedData> indexedLookup(Integer dataSourceId, final ClassIndex index, final Map<String, Object> values) {
		String key = getTableManager().getIndexedLookupKey(index, values);
		Object cached = getCacheProxy().get(key);
		if(cached != null) {
			return (List<IndexedData>) cached;
		}
		Map<String, Object> params = new HashMap<String, Object>();
		String table = index.getTableName();
		final StringBuilder sql = new StringBuilder("select * from ").append(table);
		boolean first = true;
		Boolean emptyCollection = null;
		for(Object s : values.keySet().toArray()) {
			String columnName = s.toString().replaceAll("\\.", "__");
			if(first) {
				sql.append(" where ");
				first = false;
			}else {
				sql.append(" and ");
			}
			Object value = values.get(s);
            if(value != null && value.getClass().isEnum()) {
                value = value.toString();
            }
			params.put(columnName, value);
            sql.append("`").append(columnName).append("`");
			if(value == null || value instanceof Collection) {
				sql.append(" in (:").append(columnName).append(")");
				Collection c = (Collection) value;
				emptyCollection = c.size() == 0;
			}
			else {
				sql.append("=:").append(columnName);
			}
		}
		sql.append(" order by `created` desc ");
		
		List<IndexedData> result = null;
		if(Boolean.TRUE.equals(emptyCollection)) 
			result = new ArrayList<IndexedData>();
		else if(dataSourceId != null)
			result = indexLookup(dataSourceId, sql.toString(), params);
		else 
			result = indexLookup(sql.toString(), params);
		getCacheProxy().set(key, result);
		return result;
	}

	@Override
	public int deleteIndexData(ClassIndex ind, final String id) {
		final String sql = "delete from " + ind.getTableName() + " where `id` = :id";
		ExtendedDataSource dataSource = shardedDataSource.getDataSourceByObjectId(threadContext.get(), id);
        NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
        int update = namedjc.update(sql, Collections.singletonMap("id", id));
        return update;
	}

	@Override
	public int updateAll(final String sql, final Object... objects) {
		final AtomicInteger ups = new AtomicInteger();
		forEachDataSource(new RequestAwareShardRunnable(threadContext.get()) {
			@Override
			public void run(int dataSourceId) {
			    ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(getTc(), dataSourceId);
				JdbcTemplate jc = getJdbcTemplate(dataSource);
				int update = jc.update(sql, objects);
				logger.debug(dataSourceId + " DBUpdate: " + sql);
				synchronized (ups) {
					ups.getAndAdd(update);
				}
			}
		});
		return ups.intValue();
	}

	@Override
	public int updateAll(final String sql, final Map<String, ?> params) {
		final AtomicInteger ups = new AtomicInteger();
		forEachDataSource(new RequestAwareShardRunnable(threadContext.get()) {
			@Override
			public void run(int dataSourceId) {
			    ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(getTc(), dataSourceId);
				NamedParameterJdbcTemplate jc = getNamedParameterJdbcTemplate(dataSource);
				int update = jc.update(sql, params);
				synchronized (ups) {
					ups.getAndAdd(update);
				}
			}
		});
		return ups.intValue();
	}
	
	public void forEachDataSource(ShardRunnable runnable){
		MultiTask mt = new MultiTask();
		Set<String> urlSet = new HashSet<>();
		for(int i= 0; i<shardResolver.getNumberOfShards() / shardedDataSource.getShardsPerDataSource(); i++) {
			ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(threadContext.get(), i);
			if (dataSource != null && !urlSet.contains(dataSource.getUrl())) {
				urlSet.add(dataSource.getUrl());
				ShardTask shardTask = new ShardTask(i, runnable);
				mt.addTask(shardTask);
			}
		}
		mt.execute(getExecutorService());
	}
	
	public void forEachDataSourceOneByOne(ShardRunnable runnable){
		Set<String> urlSet = new HashSet<>();
		for(int i= 0; i<shardResolver.getNumberOfShards() / shardedDataSource.getShardsPerDataSource(); i++) {
			ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(threadContext.get(), i);
			if (dataSource != null && !urlSet.contains(dataSource.getUrl())) {
				urlSet.add(dataSource.getUrl());
				runnable.run(i);
			}
		}
	}
	
	public void forSelectDataSources(Collection<Integer> dataSourceIds, ShardRunnable runnable){
        if(dataSourceIds.size()>1) {
            MultiTask mt = new MultiTask();
			Set<String> urlSet = new HashSet<>();
            for(Integer dataSourceId : dataSourceIds) {
				ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(threadContext.get(), dataSourceId);
				if (dataSource != null && !urlSet.contains(dataSource.getUrl())) {
					urlSet.add(dataSource.getUrl());
					ShardTask shardTask = new ShardTask(dataSourceId, runnable);
					mt.addTask(shardTask);
				}
            }
            mt.execute(getExecutorService());
        }
        else if(dataSourceIds.size() == 1) {
            int dataSourceId = (int) dataSourceIds.toArray()[0];
            ShardTask shardTask = new ShardTask(dataSourceId, runnable);
            shardTask.run();
        }
	}
	
	static class ObjectDataRowMapper<T extends ObjectData> implements RowMapper<T>{
		private Class cls = null;
		public ObjectDataRowMapper() {
			try {
				if(this.getClass().equals(ObjectDataRowMapper.class))
					cls = ObjectData.class;
				else
					cls = ReflectionUtil.getParameterizedType(this.getClass());
			} catch (Exception e) {
			}
		}
		
		public ObjectDataRowMapper(Class cls) {
			this.cls = cls;
		}
		
		@Override
		public T mapRow(ResultSet arg0, int arg1)
				throws SQLException {
			try {
				T row = (T) cls.newInstance();
				row.id = arg0.getString("id");
				row.created = arg0.getLong("created");
				row.updated = arg0.getLong("updated");
				row.version = arg0.getInt("version");
				row.json = arg0.getString("json");
				return row;
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
	
	@Override
	public <T extends ObjectData> List<T> objectLookup(final Class<T> cls) {
		return objectLookup(cls, -1, -1);
	}
	
	@Override
	public <T extends ObjectData> List<T> objectLookup(final Class<T> cls, int offset, int size) {
		final Map<String, Object> keyValues = new HashMap<String, Object>();
		final StringBuilder sb = new StringBuilder();
		sb.append("select * from ").append("`"+cls.getSimpleName()+"`");
		boolean first = true;
		for(Entry<String, Object>entry : keyValues.entrySet()) {
			if(first) {
				sb.append(" where ");
				first = false;
			}else {
				sb.append(" and ");
			}
			sb.append("`").append(entry.getKey()).append("`").append(" = :").append(entry.getKey());
		}
		sb.append(" order by `created` desc ");
		
		if(offset>-1) {
			sb.append(" limit :offset, :size");
			keyValues.put("offset", offset);
			keyValues.put("size", size);
		}
		
		final List<T> results = new ArrayList<T>();
		forEachDataSource(new RequestAwareShardRunnable(threadContext.get()) {
			@Override
			public void run(int dataSourceId) {
			    ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(getTc(), dataSourceId);
				NamedParameterJdbcTemplate jc = getNamedParameterJdbcTemplate(dataSource);
				List<T> dsResults = jc.query(sb.toString(), keyValues, new ObjectDataRowMapper(cls));
				synchronized (results) {
					results.addAll(dsResults);
				}
			}
		});
		return results;
	}
	
	@Override
	public boolean isType(String id, Class clss) {
		return tableManager.isType(id, clss);
	}
	
	@Override
	public void createTable(DbDialet dialet) {
	}
	
	@Override
	public void dumpTable(final OutputStream output, final String tableName, final String fields) {
		final String[] names = fields == null ? null : fields.split(",");;
		forEachDataSourceOneByOne(new RequestAwareShardRunnable(threadContext.get()) {
			@Override
			public void run(int dataSourceId) {
				String sql = "select `json` from " + tableName.replaceAll(" ", "");
				ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(getTc(), dataSourceId);
				JdbcTemplate jc = getJdbcTemplate(dataSource);
				jc.query(sql, new RowCallbackHandler(){
					@Override
					public void processRow(ResultSet rs) throws SQLException {
						try {
							String json = rs.getString(1);
							if(names == null)
								output.write(json.getBytes());
							else {
								HashMap<String, Object> map = JacksonUtil.json2map(json);
								boolean first = true;
								for(String name : names) {
									if(first) {
										first = false;
									}
									else {
										output.write('\t');
									}
									Object obj = map.get(name);
									if(obj == null)
										output.write("NULL".getBytes());
									else
										output.write(obj.toString().getBytes());
								}
							}
							output.write('\n');
						} catch (IOException e) {
							throw new SQLException(e);
						}
					}});
			}
		});
	}
	
	@Override
	public void setRequestContext(RequestContext tc) {
		threadContext.set(tc);
	}
	
	@Override
	public <T> List<T> indexQuery(int dataSourceId, String sql,
			Map<String, Object> params, RowMapper<T> rowMapper) {
	    ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(threadContext.get(), dataSourceId);
		NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
		return namedjc.query(sql, params, rowMapper);
	}
	
	@Override
	public <T> List<T> indexQuery(final String sql, final Map<String, Object> params,
			final RowMapper<T> rowMapper) {
		final List<T> result = new ArrayList<T>();
		forEachDataSource(new RequestAwareShardRunnable(threadContext.get()) {
			@Override
			public void run(int dataSourceId) {
			    ExtendedDataSource dataSource = shardedDataSource.getDataSourceByDataSourceId(getTc(), dataSourceId);
				NamedParameterJdbcTemplate namedjc = getNamedParameterJdbcTemplate(dataSource);
				List<T> list = namedjc.query(sql, params, rowMapper);
				synchronized (result) {
					result.addAll(list);
				}
			}
		});
		return result;
	}

}

