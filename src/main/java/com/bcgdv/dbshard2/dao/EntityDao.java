package com.bcgdv.dbshard2.dao;

import com.bcgdv.dbshard2.dao.entity.Entity;
import com.bcgdv.dbshard2.util.CamelUnderScore;
import com.bcgdv.dbshard2.util.ResultSetObjectMapper;
import com.bcgdv.dbshard2.util.SqlUtil;
import com.google.common.collect.Lists;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;

import java.lang.reflect.ParameterizedType;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public abstract class EntityDao<T extends Entity> {
    @Autowired
    protected JdbcTemplate jdbcTemplate;
    protected Class<T> recordClass = (Class<T>) ((ParameterizedType) getClass().getGenericSuperclass()).getActualTypeArguments()[0];
    protected String table = CamelUnderScore.underscore(recordClass.getSimpleName());

    public void insertAll(List<T> items) {
        if (items.size() > 0) {
            try {
                int index = 0;
                List<T> data = new ArrayList<T>();
                while (index < items.size()) {
                    T entity = items.get(index);
                    entity.version = 0;
                    entity.created = entity.updated = System.currentTimeMillis();
                    data.add(entity);
                    if (index % 200 == 199) {
                        String insert = SqlUtil.getInsertStatement(data);
                        jdbcTemplate.execute(insert);
                        data = new ArrayList<T>();
                    }
                    ++index;
                }

                if (data.size() > 0) {
                    String insert = SqlUtil.getInsertStatement(data);
                    jdbcTemplate.execute(insert);
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    }

    public String insert(T item) {
        KeyHolder keyHolder = new GeneratedKeyHolder();

        try {
            item.created = item.updated = System.currentTimeMillis();
            item.version = 0;
            String stmt = SqlUtil.getInsertStatement(item);
            jdbcTemplate.update(
                    (Connection conn) -> {
                        return conn.prepareStatement(stmt, new String[]{"id"});
                    }, keyHolder
            );
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        item.id = Long.parseLong(keyHolder.getKey().toString());

        return keyHolder.getKey().toString();
    }

    public int updateByVersion(T object) {
        if (object.id == null) {
            return 0;
        }

        int ret = 0;
        try {
            object.updated = System.currentTimeMillis();
            object.version += 1;
            String sql = appendVersion(SqlUtil.getUpdateStatement(object, "id"), object.version - 1);
            ret = jdbcTemplate.update(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ret;
    }

    public int update(T object) {
        if (object.id == null) {
            return 0;
        }

        int ret = 0;
        try {
            object.updated = System.currentTimeMillis();
            String sql = SqlUtil.getUpdateStatement(object, "id");
            ret = jdbcTemplate.update(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ret;
    }

    public int updateIgnoreNullByVersion(T object) {
        if (object.id == null) {
            return 0;
        }

        int ret = 0;
        try {
            object.updated = System.currentTimeMillis();
            object.version += 1;
            String sql = appendVersion(SqlUtil.getUpdateIgnoreNullStatement(object, "id"), object.version - 1);
            ret = jdbcTemplate.update(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ret;
    }

    public int updateIgnoreNull(T object) {
        if (object.id == null) {
            return 0;
        }

        int ret = 0;
        try {
            object.updated = System.currentTimeMillis();
            String sql = SqlUtil.getUpdateIgnoreNullStatement(object, "id");
            ret = jdbcTemplate.update(sql);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ret;
    }

    public int delete(Long id, Integer version) {
        if (id == null) {
            return 0;
        }

        int ret = 0;
        try {
            String table = CamelUnderScore.underscore(recordClass.getSimpleName());
            StringBuilder sb = new StringBuilder();
            sb.append(" delete from ").append(table);
            sb.append(" where id = '").append(id).append("'");
            sb.append(" and version = ").append(version);
            ret = jdbcTemplate.update(sb.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ret;
    }

    public int delete(Long id) {
        if (id == null) {
            return 0;
        }

        int ret = 0;
        try {
            String table = CamelUnderScore.underscore(recordClass.getSimpleName());
            StringBuilder sb = new StringBuilder();
            sb.append(" delete from ").append(table);
            sb.append(" where id = '").append(id).append("'");
            ret = jdbcTemplate.update(sb.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ret;
    }

    public int delete(List<T> items) {
        if (items.size() <= 0) {
            return 0;
        }

        int ret = 0;
        try {
            String table = CamelUnderScore.underscore(recordClass.getSimpleName());
            StringBuilder sb = new StringBuilder();
            sb.append(" delete from ").append(table);
            sb.append(" where (id, version) in (");

            String[] in = new String[items.size()];
            for (int i = 0; i < in.length; i++) {
                in[i] = "('" + items.get(i).id + "', '" + items.get(i).version + "')";
            }
            sb.append(StringUtils.join(in, ","));

            sb.append(")");
            ret = jdbcTemplate.update(sb.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ret;
    }

    public int deleteByIds(List<Long> ids) {
        if (ids.size() <= 0) {
            return 0;
        }

        int ret = 0;
        try {
            String table = CamelUnderScore.underscore(recordClass.getSimpleName());
            StringBuilder sb = new StringBuilder();
            sb.append(" delete from ").append(table);
            sb.append(" where id in (");

            String[] in = new String[ids.size()];
            for (int i = 0; i < in.length; i++) {
                in[i] = "'" + ids.get(i) + "' ";
            }
            sb.append(StringUtils.join(in, ","));

            sb.append(")");
            ret = jdbcTemplate.update(sb.toString());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return ret;
    }

    public T get(Long id) {
        if (id == null) {
            return null;
        }

        try {
            String table = CamelUnderScore.underscore(recordClass.getSimpleName());
            StringBuilder sb = new StringBuilder();
            sb.append(" select * from ").append(table);
            sb.append(" where id = '").append(id).append("'");
            return jdbcTemplate.queryForObject(sb.toString(), new ResultSetObjectMapper<>(recordClass));
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    public List<T> get(int offset, int size) {
        String table = CamelUnderScore.underscore(recordClass.getSimpleName());
        StringBuilder sb = new StringBuilder();
        sb.append(" select * from ").append(table);
        sb.append(" order by id limit ").append(offset).append(", ").append(size);
        return jdbcTemplate.query(sb.toString(), new ResultSetObjectMapper<>(recordClass));
    }

    public long getCount() {
        String table = CamelUnderScore.underscore(recordClass.getSimpleName());
        StringBuilder sb = new StringBuilder();
        sb.append(" select count(*) from ").append(table);
        return jdbcTemplate.queryForObject(sb.toString(), Long.class);
    }

    private static List<List> splitList(List items, int size) {
        return Lists.partition(items, size);
    }

    private static String appendVersion(String sql, int version) {
        StringBuilder sb = new StringBuilder();
        sb.append(sql).append(" and version = ").append(version);
        return sb.toString();
    }
}
