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

package com.jingsky.dbshard2.util;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import com.jingsky.dbshard2.util.reflection.FieldFoundCallback;
import com.jingsky.dbshard2.util.reflection.ReflectionUtil;
import com.fasterxml.jackson.databind.JavaType;

public class SqlUtil {
    public static boolean ignoreSupported = true;
    
    public static String getInsertStatement(Collection<?> list, boolean insertIgnore) throws Exception {
        String ret = null;
        if(list.size()>0){
            final StringBuilder sb = new StringBuilder();
            for(Object obj : list){
                if(sb.length()==0){
                    sb.append(getInsertHeader(obj,insertIgnore));
                }else{
                    sb.append(",");
                }
                sb.append(getInsertValues(obj));
            }
            ret = sb.toString();
        }
        return ret;
    }
    
    public static String getInsertStatement(Object obj) throws Exception {
        return getInsertHeader(obj)+getInsertValues(obj);
    }
    
    public static String getInsertIgnoreStatement(Object obj) throws Exception {
        return getInsertHeader(obj, true)+getInsertValues(obj);
    }
    
    public static String getInsertHeader(Object obj, boolean insertIgnore) throws Exception {
        final StringBuilder sb = new StringBuilder();
        Class<?> beanClass = obj.getClass();
        ReflectionUtil.iterateFields(beanClass, obj, new FieldFoundCallback() {
            @Override
            public void field(Object o, Field field) throws Exception {
                if(!Modifier.isStatic(field.getModifiers()) && !Modifier.isTransient(field.getModifiers())) {
	                if(sb.length() > 0) {
	                    sb.append(", ");
	                }
	                sb.append(CamelUnderScore.underscore(field.getName()));
                }
            }
        });
        String table = CamelUnderScore.underscore(obj.getClass().getSimpleName());
        if(insertIgnore && ignoreSupported)
            return "insert ignore into "+table+" ("+sb.toString()+") VALUES ";
        else
            return "insert into "+table+" ("+sb.toString()+") VALUES ";
    }
    
    public static String getInsertValues(Object obj) throws Exception {
        final StringBuilder sb = new StringBuilder();
        Class<?> beanClass = obj.getClass();
        ReflectionUtil.iterateFields(beanClass, obj, new FieldFoundCallback() {
            @Override
            public void field(Object o, Field field) throws Exception {
                field.setAccessible(true);
                if(!Modifier.isStatic(field.getModifiers()) && !Modifier.isTransient(field.getModifiers())) {
	                Object fieldValue = field.get(o);
	                if(sb.length() > 0) {
	                    sb.append(", ");
	                }
	                if(fieldValue != null){
		                Class<?> fieldType = field.getType();
		                if(String.class.equals(fieldType)) {
		                    String value = ((String)fieldValue).replaceAll("'", "''").replaceAll("\\\\", "\\\\\\\\");
		                    sb.append("'"+value+"'");
		                }
		                else if(fieldType.isEnum()) {
		                    sb.append("'"+fieldValue+"'");
		                }
		                else if(fieldType.isPrimitive() || Number.class.isAssignableFrom(fieldType)){
		                    sb.append(fieldValue);
		                }else{
		                    String value = JacksonUtil.obj2Json(fieldValue).replaceAll("'", "''").replaceAll("\\\\", "\\\\\\\\");
		                    sb.append("'"+value+"'");
		                }
	                }else{
	                	sb.append("null");
	                }
                }
            }
        });
        return "("+sb.toString()+")";
    }
    
    public static String getUpdateStatement(Object obj, String idFieldName) throws Exception {
        return getUpdateStatement(obj, idFieldName, false);
    }

    public static String getUpdateIgnoreNullStatement(Object obj, String idFieldName) throws Exception {
        return getUpdateStatement(obj, idFieldName, true);
    }

    private static String getUpdateStatement(Object obj, String idFieldName, final boolean ignoreNull) throws Exception {
        final StringBuilder sb = new StringBuilder();
        Class<?> beanClass = obj.getClass();
        ReflectionUtil.iterateFields(beanClass, obj, new FieldFoundCallback() {
            @Override
            public void field(Object o, Field field) throws Exception {
                field.setAccessible(true);
                if(!Modifier.isStatic(field.getModifiers()) && !Modifier.isTransient(field.getModifiers())){
                    Object fieldValue = field.get(o);
                    if(fieldValue != null){
                        if(sb.length() > 0) {
                            sb.append(", ");
                        }
                        sb.append(CamelUnderScore.underscore(field.getName()));
                        sb.append("=");
                        Class<?> fieldType = field.getType();
                        if(String.class.equals(fieldType)) {
                            String value = ((String)fieldValue).replaceAll("'", "''").replaceAll("\\\\", "\\\\\\\\");
                            sb.append("'"+value+"'");
                        }
                        else if(fieldType.isEnum()) {
                            sb.append("'"+fieldValue+"'");
                        }
                        else if(fieldType.isPrimitive() || Number.class.isAssignableFrom(fieldType)){
                            sb.append(fieldValue);
                        }else{
                            String value = JacksonUtil.obj2Json(fieldValue).replaceAll("'", "''").replaceAll("\\\\", "\\\\\\\\");
                            sb.append("'"+value+"'");
                        }
                    }else{
                        if (!ignoreNull) {
                            if(sb.length() > 0) {
                                sb.append(", ");
                            }
                            sb.append(CamelUnderScore.underscore(field.getName()));
                            sb.append("=");
                            sb.append("null");
                        }
                    }
                }
            }
        });
        String table = CamelUnderScore.underscore(obj.getClass().getSimpleName());
        Object id = ReflectionUtil.getFieldValue(obj, idFieldName);
        sb.append(" where ").append(CamelUnderScore.underscore(idFieldName)).append("='").append(id).append("'");
        return "update " + table + " set " + sb.toString();
    }
    
    public static String getDeleteStatement(Object obj, String idFieldName) throws Exception {
        final StringBuilder sb = new StringBuilder();
        String table = CamelUnderScore.underscore(obj.getClass().getSimpleName());
        Object id = ReflectionUtil.getFieldValue(obj, idFieldName);
        sb.append("delete from "+table);
        sb.append(" where ").append(CamelUnderScore.underscore(idFieldName)).append("='").append(id).append("'");
        return sb.toString();
    }
    
    public static String getDeleteStatement(Class<?> clazz, String idFieldName, String in){
        String table = CamelUnderScore.underscore(clazz.getSimpleName());
        String field = CamelUnderScore.underscore(idFieldName);
        return "delete from "+table+" where "+field+" = '"+in+"'";
    }

    public static String getDeleteStatementIsn(Class<?> clazz, String idFieldName, String in){
        String table = CamelUnderScore.underscore(clazz.getSimpleName());
        String field = CamelUnderScore.underscore(idFieldName);
        return "delete from "+table+" where "+field+" in ("+in+")";
    }
    public static String getInsertStatement(Collection<?> list) throws Exception {
        return getInsertStatement(list, false);
    }
    
    public static String getInsertIgnoreStatement(Collection<?> list) throws Exception {
        return getInsertStatement(list, true);
    }
    
    public static String getInsertHeader(Object obj) throws Exception {
        return getInsertHeader(obj, false);
    }

    public static String join(String[] ids) {
        return join(",",ids);
    }

    public static String join(Collection<?> ids) {
        return join(",",ids);
    }
    
    public static String join(String delimeter, Object[] ids){
        StringBuffer sb = new StringBuffer(ids.length * 8);
        for (Object id : ids) {
        	if(id instanceof String)
        		sb.append(delimeter).append("'").append(id).append("'");
        	else
        		sb.append(delimeter).append(id);
        }
        return sb.substring(ids.length > 0 ? delimeter.length() : 0);
    }
    
    public static String join(String delimeter, Collection<?> ids){
        StringBuffer sb = new StringBuffer();
        for (Object id : ids) {
        	if(id instanceof String)
        		sb.append(delimeter).append("'").append(id).append("'");
        	else
        		sb.append(delimeter).append(id);
        }
        return sb.substring(ids.size() > 0 ? delimeter.length() : 0);
    }
    
    public static <T> T resultSetToEntity(Class<T> clazz, ResultSet rs){
        T t = null;
        try{
            final HashMap<String,String> rowVals = new HashMap<String,String>();
            ResultSetMetaData rsmd = rs.getMetaData();
            for(int i = 1; i<= rsmd.getColumnCount(); i++){
                rowVals.put(rsmd.getColumnLabel(i),rs.getString(i));
            }
            t = clazz.newInstance();
            ReflectionUtil.iterateFields(t, new FieldFoundCallback() {
                @Override
                public void field(Object obj, Field field) throws Exception {
                    field.setAccessible(true);
                    String name = CamelUnderScore.underscore(field.getName());
                    if(!Modifier.isStatic(field.getModifiers())){
                        String fieldValue = rowVals.get(name);
                        if(fieldValue != null){
                        	try{
	                        	if(ReflectionUtil.isPrimeType(field.getType())){
	                                field.set(obj, ReflectionUtil.convert(fieldValue, field.getType()));
	                        	}else{
	                        		if(Collection.class.isAssignableFrom(field.getType())){
	                        			ParameterizedType type = (ParameterizedType) field.getGenericType();
										JavaType javaType = JacksonUtil.getObjectMapper().getTypeFactory().constructCollectionType((Class<? extends Collection>) type.getRawType(), (Class<?>) type.getActualTypeArguments()[0]);
		                                field.set(obj, JacksonUtil.getObjectMapper().readValue(fieldValue, javaType));
	                        		}else if(field.getType().equals(Map.class)){
	                        			ParameterizedType type = (ParameterizedType) field.getGenericType();
	                        			JavaType javaType = JacksonUtil.getObjectMapper().getTypeFactory().constructMapType((Class<? extends Map>) type.getRawType(), (Class<?>) type.getActualTypeArguments()[0], (Class<?>) type.getActualTypeArguments()[1]);
	                        			field.set(obj, JacksonUtil.getObjectMapper().readValue(fieldValue, javaType));
	                        		}else{
	                        			field.set(obj, JacksonUtil.json2Object(fieldValue, field.getType()));
	                        		}
	                        	}
                            }catch(Exception e){
                                e.printStackTrace();
                            }
                        }
                    }
                }
            });
        }catch(Exception e){
            throw new RuntimeException(e);
        }
        return t;
    }
}
