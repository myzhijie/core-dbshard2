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

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;

public class JacksonUtil {
    public static TypeReference<HashMap<String, Object>> getTypeRef() {
        TypeReference<HashMap<String, Object>> typeRef = new TypeReference<HashMap<String, Object>>() {
        };
        return typeRef;
    }
    
	public static String obj2Json(Object obj) {
		return obj2Json(obj, false);
	}
	
	public static String obj2Json(Object obj, boolean indent) {
	    if(obj == null)
	        return null;
		ObjectMapper mapper = getObjectMapper();
		if(indent)
			mapper.configure(SerializationFeature.INDENT_OUTPUT, true); 
		StringWriter sw = new StringWriter();
		try {
			mapper.writeValue(sw, obj);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return sw.toString();
	}
	
	public static String toString(Object jsonObj, ObjectMapper mapper) {
		StringWriter sw = new StringWriter();
		try {
			mapper.writeValue(sw, jsonObj);
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return sw.toString();
	}
	
	public static Object get(Object jsonObj, String... keys) {
		return get((HashMap<String, Object>)jsonObj, keys);
	}
	
	public static String getString(Object jsonObj, String... keys) {
		Object obj = get(jsonObj, keys);
		if(obj == null)
			return null;
		else
			return obj.toString();
	}
	
	public static Long getLong(Object jsonObj, String... keys) {
		return Long.parseLong(getString(jsonObj, keys));
	}
	
	public static Double getDouble(Object jsonObj, String... keys) {
		return Double.parseDouble(getString(jsonObj, keys));
	}
	
	public static Object get(HashMap<String, Object> jsonObj, String... keys) {
		Object ret = jsonObj;
		for(String s : keys) {
			ret = ((HashMap<String, Object>)ret).get(s);
		}
		return ret;
	}
	
	public static List<Object> getList(Object jsonObj, String... keys) {
		return (List<Object>) get(jsonObj, keys);
	}

    public static <T> T json2Object(String str, Class<T> clazz) throws JsonParseException, JsonMappingException, IOException {
        StringReader sr = new StringReader(str);
        return (T)getObjectMapper().readValue(sr, clazz);
    }

    public static <T> T json2Object(Reader reader, Class<T> clazz) throws JsonParseException, JsonMappingException, IOException {
        return (T)getObjectMapper().readValue(reader, clazz);
    }

    public static <T> T json2Object(ObjectMapper jsonProcessor, String str, Class<T> clazz) throws JsonParseException, JsonMappingException, IOException {
        return (T)jsonProcessor.readValue(str, clazz);
    }

    public static <T> List<T> json2List(String str, TypeReference<List<T>> typeRef) throws JsonParseException, JsonMappingException, IOException {
        StringReader sr = new StringReader(str);
        return getObjectMapper().readValue(sr, typeRef);
    }

    private static ThreadLocal<ObjectMapper> objMapper = new ThreadLocal<ObjectMapper>();
    public static ObjectMapper getObjectMapper(){
        ObjectMapper jsonProcessor = objMapper.get();
        if(jsonProcessor == null) {
            jsonProcessor = new ObjectMapper();
            jsonProcessor.setSerializationInclusion(Include.NON_NULL);
            jsonProcessor.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES,false);
            jsonProcessor.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL,true);
            jsonProcessor.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            objMapper.set(jsonProcessor);
        }
        return jsonProcessor;
    }
    
    public static HashMap<String, Object> json2map(Reader reader) {
        try {
            return getObjectMapper().readValue(reader, getTypeRef());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static HashMap<String, Object> json2map(String json) {
        try {
            return getObjectMapper().readValue(json, getTypeRef());
        }
        catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static Map<String, Object> getMap(Map<String, Object> map, String key) {
        return (Map<String, Object>) map.get(key);
    }

    public static List getList(Map<String, Object> map, String key) {
        return (List) map.get(key);
    }

	public static Map obj2Map(Object sa) {
		String json = obj2Json(sa);
		return json2map(json);
	}
}
