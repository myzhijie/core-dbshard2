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

package com.bcgdv.dbshard2.util.reflection;

import java.lang.annotation.Annotation;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

public class ReflectionUtil {
    public static <T> T copy(Class<T> clazz, Object from) {
        return copy(clazz, from, false);
    }
    
    public static <T> T copy(Class<T> clazz, Object from, boolean primeOnly) {
        try {
            Object to = clazz.newInstance();
            copy(to, from, primeOnly);
            return (T) to;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void copy(final Object to, final Object from) {
        copy(to, from, false);
    }

    public static void copy(final Object to, final Object from,
            final boolean primeOnly) {
        if ((from == null) || (to == null)) {
            return;
        }
        try {
            iterateFields(to, new FieldFoundCallback() {
                public void field(Object o, Field field) throws Exception {
                    Class<?> fieldType = field.getType();
                    if (!isPrimeType(fieldType) && primeOnly) {
                        return;
                    }
                    Method setter = getSetter(to.getClass(), field.getName(),
                            fieldType);
                    Method getter = getGetter(from.getClass(), field.getName(), fieldType);
                    if (setter == null || getter == null) {
                    	Field fromField = from.getClass().getField(field.getName());
                    	field.set(to, fromField.get(from));
                    }
                    else {
	                    Object fromFieldValue = getter.invoke(from, null);
	                    if (isPrimeType(fieldType)) {
	                        setter.invoke(to, fromFieldValue);
	                    } 
	                    else if (List.class.isAssignableFrom(fieldType) || Map.class.isAssignableFrom(fieldType)){
	                        setter.invoke(to, fromFieldValue);
	                    }
	                    else {
	                        Object toFieldValue = copy(fieldType, fromFieldValue,
	                                primeOnly);
	                        setter.invoke(to, toFieldValue);
	                    }
                    }
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static void iterateClassTree(Class<?> clazz, ClassCallback callback)
            throws Exception {
        while (true) {
            if (clazz == null || clazz.equals(Object.class)) {
                break;
            }
            callback.classFound(clazz);
            clazz = clazz.getSuperclass();
        }
    }

    public static void iterateFields(final Object o,
            final FieldFoundCallback callback) throws Exception {
        iterateFields(o.getClass(), o, callback);
    }

    public static void iterateFields(final Class<?> clazz, final Object o,
            final FieldFoundCallback callback) throws Exception {
        iterateClassTree(clazz, new ClassCallback() {
            public void classFound(Class<?> cls) throws Exception {
                for (Field field : cls.getDeclaredFields()) {
                    callback.field(o, field);
                }
            }
        });
    }

    public static void iterateMethods(final Object o,
            final MethodFoundCallback callback) throws Exception {
        iterateMethods(o.getClass(), o, callback);
    }

    private static void iterateMethods(final Class<?> clazz, final Object o,
            final MethodFoundCallback callback) throws Exception {
        iterateClassTree(clazz, new ClassCallback() {
            public void classFound(Class<?> cls) {
                for (Method method : cls.getDeclaredMethods()) {
                    callback.method(o, method);
                }
            }
        });
    }

    public static void iterateAnnotation(final Object o,
            final Class<?> annoClass, final AnnotationFoundCallback callback)
            throws Exception {
        iterateAnnotatedFields(o, annoClass, callback);
        iterateAnnotatedMethods(o, annoClass, callback);
    }

    public static void iterateAnnotatedFields(final Object o,
            final Class<?> annoClass, final AnnotatedFieldCallback callback)
            throws Exception {
        iterateAnnotatedFields(o.getClass(), o, annoClass, callback);
    }

    public static void iterateAnnotatedMethods(final Object o,
            final Class<?> annoClass, final AnnotatedMethodCallback callback)
            throws Exception {
        iterateAnnotatedMethods(o.getClass(), o, annoClass, callback);
    }

    public static void iterateAnnotatedFields(final Object o,
            final Class<?> annoClass, final Class<?> fieldType,
            final AnnotatedFieldCallback callback) throws Exception {
        iterateAnnotatedFields(o.getClass(), o, annoClass, fieldType, callback);
    }

    public static void iterateAnnotatedFields(final Class<?> clazz,
            final Object o, final Class<?> annoClass,
            final AnnotatedFieldCallback callback) throws Exception {
        iterateClassTree(clazz, new ClassCallback() {
            public void classFound(Class<?> cls) throws Exception {
                for (Field field : cls.getDeclaredFields()) {
                    for (Annotation fieldAnnot : field.getAnnotations()) {
                        if (fieldAnnot.annotationType().equals(annoClass)) {
                            callback.field(o, field);
                            break;
                        }
                    }
                }
            }
        });
    }

    private static void iterateAnnotatedFields(final Class<?> clazz,
            final Object o, final Class<?> annoClass, final Class<?> fieldType,
            final AnnotatedFieldCallback callback) throws Exception {
        iterateClassTree(clazz, new ClassCallback() {
            public void classFound(Class<?> cls) throws Exception {
                for (Field field : cls.getDeclaredFields()) {
                    Class<?> type = field.getType();
                    if (!fieldType.isAssignableFrom(type)) {
                        continue;
                    }
                    for (Annotation fieldAnnot : field.getAnnotations()) {
                        if (!fieldAnnot.equals(annoClass)) {
                            continue;
                        }
                    }
                    callback.field(o, field);
                }
            }
        });
    }

    private static void iterateAnnotatedMethods(final Class<?> clazz,
            final Object o, final Class<?> annoClass,
            final AnnotatedMethodCallback callback) throws Exception {
        iterateClassTree(clazz, new ClassCallback() {
            public void classFound(Class<?> cls) throws Exception {
                for (Method method : cls.getDeclaredMethods()) {
                    for (Annotation methodAnnot : method.getAnnotations()) {
                        if (methodAnnot.annotationType().equals(annoClass)) {
                            callback.method(o, method);
                            break;
                        }
                    }
                }
            }
        });
    }

    public static void setAnnotatedFields(final Object o,
            final Class<?> annoClass, final Object fieldValue) throws Exception {
        iterateAnnotatedFields(o, annoClass, new AnnotatedFieldCallback() {
            public void field(Object obj, Field field)
                    throws IllegalArgumentException, IllegalAccessException {
                field.setAccessible(true);
                field.set(obj, fieldValue);
            }
        });
    }
    
    public static AnnotatedField<?> getAnnotatedField(final Object o, final Class<?> annoClass) throws Exception {
        return getAnnotatedField(o, annoClass, Object.class);
    }
    
    public static <T> AnnotatedField<T> getAnnotatedField(final Object o, final Class<?> annoClass, Class<T> clazz) throws Exception {
        AnnotatedField<T> raf = new AnnotatedField<T>();
        iterateAnnotatedFields(o, annoClass, raf);
        return raf;
    }

    public static Field getAnnotatedField(final Class<?> annoClass, Class<?> clazz) throws Exception {
        final ArrayList<Field> holder = new ArrayList<Field>();
        iterateAnnotatedFields(clazz, (Object) null, annoClass, new AnnotatedFieldCallback() {
            @Override
            public void field(Object o, Field field) throws Exception {
                field.setAccessible(true);
                holder.add(field);
            }
        });
        return holder.get(0);
    }

    public static void setAnnotatedFields(final Object o,
            final Class<?> annoClass, final Class<?> fieldType,
            final Object fieldValue) throws Exception {
        iterateAnnotatedFields(o, annoClass, fieldType,
                new AnnotatedFieldCallback() {
                    public void field(Object obj, Field field)
                            throws IllegalArgumentException,
                            IllegalAccessException {
                        field.setAccessible(true);
                        field.set(obj, fieldValue);
                    }
                });
    }

    public static Class getParameterizedType(Class clazz) {
        return getParameterizedType(clazz, 0);
    }

    public static Class getParameterizedType(Class clazz, int index) {
        Type superclass = clazz.getGenericSuperclass();
		try {
			ParameterizedType s = (ParameterizedType) superclass;
			return (Class) s.getActualTypeArguments()[index];
		} catch (Exception e) {
			if(clazz.equals(Object.class))
				return null;
			return getParameterizedType(clazz.getSuperclass(), index);
		}
    }

    public static Class getFieldGenericType(Class c, String fieldName)
            throws Exception {
        Field f = c.getDeclaredField(fieldName);
        return getFieldGenericType(f);
    }

    public static Class getFieldGenericType(Field f) throws Exception {
        ParameterizedType gt = (ParameterizedType) f.getGenericType();
        return (Class) gt.getActualTypeArguments()[0];
    }

    public static Class getFieldGenericType(Field f, int index) throws Exception {
        ParameterizedType gt = (ParameterizedType) f.getGenericType();
        return (Class) gt.getActualTypeArguments()[index];
    }

    public static Method getMethod(Class cls, final String name,
            final Class<?>... parmTypes) throws Exception {
        final ArrayList<Method> holder = new ArrayList<Method>();
        iterateClassTree(cls, new ClassCallback() {

            public void classFound(Class<?> clazz) throws Exception {
                try {
                    Method method = clazz.getMethod(name, parmTypes);
                    holder.add(method);
                } catch (Exception e) {
                    // don't remove try...catch
                }
            }
        });

        if (holder.size() == 0) {
            return null;
        }
        return holder.get(0);
    }

    public static boolean annotatedWith(Method method, Class annoCls) {
        try {
            Annotation annotation = method.getAnnotation(annoCls);
            return annotation != null;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean annotatedWith(Field field, Class annoCls) {
        try {
            Annotation annotation = field.getAnnotation(annoCls);
            return annotation != null;
        } catch (Exception e) {
            return false;
        }
    }

    public static boolean annotatedWith(Class<?> target, Class annoCls) {
        try {
            Annotation annotation = target.getAnnotation(annoCls);
            return annotation != null;
        } catch (Exception e) {
            return false;
        }
    }

    public static Annotation getAnnotation(Class<?> target, Class annoCls) {
        try {
            Annotation annotation = target.getAnnotation(annoCls);
            return annotation;
        } catch (Exception e) {
            return null;
        }
    }

    public static Field getField(Class clazz, final String fieldName)
            throws Exception {
        final ArrayList<Field> holder = new ArrayList<Field>();
        iterateClassTree(clazz, new ClassCallback() {

            public void classFound(Class<?> clazz) throws Exception {
                try {
                    Field field = clazz.getDeclaredField(fieldName);
                    holder.add(field);
                } catch (Exception e) {
                    // don't remove this try...catch block.
                }
            }
        });
        if (holder.size() == 0) {
            return null;
        }
        return holder.get(0);
    }

    public static Method getMethod(Class clazz, final String methodName,
            final Class<?> parameterTypes) throws Exception {
        final ArrayList<Method> holder = new ArrayList<Method>();
        iterateClassTree(clazz, new ClassCallback() {
            public void classFound(Class<?> clazz) throws Exception {
                try {
                    Method method = clazz.getDeclaredMethod(methodName,
                            parameterTypes);
                    holder.add(method);
                } catch (Exception e) {
                    // don't remove this try...catch block.
                }
            }
        });
        if (holder.size() == 0) {
            return null;
        }
        return holder.get(0);
    }

    public static Method getSetter(Class clazz, final String fieldName,
            final Class<?> fieldType) throws Exception {
        final String methodName = "set"
                + fieldName.substring(0, 1).toUpperCase()
                + fieldName.substring(1);
        return getMethod(clazz, methodName, fieldType);
    }

    public static Method getGetter(Class clazz, final String fieldName, final Class<?> fieldType)
            throws Exception {
        String prefix = boolean.class.equals(fieldType) ? "is" : "get";
        
        String methodName = prefix + fieldName.substring(0, 1).toUpperCase()
                + fieldName.substring(1);
        return getMethod(clazz, methodName, new Class<?>[0]);
    }

    public static void setField(Object target, String fieldName,
            Object fieldValue) throws Exception {
        Field field = getField(target.getClass(), fieldName);
        field.setAccessible(true);
        field.set(target, fieldValue);
    }

    public static void callSetter(Object target, String fieldName,
            Object fieldValue) throws Exception {
        Method setter = getSetter(target.getClass(), fieldName,
                fieldValue.getClass());
        setter.invoke(target, fieldValue);
    }

    public static void callSetter(Object target, String fieldName,
            Object fieldValue, Class<?> fieldType) throws Exception {
        Method setter = getSetter(target.getClass(), fieldName, fieldType);
        setter.invoke(target, fieldValue);
    }

    public static void callSetter(Object target, String fieldName,
            String fieldValue) throws Exception {
        Field field = getField(target.getClass(), fieldName);
        if (field == null) {
            return;
        }
        Class<?> fieldType = field.getType();
        if (isPrimeType(fieldType)) {
            Object value = convert(fieldValue, fieldType);
            callSetter(target, fieldName, value, fieldType);
        } else if (fieldType.isEnum()) {
            for (Object o : fieldType.getEnumConstants()) {
                Enum<?> c = (Enum<?>) o;
                if (c.name().equals(fieldValue)) {
                    callSetter(target, fieldName, c);
                }
            }
        } else {
            Method fromString = fieldType.getDeclaredMethod("fromString",
                    String.class);
            Object value = fromString.invoke(fieldType.newInstance(),
                    fieldValue);
            callSetter(target, fieldName, value);
        }
    }

    public static Object callGetter(Object target, String fieldName)
            throws Exception {
        Class<?> type = target.getClass().getDeclaredField(fieldName).getType();
        Method getter = getGetter(target.getClass(), fieldName, type);
        return getter.invoke(target, new Object[0]);
    }

    public static boolean isPrimeField(Field field) {
        return isPrimeType(field.getType());
    }

    public static boolean isPrimeType(Class type) {
        if (type.equals(String.class)) {
            return true;
        }
        if (type.equals(Integer.class) || type.equals(int.class)) {
            return true;
        }
        if (type.equals(Float.class) || type.equals(float.class)) {
            return true;
        }
        if (type.equals(Double.class) || type.equals(double.class)) {
            return true;
        }
        if (type.equals(Long.class) || type.equals(long.class)) {
            return true;
        }
        if (type.equals(Boolean.class) || type.equals(boolean.class)) {
            return true;
        }
        if (type.equals(Date.class)) {
            return true;
        }
        if (type.equals(Calendar.class)) {
            return true;
        }
        if (type.equals(BigInteger.class)) {
            return true;
        }
        if (type.equals(BigDecimal.class)) {
            return true;
        }
        if(type.isEnum()) {
            return true;
        }

        return false;
    }

    public static Object convert(String value, Class toType) throws Exception {
        if (value == null) {
            return null;
        }
        if (toType.equals(String.class)) {
            return value;
        }
        if (toType.equals(Integer.class) || toType.equals(int.class)) {
            return Integer.parseInt(value);
        }
        if (toType.equals(Float.class) || toType.equals(float.class)) {
            return Float.parseFloat(value);
        }
        if (toType.equals(Double.class) || toType.equals(double.class)) {
            return Double.parseDouble(value);
        }
        if (toType.equals(Long.class) || toType.equals(long.class)) {
            return Long.parseLong(value);
        }
        if (toType.equals(BigInteger.class)) {
            return new BigInteger(value);
        }
        if (toType.equals(BigDecimal.class)) {
            return new BigDecimal(value);
        }
        if (toType.equals(Boolean.class) || toType.equals(boolean.class)) {
            value = value.toLowerCase();
            if (value.equals("1") || value.startsWith("t")
                    || value.startsWith("y") || value.equalsIgnoreCase("on")) {
                return Boolean.TRUE;
            } else {
                return Boolean.FALSE;
            }
        }
        if (toType.equals(Date.class)) {
            try {
                Long millisecond = Long.parseLong(value);
                if(millisecond == 0)
                    return null;
                return new Date(millisecond);
            } catch (Exception e) {
                SimpleDateFormat sdf = getIso8601DateFormat();
                return sdf.parse(value);
            }
        }
        if (toType.equals(Calendar.class)) {
            Long millisecond = Long.parseLong(value);
            if(millisecond == 0)
                return null;
            Calendar cal = Calendar.getInstance();
            cal.setTimeInMillis(millisecond);
            return cal;
        }
        if (toType.isEnum()) {
            for (Object e : toType.getEnumConstants()) {
                if (e.toString().equals(value))
                    return e;
            }
        }
        throw new Exception("Unhandled data type: " + toType);
    }

    public static SimpleDateFormat getIso8601DateFormat() {
        final SimpleDateFormat ISO8601UTC = new SimpleDateFormat(
                "yyyy-MM-dd'T'HH:mm:ss.SSSZ");// 24
        // characters
        ISO8601UTC.setTimeZone(TimeZone.getTimeZone("UTC")); // UTC == GMT
        return ISO8601UTC;
    }

    public static boolean isPrimeType(Object item) {
        return isPrimeType(item.getClass());
    }

    public static void setFieldValue(Object target, Field field, Object value) {
        try {
            field.setAccessible(true);
            if(value == null) {
                field.set(target, null);
            }
            else if(field.getType().isAssignableFrom(value.getClass())) {
                field.set(target, value);
            }
            else if (isPrimeField(field)){
                Object convert = convert(value.toString(), field.getType());
                field.set(target, convert);
            }
            else {
                throw new Exception("cannot set field value " + field.getName() + " for " + target.getClass());
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static String primeString(Object primeObject) {
        if(primeObject == null)
            return null;
        if(primeObject instanceof Date) {
            return getIso8601DateFormat().format((Date)primeObject);
        }
        if(primeObject instanceof Calendar) {
            return getIso8601DateFormat().format(((Calendar)primeObject).getTime());
        }
        return primeObject.toString();
    }

    public static Object getFieldValue(Object obj, String fieldName) throws Exception {
        Field field = getField(obj.getClass(), fieldName);
        field.setAccessible(true);
        return field.get(obj);
    }
    
	public static List getFieldValues(List objs, String fieldName) {
		List values = new ArrayList();
		if(objs == null || objs.size() == 0)
			return values;
		Class cls = objs.get(0).getClass();
		try {
			Field field = cls.getField(fieldName);
			for(Object obj : objs) {
				values.add(field.get(obj));
			}
		} catch (Exception e) {
			throw new RuntimeException(e);
		}
		return values;
	}

    public static void copyNonNullFields(final Object to, final Object from) {
    	copyNonNullFields(to, from, Collections.EMPTY_LIST);
    }

    public static void copyNonNullFields(final Object to, final Object from, final List<String>skipFields) {
        if ((from == null) || (to == null)) {
            return;
        }
        try {
            iterateFields(to, new FieldFoundCallback() {
                public void field(Object o, Field field) throws Exception {
                	if (java.lang.reflect.Modifier.isStatic(field.getModifiers()))
                			return;
                	if(skipFields != null && skipFields.contains(field.getName()))
                		return;
                    Object fromFieldValue = field.get(from);
                    if(fromFieldValue == null)
                        return;
                    if (isPrimeType(field.getType())) {
                        field.setAccessible(true);
                    	if(fromFieldValue.equals("")) {
                    		field.set(to, null);
                    	}
                    	else {
                    		field.set(to, fromFieldValue);
                    	}
                    }
                    else if (List.class.isAssignableFrom(field.getType()) || Map.class.isAssignableFrom(field.getType())){
                        field.setAccessible(true);
                        field.set(to, fromFieldValue);
                    }
                    else {
                        Object toFieldValue = field.get(to);
                        if(toFieldValue == null) {
                        	field.set(to, fromFieldValue);
                        	return;
                        }
                        copyNonNullFields(toFieldValue, fromFieldValue, skipFields);
                    }
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
    
    public static Object setEmptyStringToNull(Object obj) {
        if(obj == null)
            return null;
        
        Class objType = obj.getClass();
        if(String.class.equals(objType)) {
            String s = (String) obj;
            s = s.trim();
            if(s.length() == 0)
                return null;
            return s;
        }
        
        if(ReflectionUtil.isPrimeType(obj))
            return obj;
        
        if (List.class.isAssignableFrom(objType)){
            List list = (List) obj;
            for(int i=0; i<list.size(); i++) {
                Object child = list.get(i);
                child = setEmptyStringToNull(child);
                list.set(i, child);
            }
            return obj;
        }
        else if (Map.class.isAssignableFrom(objType)){
            Map map = (Map) obj;
            for(Object key : map.keySet().toArray()) {
                Object child = map.get(key);
                child = setEmptyStringToNull(child);
                map.put(key, child);
            }
            return obj;
        }
        
        try {
            ReflectionUtil.iterateFields(obj, new FieldFoundCallback() {
                @Override
                public void field(Object o, Field field) throws Exception {
                    field.setAccessible(true);
                    Object value = field.get(o);
                    if(value == null)
                        return;
                    Class<?> type = field.getType();
                    if(String.class.equals(type)) {
                        String s = (String) value;
                        s = s.trim();
                        if(s.length() == 0)
                            field.set(o, null);
                    }
                    if(ReflectionUtil.isPrimeType(type))
                        return;
                    else if (type.isAssignableFrom(List.class)){
                        List list = (List) value;
                        for(Object child : list) {
                            setEmptyStringToNull(child);
                        }
                    }
                    else if (type.isAssignableFrom(Map.class)){
                        Map map = (Map) value;
                        for(Object key : map.keySet().toArray()) {
                            Object child = map.get(key);
                            child = setEmptyStringToNull(child);
                            map.put(key, child);
                        }
                    }
                    else {
                        setEmptyStringToNull(value);
                    }
                }
            });
            return obj;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }
}
