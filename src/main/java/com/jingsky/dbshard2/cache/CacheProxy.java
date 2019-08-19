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

package com.jingsky.dbshard2.cache;

import java.util.Collection;
import java.util.Map;

public interface CacheProxy { 
    CacheType getType();
    
    Object get(String key);
    Object get(String key, int expiration);
    Object getAndTouch(String key, int expiration);
    Map<String, Object> getBulk(Collection<String> keys);
    
    <T> T getObject(String key);
    <T> T getObject(String key, int expiration);
    <T> T getAndTouchObject(String key, int expiration);
    <T> Map<String, T> getBulkObjects(Collection<String> keys);
    
    void set(String key, Object value);
    void set(String key, Object value, int expiration);
    void setBulk(Map<String, Object> entities);
    void setBulk(Map<String, Object> entities, int expiration);

    void delete(String key);
    void deleteBulk(Collection<String> keys);
    
    long incr(String key, long delta, int expiration);
    long decr(String key, long delta, int expiration);
}
