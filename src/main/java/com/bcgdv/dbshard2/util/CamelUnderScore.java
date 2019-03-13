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

package com.bcgdv.dbshard2.util;

public class CamelUnderScore {
    public static String toCamelCase(String underscored) {
        StringBuilder sb = new StringBuilder();
        String[] items = underscored.split("_");
        for(int i=0; i<items.length; i++) {
            if(i==0) {
                sb.append(items[i]);
            }
            else {
                sb.append(items[i].substring(0, 1).toUpperCase()).append(items[i].substring(1));
            }
        }
        return sb.toString();
    }
    
    public static String underscore(String camel) {
        StringBuilder sb = new StringBuilder();
        for(int i=0; i<camel.length(); i++) {
            char c = camel.charAt(i);
            if(Character.isUpperCase(c)) {
                if(i > 0) {
                    sb.append("_");
                }
                sb.append(Character.toLowerCase(c));
            }
            else {
                sb.append(c);
            }
        }
        return sb.toString();
    }
    
    public static String upperCaseFirst(String spaced) {
    	if(spaced == null) return null;
        spaced = spaced.toLowerCase();
        StringBuilder sb = new StringBuilder();
        String[] items = spaced.split(" ");
        for(int i=0; i<items.length; i++) {
            if(items[i].length() == 0 )
                sb.append(" ");
            else
                sb.append(items[i].substring(0, 1).toUpperCase()).append(items[i].substring(1));
            sb.append(" ");
        }
        return sb.substring(0, sb.length() - 1);
    }
    
    public static String words(String s) {
    	if(s.contains(" "))
    		return upperCaseFirst(s);
    	if(s.contains("_")) {
    		s = s.replaceAll("_", " ");
    		return upperCaseFirst(s);
    	}
    	s = underscore(s);
		s = s.replaceAll("_", " ");
		return upperCaseFirst(s);
    }
    
    public static void main(String[] args) {
        System.out.println(words("cabDefKhjfd"));
    }
}
