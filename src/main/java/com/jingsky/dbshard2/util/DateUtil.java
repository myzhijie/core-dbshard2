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

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

public class DateUtil {
    public static long DAY = 86400000l;
    public static int DAY_SECOND = 86400;
    public static int DAYS_30 = 2592000;
    public static int ONE_HOUR = 3600;
    public static Map<String, Integer> months = new HashMap<String, Integer>(){{
        put("JANUARY", 1);
        put("JAN", 1);
        put("FEBRUARY", 2);
        put("FEB", 2);
        put("MARCH", 3);
        put("MAR", 3);
        put("APRIL", 4);
        put("APR", 4);
        put("MAY", 5);
        put("MAY", 5);
        put("JUNE", 6);
        put("JUN", 6);
        put("JULY", 7);
        put("JUL", 7);
        put("AUGUST", 8);
        put("AUG", 8);
        put("SEPTEMBER", 9);
        put("SEP", 9);
        put("SEPT", 9);
        put("OCTOBER", 10);
        put("OCT", 10);
        put("NOVEMBER", 11);
        put("NOV", 11);
        put("DECEMBER", 12);
        put("DEC", 12);
    }};
	private static SimpleDateFormat ISO8601UTC = null;
	private static SimpleDateFormat simpleDateFormat = null;
    
	public static SimpleDateFormat getIso8601DateFormat() {
		if(ISO8601UTC == null) {
			ISO8601UTC = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");// 24
			ISO8601UTC.setTimeZone(TimeZone.getTimeZone("UTC")); // UTC == GMT
		}
		return ISO8601UTC;
	}
	
	public static SimpleDateFormat getSimpleDateFormat() {
		if(simpleDateFormat == null) {
			simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSZ");// 24
			simpleDateFormat.setTimeZone(TimeZone.getTimeZone("UTC"));
		}
		return simpleDateFormat;
	}
	
	public static String getSimpleDateString(long millis) {
		return getSimpleDateFormat().format(new Date(millis));
	}
	
	public static SimpleDateFormat getYmdUtcDateFormat() {
		final SimpleDateFormat ISO8601UTC = new SimpleDateFormat("yyyy-MM-dd");// 24
		ISO8601UTC.setTimeZone(TimeZone.getTimeZone("UTC")); // UTC == GMT
		return ISO8601UTC;
	}
	
	public static DateFormat getDateFormat(String format) {
		return new SimpleDateFormat(format);
	}
	
	public static Calendar getCurrentUtcTime() {
		Calendar instance = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		instance.setTimeInMillis(currentTimeMillis());
		return instance;
	}

	public static Long parse(String s) {
	    if(s == null)
	        return null;
	    s = s.toUpperCase().trim();
	    if(s.length() == 0)
	        return null;
	    s.replaceAll("[-/ \\.]+", " ");
	    String[] item = s.split(" ");
	    
	    Integer year = null;
        try {
            year = Integer.parseInt(item[0]);
        }
        catch (Exception e) {
        }
        
        Integer month = null;
        Integer day = null;
        if(year != null) {
            try {
                month = Integer.parseInt(item[1]);
            }
            catch (Exception e) {
                month = months.get(item[1]);
            }
            if(month == null)
                throw new RuntimeException("unknow date format");
        }
        
        return null;
	}
	
	public static long firstDayOfNextMonth() {
		Calendar aCalendar = getCurrentUtcTime();
		aCalendar.add(Calendar.MONTH, 1);
		aCalendar.set(Calendar.DATE, 1);
		aCalendar.set(Calendar.HOUR_OF_DAY, 0);
		aCalendar.set(Calendar.MINUTE, 0);
		aCalendar.set(Calendar.SECOND, 0);
		aCalendar.set(Calendar.MILLISECOND, 0);
		return aCalendar.getTime().getTime();
	}
	
	public static long firstDayOfNextMonth(long date) {
		Calendar aCalendar = getCurrentUtcTime();
		aCalendar.setTimeInMillis(date);
		aCalendar.add(Calendar.MONTH, 1);
		aCalendar.set(Calendar.DATE, 1);
		aCalendar.set(Calendar.HOUR_OF_DAY, 0);
		aCalendar.set(Calendar.MINUTE, 0);
		aCalendar.set(Calendar.SECOND, 0);
		aCalendar.set(Calendar.MILLISECOND, 0);
		return aCalendar.getTime().getTime();
	}
	
	public static long firstDayOfNextYear() {
		Calendar aCalendar = getCurrentUtcTime();
		aCalendar.add(Calendar.YEAR, 1);
		aCalendar.set(Calendar.MONTH, 0);
		aCalendar.set(Calendar.DATE, 1);
		aCalendar.set(Calendar.HOUR_OF_DAY, 0);
		aCalendar.set(Calendar.MINUTE, 0);
		aCalendar.set(Calendar.SECOND, 0);
		aCalendar.set(Calendar.MILLISECOND, 0);
		return aCalendar.getTime().getTime();
	}
	
	public static long firstDayOfLastYear() {
		Calendar aCalendar = getCurrentUtcTime();
		aCalendar.add(Calendar.YEAR, -1);
		aCalendar.set(Calendar.MONTH, 0);
		aCalendar.set(Calendar.DATE, 1);
		aCalendar.set(Calendar.HOUR_OF_DAY, 0);
		aCalendar.set(Calendar.MINUTE, 0);
		aCalendar.set(Calendar.SECOND, 0);
		aCalendar.set(Calendar.MILLISECOND, 0);
		return aCalendar.getTime().getTime();
	}
	
	public static long firstDayOfCurrentMonth() {
		Calendar aCalendar = getCurrentUtcTime();
		aCalendar.set(Calendar.DATE, 1);
		aCalendar.set(Calendar.HOUR_OF_DAY, 0);
		aCalendar.set(Calendar.MINUTE, 0);
		aCalendar.set(Calendar.SECOND, 0);
		aCalendar.set(Calendar.MILLISECOND, 0);
		return aCalendar.getTime().getTime();
	}
	
	public static long firstDayOfCurrentYear() {
		Calendar aCalendar = getCurrentUtcTime();
		aCalendar.set(Calendar.MONTH, 0);
		aCalendar.set(Calendar.DATE, 1);
		aCalendar.set(Calendar.HOUR_OF_DAY, 0);
		aCalendar.set(Calendar.MINUTE, 0);
		aCalendar.set(Calendar.SECOND, 0);
		aCalendar.set(Calendar.MILLISECOND, 0);
		return aCalendar.getTime().getTime();
	}
	
	public static long firstDayOfLastMonth() {
		Calendar aCalendar = getCurrentUtcTime();
		aCalendar.add(Calendar.MONTH, -1);
		aCalendar.set(Calendar.DATE, 1);
		aCalendar.set(Calendar.HOUR_OF_DAY, 0);
		aCalendar.set(Calendar.MINUTE, 0);
		aCalendar.set(Calendar.SECOND, 0);
		aCalendar.set(Calendar.MILLISECOND, 0);
		return aCalendar.getTime().getTime();
	}
	
	public static long firstDayOfMonth(long date) {
		Calendar aCalendar = getCurrentUtcTime();
		aCalendar.setTimeInMillis(date);
		aCalendar.set(Calendar.DATE, 1);
		aCalendar.set(Calendar.HOUR_OF_DAY, 0);
		aCalendar.set(Calendar.MINUTE, 0);
		aCalendar.set(Calendar.SECOND, 0);
		aCalendar.set(Calendar.MILLISECOND, 0);
		return aCalendar.getTime().getTime();
	}
	
	public static long firstDayOfYear(long date) {
		Calendar aCalendar = getCurrentUtcTime();
		aCalendar.setTimeInMillis(date);
		aCalendar.set(Calendar.MONTH, 0);
		aCalendar.set(Calendar.DATE, 1);
		aCalendar.set(Calendar.HOUR_OF_DAY, 0);
		aCalendar.set(Calendar.MINUTE, 0);
		aCalendar.set(Calendar.SECOND, 0);
		aCalendar.set(Calendar.MILLISECOND, 0);
		return aCalendar.getTime().getTime();
	}
	
	public static Calendar firstDayOfMonth(int year, int month) {
		Calendar aCalendar = getCurrentUtcTime();
		aCalendar.set(Calendar.YEAR, year);
		aCalendar.set(Calendar.MONTH, month-1);
		aCalendar.set(Calendar.DATE, 1);
		aCalendar.set(Calendar.HOUR_OF_DAY, 0);
		aCalendar.set(Calendar.MINUTE, 0);
		aCalendar.set(Calendar.SECOND, 0);
		aCalendar.set(Calendar.MILLISECOND, 0);
		return aCalendar;
	}

	public static Calendar dayOfCurrentMonth(int day) {
		Calendar aCalendar = getCurrentUtcTime();
		aCalendar.set(Calendar.DAY_OF_MONTH, day);
		aCalendar.set(Calendar.HOUR_OF_DAY, 0);
		aCalendar.set(Calendar.MINUTE, 0);
		aCalendar.set(Calendar.SECOND, 0);
		aCalendar.set(Calendar.MILLISECOND, 0);
		return aCalendar;
	}

	public static Calendar dayOfMonth(int year, int month, int day) {
		Calendar aCalendar = getCurrentUtcTime();
		aCalendar.set(Calendar.YEAR, year);
		aCalendar.set(Calendar.MONTH, month-1);
		aCalendar.set(Calendar.DAY_OF_MONTH, day);
		aCalendar.set(Calendar.HOUR_OF_DAY, 0);
		aCalendar.set(Calendar.MINUTE, 0);
		aCalendar.set(Calendar.SECOND, 0);
		aCalendar.set(Calendar.MILLISECOND, 0);
		return aCalendar;
	}

	public static Calendar dayOfPreviousMonth(int year, int month, int day) {
		Calendar aCalendar = getCurrentUtcTime();
		aCalendar.set(Calendar.YEAR, year);
		aCalendar.set(Calendar.MONTH, month-1);
		aCalendar.set(Calendar.DAY_OF_MONTH, day);
		aCalendar.set(Calendar.HOUR_OF_DAY, 0);
		aCalendar.set(Calendar.MINUTE, 0);
		aCalendar.set(Calendar.SECOND, 0);
		aCalendar.set(Calendar.MILLISECOND, 0);
		aCalendar.add(Calendar.MONTH, -1);
		return aCalendar;
	}

	public static Calendar getCalendarInstance() {
		//Calendar cal = Calendar.getInstance(TimeZone.getTimeZone("UTC"));
		  Calendar cal = Calendar.getInstance();
		return cal;
	}
	
	private static long currentTimeMillisOffset = 0l;
	public static long currentTimeMillis() {
		return System.currentTimeMillis() + currentTimeMillisOffset;
	}
	
	public static void setCurrentTimeMillisOffset(long offset) {
		currentTimeMillisOffset = offset;
	}
}
