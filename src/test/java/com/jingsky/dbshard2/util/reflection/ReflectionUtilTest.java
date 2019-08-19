package com.jingsky.dbshard2.util.reflection;

import org.junit.Assert;
import org.junit.Test;

public class ReflectionUtilTest {
	@Test
	public void testCopyNonNullField() {
		User u1 = new User();
		u1.fname = "f1";
		u1.lname = "l1";
		u1.age1 = 12;
		u1.age2 = 10;
		
		User u2 = new User();
		u1.fname = "f2";
		
		ReflectionUtil.copyNonNullFields(u1, u2);
		Assert.assertEquals("f2", u1.fname);
		Assert.assertEquals("l1", u1.lname);
		Assert.assertEquals(0, u1.age1);
		Assert.assertEquals((Integer)10, u1.age2);
		
		u1.c = new C();
		u2.c = null;
		ReflectionUtil.copyNonNullFields(u2, u1);
		Assert.assertNotNull(u2.c);
	}
	
	public static class User {
		String fname;
		int age1;
		Integer age2;
		String lname;
		C c;
	}
	
	public static class C {
		String s1;
		String s2;
	}
}
