package com.semantico.cassandra.fs.tests;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.contrib.fs.FSConstants;
import org.apache.cassandra.contrib.fs.PathUtil;
import org.apache.cassandra.contrib.fs.util.Bytes;
import org.apache.cassandra.contrib.fs.util.Helper;
import org.junit.Test;
import static org.junit.Assert.*;

public class UtilTest {

	@Test
	public void helperIsNullOrEmptyTest() {
		assertTrue(Helper.isNullOrEmpty(" \n "));
		assertTrue(Helper.isNullOrEmpty(""));
		assertTrue(Helper.isNullOrEmpty(null));
		assertTrue(!Helper.isNullOrEmpty("blablalbal"));
	}
	
	@Test
	public void byteBufferToArrayTest() {
		ByteBuffer buffer = ByteBuffer.allocate(10);
		buffer.put((byte) 5);
		byte[] bufferContent = buffer.array();
		byte[] result = Bytes.toBytes(buffer);
		assertArrayEquals(bufferContent, result);
	}
	
	@Test
	public void bytesToStringTest() {
		String content = "some content string";
		byte[] bytes = content.getBytes();
		String result = Bytes.toString(bytes);
		assertEquals(content, result);
	}
	
	@Test
	public void twoByteArrayToStringTest() {
		String content1 = "foo";
		String content2 = "bar";
		byte[] bytes1 = content1.getBytes();
		byte[] bytes2 = content2.getBytes();
		String result = Bytes.toString(bytes1, " ", bytes2);
		assertEquals("foo bar", result);
	}
	
	@Test
	public void emptyToStringTest() {
		String result = Bytes.toString(new byte[0], 0, 0);
		assertEquals("", result);
	}
	
	@Test
	public void nullToStringTest1() {
		String result = Bytes.toString(null);
		assertEquals(null, result);
	}
	
	@Test
	public void nullToStringTest2() {
		String result = Bytes.toString(null, 0, 1);
		assertEquals(null, result);
	}
	
	@Test
	public void binaryToStringTest() {
		String content = "some content string";
		byte[] bytes = content.getBytes();
		String result = Bytes.toStringBinary(bytes);
		assertEquals(content, result);
	}
	
	@Test
	public void booleanToByteTest() {
		byte[] result = Bytes.toBytes(true);
		assertEquals(result[0], (byte) -1);
		result = Bytes.toBytes(false);
		assertEquals(result[0], (byte) 0);
	}
	
	@Test
	public void floatToByteTest() {
		float f = 0.233567f;
		byte[] bytes = Bytes.toBytes(f);
		float result = Bytes.toFloat(bytes);
		assertEquals(f, result, 0.000001f);
	}
	
	@Test
	public void doubleToByteTest() {
		double d = 0.233567f;
		byte[] bytes = Bytes.toBytes(d);
		double result = Bytes.toDouble(bytes);
		assertEquals(d, result, 0.000001f);
	}
	
	@Test
	public void intToByteTest() {
		int i = 124;
		byte[] bytes = Bytes.toBytes(i);
		int result = Bytes.toInt(bytes);
		assertEquals(i, result);
	}
	
	@Test
	public void shortToByteTest() {
		short i = 124;
		byte[] bytes = Bytes.toBytes(i);
		short result = Bytes.toShort(bytes);
		assertEquals(i, result);
	}
	
	@Test
	public void charToByteTest() {
		char i = 'i';
		byte[] bytes = Bytes.toBytes(i);
		char result = Bytes.toChar(bytes);
		assertEquals(i, result);
	}
	
	@Test
	public void charArrayToByteTest() {
		char[] i = {'a', 'b', 'c'};
		byte[] bytes = Bytes.toBytes(i);
		char[] result = Bytes.toChars(bytes);
		for(int j = 0 ; j < 3; j++) {
			assertEquals(i[j], result[j]);
		}
	}
	
	@Test
	public void stringToByteArraysTest() {
		String i = "blabla";
		byte[][] bytes = Bytes.toByteArrays(i);
		String result = Bytes.toString(bytes[0]);
		assertEquals(i, result);
	}
	
	@Test
	public void stringsToByteArraysTest() {
		String[] i = {"random","blabla", "random"};
		byte[][] bytes = Bytes.toByteArrays(i);
		String result = Bytes.toString(bytes[1]);
		assertEquals(i[1], result);
	}
	
	@Test
	public void byteToBooleantest() {
		byte[] input = {0};
		boolean result = Bytes.toBoolean(input);
		assertTrue(!result);
		byte[] input2 = {-1};
		result = Bytes.toBoolean(input2);
		assertTrue(result);
	}
	
	@Test
	public void pathUtilGetParentTest1() {
		assertNull(PathUtil.getParent("/"));
	}
	
	@Test
	public void pathUtilGetParentTest2() {
		assertEquals("/",PathUtil.getParent("/somefolder/"));
	}
	
	@Test
	public void pathUtilGetParentTest3() {
		assertEquals("/somefolder",PathUtil.getParent("/somefolder/somefile/"));
	}
	
	@Test(expected=RuntimeException.class)
	public void pathUtilGetParentTest4() {
		PathUtil.getParent("somefile");
	}
	
	@Test
	public void pathUtilRemoveTrailingSlashTest1() {
		assertEquals("/somefolder",PathUtil.removeTrailingSlash("/somefolder/"));
	}
	
	@Test
	public void pathUtilRemoveTrailingSlashTest2() {
		assertEquals("/",PathUtil.removeTrailingSlash("/"));
	}
	
	@Test
	public void pathUtilRemoveTrailingSlashTest3() {
		assertEquals("aString",PathUtil.removeTrailingSlash("aString"));
	}
	
	@Test
	public void pathUtilNormalizeTest1() {
		assertEquals("aString",PathUtil.normalizePath("aString"));
	}
	
	@Test
	public void pathUtilNormalizeTest2() {
		assertEquals("/aString/anotherString",PathUtil.normalizePath("\\aString\\anotherString\\"));
	}

	@Test(expected=IOException.class)
	public void pathUtilCheckPathTest1() throws IOException {
		PathUtil.checkPath("");
	}
	
	@Test(expected=IOException.class)
	public void pathUtilCheckPathTest2() throws IOException {
		PathUtil.checkPath(null);
	}
	
	@Test(expected=IOException.class)
	public void pathUtilCheckPathTest3() throws IOException {
		PathUtil.checkPath("foo:bar");
	}
	
	@Test(expected=IOException.class)
	public void pathUtilCheckPathTest4() throws IOException {
		PathUtil.checkPath("foo$bar");
	}
	
	@Test()
	public void pathUtilCheckPathTest5() throws IOException {
		PathUtil.checkPath("/foo/bar/");
	}
	
	@Test
	public void fsConstantsInstantiationTest() {
		assertNotNull(new FSConstants());
	}
	
}
