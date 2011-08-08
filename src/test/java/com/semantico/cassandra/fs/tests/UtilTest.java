package com.semantico.cassandra.fs.tests;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.cassandra.contrib.fs.PathUtil;
import org.apache.cassandra.contrib.fs.util.Bytes;
import org.apache.cassandra.contrib.fs.util.Helper;
import org.junit.Assert;
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
	
}
