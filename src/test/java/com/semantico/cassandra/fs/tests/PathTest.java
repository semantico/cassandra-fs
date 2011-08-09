package com.semantico.cassandra.fs.tests;

import org.apache.cassandra.contrib.fs.InvalidPathException;
import org.apache.cassandra.contrib.fs.Path;
import org.junit.Test;

import static org.junit.Assert.*;

public class PathTest {

	@Test
	public void makePathTest() {
		assertNotNull(new Path("folder/folder2/file.txt"));
	}
	
	@Test
	public void nameFromPathTest() {
		Path path = new Path("folder/folder2/file.txt");
		assertEquals(path.getName(), "file.txt");
	}
	
	@Test
	public void nameFromEmptyPathTest() {
		Path path = new Path("/");
		assertEquals(path.getName(), "/");
	}
	
	@Test
	public void nameFromFolderPathTest() {
		Path path = new Path("folder/folder2/");
		assertEquals(path.getName(), "");
	}
	
	@Test
	public void toStringTest() {
		Path.MaxSizeLength = 10;
		Path path = new Path("folder/folder2/");
		path.toString();
	}
	
	@Test
	public void invalidPathTest() {
		InvalidPathException e = new InvalidPathException();
		assertNotNull(e);
		e= new InvalidPathException("message");
		assertEquals("message", e.getMessage());
		Exception cause = new Exception("bla");
		e = new InvalidPathException(cause);
		assertEquals(cause, e.getCause());
		e= new InvalidPathException("message", cause);
		assertEquals("message", e.getMessage());
		assertEquals(cause, e.getCause());
		
	}
	
}
