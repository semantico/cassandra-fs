package com.semantico.cassandra.fs.tests;

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
	
}
