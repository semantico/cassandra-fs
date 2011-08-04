package com.semantico.cassandra.fs.tests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.junit.Test;
import static org.junit.Assert.*;

public class BasicCassandraFsTests extends CassandraFsTest {

	@Test
	public void makeDirTest() throws IOException {
		String path = "/testDirectory";
		fs.mkdir(path);
		assertTrue(fs.existDir(path));
	}
	
	@Test
	public void makeFileTest() throws IOException {
		String path = "/testDirectory";
		String filename = "testfile.txt";
		byte[] content = "some content".getBytes();
		fs.mkdir(path);
		fs.createFile(path+filename, content);
		assertTrue(fs.existFile(path+filename));
	}
	
	@Test
	public void readWriteFileTest() throws IOException {
		String path = "/testDirectory";
		String filename = "testfile.txt";
		String content = "some content";
		byte[] bytes = (content+"\n").getBytes();
		fs.mkdir(path);
		fs.createFile(path+filename, bytes);
		assertTrue(new BufferedReader(new InputStreamReader(fs.readFile(path+filename))).readLine().equals(content));
	}
	
	
}
