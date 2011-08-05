package com.semantico.cassandra.fs.tests;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.logging.Logger;

import org.apache.cassandra.contrib.fs.Path;
import org.junit.Test;
import static org.junit.Assert.*;

public class BasicCassandraFsTests extends CassandraFsTest {

	private static final String path = "/testDirectory/";
	private static final String nestedPath = "/testDirectory/nestedFolder/";
	private static final String filename = "testfile.txt";
	private static final String filename2 = "testfile2.txt";
	private static final String filename3 = "testfile3.txt";
	private static final String content = "some content\n";
	private static final byte[] contentBytes = content.getBytes();
	
	@Test
	public void makeDirTest() throws IOException {
		fs.mkdir(path);
		assertTrue(fs.existDir(path));
	}
	
	@Test
	public void makeFileTest() throws IOException {
		fs.mkdir(path);
		fs.createFile(path+filename, contentBytes);
		assertTrue(fs.existFile(path+filename));
	}
	
	@Test
	public void readWriteFileTest() throws IOException {
		fs.mkdir(path);
		fs.createFile(path+filename, contentBytes);
		assertEquals(content.trim() , new BufferedReader(new InputStreamReader(fs.readFile(path+filename))).readLine());
		
	}
	
	@Test
	public void listTest() throws IOException {
		
		fs.mkdir(path);
		fs.createFile(path+filename, contentBytes);
		assertTrue(fs.existFile(path+filename));
		fs.createFile(path+filename2, contentBytes);
		assertTrue(fs.existFile(path+filename2));
		List<Path> paths = fs.list(path);
		
		assertEquals(2, paths.size());
		assertEquals(path+filename,paths.get(0).getURL());
		assertEquals(path+filename2,paths.get(1).getURL());
	}
	
	@Test
	public void listAllTest() throws IOException {
		Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
		fs.mkdir(path);
		fs.mkdir(nestedPath);
		fs.createFile(path+filename, contentBytes);
		fs.createFile(path+filename2, contentBytes);
		fs.createFile(nestedPath+ filename3, contentBytes); //this file shouldnt shown up because its nested
		List<Path> paths = fs.listAll(path);
		for(Path path : paths) {
			logger.info(path.toString());
		}
		assertEquals(4, paths.size()); //$_Folder_$ tag, nestedFolder, file1 ,file 2
	}
}
