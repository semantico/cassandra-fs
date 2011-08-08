package com.semantico.cassandra.fs.tests;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;

import org.apache.cassandra.contrib.fs.FSConstants;
import org.apache.cassandra.contrib.fs.Path;
import org.junit.Test;
import static org.junit.Assert.*;

public class CassandraFileSystemTest extends AbstractCassandraFsTest {

	private static final String path = "/testDirectory/";
	private static final String nestedPath = "/testDirectory/nestedFolder/";
	private static final String filename = "testfile.txt";
	private static final String filename2 = "testfile2.txt";
	private static final String filename3 = "testfile3.txt";
	private static final String content = "some content\n";
	private static final byte[] contentBytes = content.getBytes();
	private static String longContent;
	private static byte[] longContentBytes;
	
	static {
		for(int i = 0; i < (10000); i++) {
			longContent+= "lotsandlotsofchars"; 
		}
		longContentBytes = longContent.getBytes();
	}
	

	@Test
	public void makeDirTest() throws IOException {
		fs.mkdir(path);
		assertTrue(fs.existDir(path));
	}
	
	@Test
	public void makeMultipleDirsTest() throws IOException {
		fs.mkdir(nestedPath); //non existant parent folders should be made too
		assertTrue(fs.existDir(path));
		assertTrue(fs.existDir(nestedPath));
	}
	
	@Test
	public void failingMakeDirTest() throws IOException {
		fs.mkdir(nestedPath); //non existant parent folders should be made too
		assertTrue(!fs.mkdir(nestedPath));
	}

	@Test
	public void makeFileTest() throws IOException {
		fs.mkdir(path);
		fs.createFile(path+filename, contentBytes);
		assertTrue(fs.existFile(path+filename));
	}
	
	@Test
	public void makeFileTwiceTest() throws IOException {
		fs.createFile(path+filename, contentBytes);
		fs.createFile(path+filename, contentBytes);
	}
	
	@Test
	public void makeBiggerFileTest() throws IOException {
		fs.mkdir(path);
		fs.createFile(path+filename, longContentBytes);
		assertTrue(fs.existFile(path+filename));
	}
	
	@Test
	public void nonExistantFileTest() throws IOException {
		fs.mkdir(path);
		assertTrue(!fs.existFile(path+filename));
	}

	@Test
	public void readWriteFileTest() throws IOException {
		fs.mkdir(path);
		fs.createFile(path+filename, contentBytes);
		assertEquals(content.trim() , new BufferedReader(new InputStreamReader(fs.readFile(path+filename))).readLine());
	}
	
	@Test
	public void existTest() throws IOException {
		fs.createFile(path+filename, contentBytes);
		assertTrue(fs.exist(path+filename));
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
	public void listNonExistantTest() throws IOException {
		List<Path> paths = fs.list(path);
		assertEquals(0, paths.size());
	}
	
	@Test
	public void listAFileTest() throws IOException {
		fs.mkdir(path);
		fs.createFile(path+filename, contentBytes);
		List<Path> paths = fs.list(path+filename);
		assertEquals(1, paths.size());
		assertEquals(path+filename, paths.get(0).getURL());
	}

	@Test
	public void listAllTest() throws IOException {
		//Logger logger = Logger.getLogger(Logger.GLOBAL_LOGGER_NAME);
		fs.mkdir(path);
		fs.mkdir(nestedPath);
		fs.createFile(path+filename, contentBytes);
		fs.createFile(path+filename2, contentBytes);
		fs.createFile(nestedPath+ filename3, contentBytes); //this file shouldnt shown up because its nested deeper
		List<Path> paths = fs.listAll(path);
		//		for(Path path : paths) {
		//			logger.info(path.toString());
		//		}
		assertEquals(4, paths.size()); //$_Folder_$ tag, nestedFolder, file1 ,file2
	}

	@Test
	public void deleteFileTest() throws IOException {
		fs.mkdir(path);
		fs.createFile(path+filename, contentBytes);
		assertTrue("The File Wasn't Created", fs.existFile(path+filename));
		fs.deleteFile(path+filename);
		assertTrue("The File Wasn't Deleted", !fs.existFile(path+filename));
	}
	
	@Test
	public void deleteNonExistantFileTest() throws IOException {
		assertTrue(!fs.deleteFile(path+filename));
	}

	@Test
	public void deleteFolderTest() throws IOException {
		fs.mkdir(path);
		assertTrue("The Folder Wasn't Created", fs.existDir(path));
		fs.deleteDir(path, false);
		assertTrue("The Folder Wasn't Deleted", !fs.existDir(path));
	}
	
	@Test
	public void deleteNonExistantFolderTest() throws IOException {
		assertTrue(!fs.deleteDir(nestedPath, false));
	}

	@Test
	public void failingDeleteNonEmptyFolderTest() throws IOException {
		fs.mkdir(path);
		assertTrue("The Folder Wasn't Created", fs.existDir(path));
		fs.createFile(path+filename, contentBytes);
		assertTrue("The File Wasn't Created", fs.existFile(path+filename));
		fs.deleteDir(path, false);// non recursive delete
		assertTrue("The Folder Was Deleted, But it still had Files in!", fs.existDir(path));
	}

	@Test
	public void succeedingDeleteNonEmptyFolderTest() throws IOException {
		fs.mkdir(nestedPath);
		assertTrue("The Folder Wasn't Created", fs.existDir(path));
		fs.createFile(path+filename, contentBytes);
		assertTrue("The File Wasn't Created", fs.existFile(path+filename));
		fs.deleteDir(path, true); // recursive delete
		assertTrue("The Folder Wasn't Deleted", !fs.existDir(path));
		assertTrue("The File Wasn't Deleted", !fs.existFile(path+filename));
	}
	
	@Test
	public void readWriteSmallFileStreamTest() throws IOException {
		readWriteStreamFromLocalTest("smallTextFile.txt");
	}
	
	@Test
	public void readWriteDeleteLargeFileStreamTest() throws IOException {
		readWriteStreamFromLocalTest("largeTextFile.txt");
		fs.deleteFile(path+"largeTextFile.txt");
		assertTrue(!fs.exist(path+"largeTextFile.txt"));
		
	}

	private void readWriteStreamFromLocalTest(String fileToTestWith) throws IOException {
		InputStream inputStream = new FileInputStream(new File("src/test/resources/" + fileToTestWith));
		fs.createFile(path+fileToTestWith, inputStream);
		inputStream.close();
		inputStream = new FileInputStream(new File("src/test/resources/"+ fileToTestWith));
		assertTrue("File Was Not Copied Exactly", isStreamContentEqual(inputStream, fs.readFile(path+fileToTestWith)));
	}

	private boolean isStreamContentEqual(InputStream inputStream1, InputStream inputStream2) throws IOException {
		int in1 = 0;
		int in2 = 0;
		while((in1 = inputStream1.read()) != -1 | (in2 = inputStream2.read()) != -1) {
			if(in1 != in2) {
				return false;
			}
		}
		return true;
	}


}
