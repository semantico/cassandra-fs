//package com.semantico.cassandra.fs.tests;
//
//import java.io.File;
//import java.io.FileInputStream;
//import java.io.FileNotFoundException;
//import java.io.FileOutputStream;
//import java.io.FileWriter;
//import java.io.IOException;
//import java.io.InputStream;
//import java.io.OutputStream;
//import java.util.Random;
//
//import org.apache.cassandra.contrib.fs.CassandraFileSystem;
//import org.apache.cassandra.contrib.fs.Configuration;
//import org.apache.cassandra.contrib.fs.FSConstants;
//import org.apache.thrift.transport.TTransportException;
//import org.junit.Before;
//import org.junit.Test;
//
//import com.eaio.uuid.UUID;
//
///**
// * Note this test needs to be run while an actual cassandra server is running else it wont work
// * (not the embedded one)
// * @author ed
// *
// */
//public class StressTest extends AbstractCassandraFsTest {
//
//	private File testFile;
//	private File resultsFile;
//	private Random random;
//
//	@Before
//	public void realServerConfigSetup() throws TTransportException, IOException {
//		properties.setProperty(FSConstants.BlockSizeKey, "" + 5*1024*1024*1); //5mb chunk
//		properties.setProperty(FSConstants.ClusterNameKey, FSConstants.DefaultClusterName);
//		properties.setProperty(FSConstants.HostsKey, "localhost:9160");
//		cleanPrebuiltKeyspace();
//		conf = new Configuration(properties);
//		fs = CassandraFileSystem.getInstance(conf);
//		random = new Random();
//	}
//
//	@Test
//	public void fileWrite() throws IOException {
//		int repeats = 3;
//		makeTestResultsFile("writeResults.txt");
//		makeTestDataFile();
//		int sizeOfFile = 0;
//		int fileNo = 0;
//		double rateOfGrowth = 1.1;
//		while(sizeOfFile < 1024*1024*3) {
//			for(int j = 0; j < repeats; j++) {
//				long timeTaken = writeFile(fileNo);
//				deleteFile(fileNo);
//				if(j == 0) {
//					writeToResultsFile("\n"+testFile.length()+","+timeTaken);
//				} else {
//					writeToResultsFile(","+timeTaken);
//				}
//				fileNo++;
//			}
//			byte[] bytes = new byte[sizeOfFile];
//			random.nextBytes(bytes);
//			appendLineToTestDataFile(bytes);
//			sizeOfFile = (int) (sizeOfFile*rateOfGrowth + 1);
//
//		}
//	}
//
//	@Test
//	public void fileRead() throws IOException {
//		int repeats = 4;
//		makeTestResultsFile("readResults.txt");
//		makeTestDataFile();
//		int sizeOfFile = 0;
//		int fileNo = 0;
//		double rateOfGrowth = 1.1;
//		while(sizeOfFile < 1024*1024*30) {
//			writeFile(fileNo);
//			for(int j = 0; j < repeats; j++) {
//
//				long timeTaken = readFile(fileNo);
//
//				if(j == 0) {
//					writeToResultsFile("\n"+testFile.length()+","+timeTaken);
//				} else {
//					writeToResultsFile(","+timeTaken);
//				}
//
//			}
//			deleteFile(fileNo);
//			byte[] bytes = new byte[sizeOfFile];
//			random.nextBytes(bytes);
//			appendLineToTestDataFile(bytes);
//			sizeOfFile = (int) (sizeOfFile*rateOfGrowth + 1);
//			fileNo++;
//		}
//	}
//
//	@Test
//	public void readAsDatabaseSizeIncreases() throws IOException {
//		makeTestResultsFile("read2Results.txt");
//		makeTestDataFile();
//		int repeats = 4;
//		int fileNo = 0;
//		byte[] bytes = new byte[1024*1024];
//		random.nextBytes(bytes);
//		appendLineToTestDataFile(bytes);
//		for(int i = 0; i < 3000; i++) {
//			writeFile(fileNo);
//			for(int j = 0; j < repeats; j++) {
//				long timeTaken = readFile(fileNo);
//				if(j == 0) {
//					writeToResultsFile("\n"+bytes.length*(i+1)+","+timeTaken);
//				} else {
//					writeToResultsFile(","+timeTaken);
//				}
//			}
//			fileNo++;
//		}
//	}
//
//	@Test
//	public void writeAsDatabaseSizeIncreases() throws IOException {
//		makeTestResultsFile("write2Results.txt");
//		makeTestDataFile();
//		int repeats = 4;
//		int fileNo = 0;
//		byte[] bytes = new byte[1024*1024];
//		random.nextBytes(bytes);
//		appendLineToTestDataFile(bytes);
//		for(int i = 0; i < 3000; i++) {
//			long timeTaken = writeFile(fileNo);
//			writeToResultsFile("\n"+bytes.length*(i+1)+","+timeTaken);
//			fileNo++;
//		}
//	}
//
//	private void makeTestResultsFile(String name) {
//		resultsFile = new File("src/test/resources/" + name);
//		if(resultsFile.exists()) {
//			resultsFile.delete();
//		}
//	}
//
//	private void makeTestDataFile() throws IOException {
//		testFile = new File("src/test/resources/bigDataFile.dat");
//		if(testFile.exists()) {
//			testFile.delete();
//		}
//		OutputStream out = new FileOutputStream(testFile, false);
//		out.write("StartOfFile\n".getBytes());
//		out.flush();
//		out.close();
//	}
//
//	private long writeFile(int version) throws IOException {
//		InputStream inputStream = new FileInputStream(testFile);
//		long startTime = System.nanoTime();
//		fs.createFile("/bigDataFile" + version + ".txt", inputStream);
//		long estimatedTime = System.nanoTime() - startTime;
//		inputStream.close();
//		return estimatedTime;
//	}
//
//	private long readFile(int version) throws IOException {
//		long startTime = System.nanoTime();
//		InputStream stream = fs.readFile("/bigDataFile" + version + ".txt");
//		byte[] buffer = new byte[1024];
//		while(stream.read(buffer) != -1);
//		long estimatedTime = System.nanoTime() - startTime;
//		stream.close();
//		return estimatedTime;
//	}
//
//	private long deleteFile(int version) throws IOException {
//		long startTime = System.nanoTime();
//		fs.deleteFile("/bigDataFile" + version + ".txt");
//		long estimatedTime = System.nanoTime() - startTime;
//		return estimatedTime;
//	}
//
//	private void appendLineToTestDataFile(byte[] data) throws IOException {
//		FileOutputStream writer = new FileOutputStream(testFile, true);
//		writer.write(data);
//		writer.flush();
//		writer.close();
//	}
//
//	private void writeToResultsFile(String output) throws IOException {
//		FileOutputStream writer = new FileOutputStream(resultsFile, true);
//		writer.write(output.getBytes());
//		writer.flush();
//		writer.close();
//	}
//}
