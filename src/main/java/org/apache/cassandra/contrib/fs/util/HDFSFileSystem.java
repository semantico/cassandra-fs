package org.apache.cassandra.contrib.fs.util;
/*
 * Notice: This file is modified from the original as provided under the apache 2.0 license
 */
//import java.io.IOException;
//import java.net.URI;
//import java.net.URISyntaxException;
//import java.util.Collections;
//import java.util.HashMap;
//import java.util.Map;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FileSystem;


//public class HDFSFileSystem {
//
//	private static Map<String, FileSystem> caches = Collections
//			.synchronizedMap(new HashMap<String, FileSystem>());
//
//	public static FileSystem getFileSystem(String url) throws IOException {
//		FileSystem fs = caches.get(url);
//		if (fs == null) {
//			try {
//				fs = FileSystem.get(new URI(url), new Configuration());
//				caches.put(url, fs);
//			} catch (URISyntaxException e) {
//				throw new IOException(e);
//			}
//		}
//		return fs;
//	}
//
//}
