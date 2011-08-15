package org.apache.cassandra.contrib.fs;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import java.io.InputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.contrib.fs.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.thrift.transport.TTransportException;
import org.xerial.snappy.Snappy;

/**
 * The path here must be absolute, the relative path is handled by FSCliMain
 * 
 * @author zhanje
 * 
 */
public class CassandraFileSystem implements IFileSystem {

	private static Logger LOGGER = Logger.getLogger(CassandraFileSystem.class);

	private static SimpleDateFormat format = new SimpleDateFormat(
			"yyyy/MM/dd hh:mm");

	private static IFileSystem instance;

	private CassandraFacade facade;

	private byte[] buffer;

	public static IFileSystem getInstance(Configuration conf) throws TTransportException, IOException {
		if (instance == null) {
			synchronized (CassandraFileSystem.class) {
				if (instance == null) {
					instance = new CassandraFileSystem(conf);
				}
			}
		}
		return instance;
	}

	public static void dropInstance() { // used for ensuring different keyspaces between tests
		instance = null;
	}

	private CassandraFileSystem(Configuration conf) throws TTransportException, IOException {
		this.facade = CassandraFacade.getInstance(conf); //will initialize conf if its null
		buffer = new byte[facade.getConf().getBlockSize()];
		if (!existDir("/")) {
			mkdir("/");
		}
	}

	public void createFile(String path, byte[] content) throws IOException {
		createFile(path, new ByteArrayInputStream(content));
	}

	public void createFile(String path, InputStream in) throws IOException {
		PathUtil.checkPath(path);
		path = PathUtil.normalizePath(path);
		String parent = PathUtil.getParent(path);
		if (!existDir(parent)) {
			mkdir(parent);
		}

		long length = 0;
		long compressedLength = 0;
		int index = 0;
		int num = 0;
		while (true) {
			int count = 0;
			int remaining = buffer.length;
			while(true) { //fill the buffer!
				num = in.read(buffer, count, remaining);
				if (remaining == 0 || num == -1) {
					break;
				}
				count+= num;
				remaining-= num;
			}
			byte[] content = new byte[count];
			System.arraycopy(buffer, 0, content, 0, count);
			byte[] compress = Snappy.compress(content);

			length += count;
			compressedLength += compress.length;
			if (index == 0) {
				facade.put(path, FSConstants.DefaultFileCF + ":"
						+ FSConstants.ContentAttr, compress);
			} else {
				facade.put(path + "_$" + index, FSConstants.DefaultFileCF + ":"
						+ FSConstants.ContentAttr, compress);
			}
			index++;
			if (num == -1) {
				break;
			}
		}
		Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
		map.put(Bytes.toBytes(FSConstants.TypeAttr), Bytes.toBytes("File"));
		map.put(Bytes.toBytes(FSConstants.LengthAttr), Bytes.toBytes(length));
		map.put(Bytes.toBytes(FSConstants.CompressedLengthAttr), Bytes.toBytes(compressedLength));
		map.put(Bytes.toBytes(FSConstants.LastModifyTime), Bytes.toBytes(format.format(new Date())));
		map.put(Bytes.toBytes(FSConstants.OwnerAttr), FSConstants.DefaultOwner);
		map.put(Bytes.toBytes(FSConstants.GroupAttr), FSConstants.DefaultGroup);
		facade.batchPut(path, FSConstants.DefaultFileCF, null, map, false);

		// add meta data for parent, except the Content
		map = new HashMap<byte[], byte[]>();
		map.put(Bytes.toBytes(FSConstants.TypeAttr), Bytes.toBytes("File"));
		map.put(Bytes.toBytes(FSConstants.LengthAttr), Bytes.toBytes(length));
		map.put(Bytes.toBytes(FSConstants.CompressedLengthAttr), Bytes.toBytes(compressedLength));
		map.put(Bytes.toBytes(FSConstants.LastModifyTime), Bytes.toBytes(format.format(new Date())));
		map.put(Bytes.toBytes(FSConstants.OwnerAttr), FSConstants.DefaultOwner);
		map.put(Bytes.toBytes(FSConstants.GroupAttr), FSConstants.DefaultGroup);

		facade.batchPut(parent, FSConstants.DefaultFolderCF, path, map, true);
	}

	public boolean deleteFile(String path) throws IOException {
		PathUtil.checkPath(path);
		path = PathUtil.normalizePath(path);
		if (!existFile(path)) {
			LOGGER.warn("File '" + path
					+ "' can not been deleted, because it doesn't exist");
			return false;
		}
		String parent = PathUtil.getParent(path);
		facade.delete(path);
		for (int i = 1; facade.exist(path + "_$" + i); ++i) {
			facade.delete(path + "_$" + i); //TODO: ------------------CODE COVER HERE IS CRAP ? WHAT IS HAPPENING ?!
		}
		facade.delete(parent, FSConstants.DefaultFolderCF, path);
		return true;
	}

	public boolean deleteDir(String path, boolean recursive) throws IOException {
		PathUtil.checkPath(path);
		path = PathUtil.normalizePath(path);
		if (!exist(path)) {
			LOGGER.warn("Folder '" + path
					+ "' can not been deleted, because it doesn't exist");
			return false;
		}
		if (!recursive) {
			List<Path> paths = list(path);
			if (paths.size() > 0) {
				LOGGER.warn("Folder '" + path
						+ "' is not empty, and can not been deleted");
				return false;
			} else {
				String parent = PathUtil.getParent(path);
				facade.delete(path);
				facade.delete(parent, FSConstants.DefaultFolderCF, path);
				return true;
			}
		} else {
			List<Path> paths = list(path);
			for (Path p : paths) {
				if (p.isDir()) {
					deleteDir(p.getURL(), true);
				} else {
					deleteFile(p.getURL());
				}
			}
			deleteDir(path, false);
			return true;
		}
	}

	public InputStream readFile(String path) throws IOException {
		PathUtil.checkPath(path);
		path = PathUtil.normalizePath(path);
		LOGGER.debug("Reading file '" + path + "'");
		return new CFileInputStream(path, facade);
	}

	public boolean mkdir(String path) throws IOException {
		PathUtil.checkPath(path);
		path = PathUtil.normalizePath(path);
		if (existDir(path)) {
			LOGGER.warn("'" + path + "' is already existed");
			return false;
		}
		String parent = PathUtil.getParent(path);
		if (parent != null && !existDir(parent)) {
			mkdir(parent);
		}

		facade.put(path, FSConstants.DefaultFolderCF + ":" + FSConstants.DefaultFolderFlag
				+ ":" + FSConstants.TypeAttr, Bytes.toBytes("Dummy"));
		if (parent != null) {
			Map<byte[], byte[]> map = new HashMap<byte[], byte[]>();
			map.put(Bytes.toBytes(FSConstants.TypeAttr), Bytes
					.toBytes("Folder"));
			map.put(Bytes.toBytes(FSConstants.LengthAttr), Bytes.toBytes(0L));
			map.put(Bytes.toBytes(FSConstants.CompressedLengthAttr), Bytes.toBytes(0L));
			map.put(Bytes.toBytes(FSConstants.LastModifyTime), Bytes
					.toBytes(format.format(new Date())));
			map.put(Bytes.toBytes(FSConstants.OwnerAttr),
					FSConstants.DefaultOwner);
			map.put(Bytes.toBytes(FSConstants.GroupAttr),
					FSConstants.DefaultGroup);

			facade.batchPut(parent, FSConstants.DefaultFolderCF, path, map, true);
		}
		LOGGER.debug("Creat dir '" + path + "' succesfully");
		return true;
	}

	public List<Path> list(String path) throws IOException {
		PathUtil.checkPath(path);
		List<Path> result = new ArrayList<Path>();
		path = PathUtil.normalizePath(path);
		if (existDir(path)) {
			result = facade.list(path, FSConstants.DefaultFolderCF, false);
		} else if (existFile(path)) {
			result = facade.list(path, FSConstants.DefaultFileCF, false);
		} else {
			return result;
		}
		// set the max size length, the max size length is for cli formatting
		int max = 0;
		for (Path child : result) {
			if ((child.getLength() + "").length() > max) {
				max = (child.getLength() + "").length();
			}
		}
		Path.MaxSizeLength = max;
		return result;
	}

	public List<Path> listAll(String path) throws IOException {
		PathUtil.checkPath(path);
		path = PathUtil.normalizePath(path);
		return facade.list(path, FSConstants.DefaultFolderCF, true);
	}

	public boolean existDir(String path) throws IOException {
		PathUtil.checkPath(path);
		path = PathUtil.normalizePath(path);
		return facade.list(path, FSConstants.DefaultFolderCF, true).size() != 0;
	}

	public boolean existFile(String path) throws IOException {
		PathUtil.checkPath(path);
		path = PathUtil.normalizePath(path);
		return facade.list(path, FSConstants.DefaultFileCF, true).size() != 0;
	}

	public boolean exist(String path) throws IOException {
		return existDir(path) || existFile(path);
	}

}
