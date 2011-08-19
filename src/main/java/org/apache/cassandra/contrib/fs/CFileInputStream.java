package org.apache.cassandra.contrib.fs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.cassandra.thrift.NotFoundException;
import org.xerial.snappy.Snappy;
/*
 * Notice: This file is modified from the original as provided under the apache 2.0 license
 * Files are decompressed (using snappy) block by block as they are read out of the database.
 * This should still be compatible with files that werent compressed (using older versions of cassandra-fs),
 * but this has not been tested.
 */
/**
 * An input stream that reads a file from cassandraFS.
 * It will request one block at a time, uncompress it and serve it up, delegating read methods to a
 * ByteArrayInputStream.
 * @author zhanje, Edd King
 *
 */
public class CFileInputStream extends InputStream {

	private InputStream curBlockStream;

	private int blockIndex = 0;

	private String path;

	private CassandraFacade facade;

	/**
	 * Makes a new input stream to the file spacified by path, using the connection given by facade
	 * @param path path to the file to open
	 * @param facade connection to cassandra to use
	 * @throws IOException
	 */
	public CFileInputStream(String path, CassandraFacade facade) throws IOException {
		this.path = path;
		this.facade = facade;
		byte[] bytes = facade.get(path, FSConstants.DefaultFileCF + ":" + FSConstants.ContentAttr);
		if(Snappy.isValidCompressedBuffer(bytes)) {
			bytes = Snappy.uncompress(bytes);
		}
		this.curBlockStream = new ByteArrayInputStream(bytes);
		this.blockIndex++;
	}

	@Override
	@Deprecated
	public int read() throws IOException {
		byte[] singleByte = new byte[1];
		int nread = read(singleByte, 0, 1);
		if(nread ==-1) {
			return -1;
		}
		return singleByte[0];
	}

	@Override
	public int read(byte[] buffer, int offset, int length) throws IOException {
		int nRead = curBlockStream.read(buffer, offset, length);
		if (nRead != -1) {
			return nRead;
		}
		try {

			byte[] bytes = facade.get(path + "_$" + blockIndex++,FSConstants.DefaultFileCF + ":" + FSConstants.ContentAttr);
			if(Snappy.isValidCompressedBuffer(bytes)) {
				bytes = Snappy.uncompress(bytes);
			}
			curBlockStream = new ByteArrayInputStream(bytes);
			return curBlockStream.read(buffer, offset, length);

		} catch (IOException e) {
			if (e.getCause() instanceof NotFoundException) {
				return -1;
			}
			throw e;
		}
	}
}
