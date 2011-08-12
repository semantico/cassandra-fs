package org.apache.cassandra.contrib.fs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.cassandra.thrift.NotFoundException;
import org.xerial.snappy.Snappy;

public class CFileInputStream extends InputStream {

	private InputStream curBlockStream;

	private int blockIndex = 0;

	private String path;

	private CassandraFacade facade;

	public CFileInputStream(String path, CassandraFacade facade) throws IOException {
		this.path = path;
		this.facade = facade;
		byte[] bytes = facade.get(path, FSConstants.DefaultFileCF + ":" + FSConstants.ContentAttr);
		this.curBlockStream = new ByteArrayInputStream(Snappy.uncompress(bytes));
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
			curBlockStream = new ByteArrayInputStream(Snappy.uncompress(bytes));
			return curBlockStream.read(buffer, offset, length);

		} catch (IOException e) {
			if (e.getCause() instanceof NotFoundException) {
				return -1;
			}
			throw e;
		}

	}

}
