package org.apache.cassandra.contrib.fs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.concurrent.ArrayBlockingQueue;

import org.apache.cassandra.thrift.NotFoundException;

public class CFileInputStream extends InputStream {

	private InputStream curBlockStream;


	private String path;

	private CassandraFacade facade;
	private ArrayBlockingQueue<byte[]> blockQueue;
	private ReadAheadBlockRequester blockRequester;

	public CFileInputStream(String path, CassandraFacade facade) throws IOException {
		this.path = path;
		this.facade = facade;
		this.blockQueue = new ArrayBlockingQueue<byte[]>(6);
		this.blockRequester = new ReadAheadBlockRequester();
		blockRequester.start();
		byte[] bytes = facade.get(path, FSConstants.DefaultFileCF + ":" + FSConstants.ContentAttr);
		this.curBlockStream = new ByteArrayInputStream(bytes);
	}

	@Override
	@Deprecated
	public int read() throws IOException {
		int next = curBlockStream.read();
		if (next != -1) {
			return next;
		} else {
			try {
				byte[] bytes = blockQueue.take();
				if( bytes.length > 0) {
					curBlockStream = new ByteArrayInputStream(bytes);
					return curBlockStream.read();
				} else {
					return -1;
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	@Override
	public int read(byte[] buffer, int offset, int length) throws IOException {
		int nRead = curBlockStream.read(buffer, offset, length);
		if (nRead != -1) {
			return nRead;
		} else {
			try {
				byte[] bytes = blockQueue.take();
				if(bytes.length > 0) {
					curBlockStream = new ByteArrayInputStream(bytes);
					return curBlockStream.read(buffer, offset, length);
				} else {
					return -1;
				}
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	private class ReadAheadBlockRequester extends Thread {

		@Override
		public void run() {
			try {
				int blockIndex = 1;
				while(true) {
					byte[] bytes = facade.get(path + "_$" + blockIndex++,FSConstants.DefaultFileCF + ":" + FSConstants.ContentAttr);
					blockQueue.put(bytes);
				}
			} catch (IOException e) {
				if (e.getCause() instanceof NotFoundException) {
					try {
						blockQueue.put(new byte[0]);
					} catch (InterruptedException e1) {
						e1.printStackTrace();
					}
				} else {
					e.printStackTrace();
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}

}
