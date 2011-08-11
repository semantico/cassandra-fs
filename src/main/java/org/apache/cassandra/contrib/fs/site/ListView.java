package org.apache.cassandra.contrib.fs.site;

import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.cassandra.contrib.fs.CassandraFileSystem;
import org.apache.cassandra.contrib.fs.Path;

public class ListView extends AbstractCassandraFsView {

	@Override
	protected void handleRequest(HttpServletRequest request,HttpServletResponse response) throws Exception {
		String dir = getPath(request);
		List<Path> subs = CassandraFileSystem.getInstance(null).list(dir);
		writePaths(response, dir, subs);
	}

}
