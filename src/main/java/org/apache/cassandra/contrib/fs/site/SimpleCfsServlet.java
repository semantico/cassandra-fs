package org.apache.cassandra.contrib.fs.site;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Extends the cfs site servlet by making the request methods public
 * @author ed
 */
public class SimpleCfsServlet extends CfsSiteServlet {

	private static final long serialVersionUID = 1L;

	@Override
	public void doGet(HttpServletRequest request,HttpServletResponse resp) throws ServletException, IOException {
		super.doGet(request, resp);
	}

	@Override
	public void doPut(HttpServletRequest request,HttpServletResponse resp) throws ServletException, IOException {
		super.doPut(request, resp);
	}

	@Override
	public void doDelete(HttpServletRequest request,HttpServletResponse resp) throws ServletException, IOException {
		super.doDelete(request, resp);
	}
	
}
