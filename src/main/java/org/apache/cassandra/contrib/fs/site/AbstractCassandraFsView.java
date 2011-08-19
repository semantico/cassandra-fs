package org.apache.cassandra.contrib.fs.site;

import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.List;
import java.util.Map;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.cassandra.contrib.fs.Path;
import org.apache.commons.lang.StringEscapeUtils;
import org.springframework.web.servlet.view.AbstractView;

public abstract class AbstractCassandraFsView extends AbstractView {

	@Override
	protected void renderMergedOutputModel(Map<String, Object> model,HttpServletRequest request, HttpServletResponse response)throws Exception {
		try {
			handleRequest(request, response);
		} catch (Exception e) {
			writeException(response, e);
		} finally {
			response.getOutputStream().flush();
		}
	}
	
	abstract protected void handleRequest(HttpServletRequest request, HttpServletResponse response) throws Exception;
	
	protected void writeSuccessful(HttpServletResponse resp,String msg) {
		resp.setContentType("text/xml");
		OutputStreamWriter writer = null;
		try {
			//TODO: use more advanced xml generation
			writer = new OutputStreamWriter(resp.getOutputStream());
			writer.write("<Response>\n");
			writer.write("<Code>\n");
			writer.write("OK");
			writer.write("</Code>\n");
			writer.write("<Message>\n");
			writer.write(msg); //TODO: replace special chars
			writer.write("</Message>\n");
			writer.write("</Response>");
		}
		catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(writer != null) {
				try {
					writer.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	protected void writeException(HttpServletResponse resp, Exception ex) {
		resp.setContentType("text/xml");
		OutputStreamWriter writer = null;
		try {
			//System.out.print("Line Sperator:" + System.getProperty("line.separator"));
			writer = new OutputStreamWriter(resp.getOutputStream());
			writer.write("<Error>\n");
			writer.write("<Code>\n");
			writer.write(StringEscapeUtils.escapeXml(ex.getClass().getName()));
			writer.write("</Code>\n");
			writer.write("<Message>\n");
			String message = ex.getMessage() + "";
			message = StringEscapeUtils.escapeXml(message);
			writer.write(message); 
			writer.write("</Message>\n");
			writer.write("<StackTrace>");
			ex.printStackTrace(new PrintWriter(writer));
			writer.write("</StackTrace>");
			writer.write("</Error>");
		}
		catch (IOException e) {
			e.printStackTrace();
		} finally {
			if(writer != null) {
				try {
					writer.flush();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
	}

	protected String getPath(HttpServletRequest request){
		return request.getRequestURI();
	}
	
	protected String getFileName(HttpServletRequest request) {
		String[] parts = request.getRequestURI().split("/");
		return parts[parts.length-1];
	}
	
}
