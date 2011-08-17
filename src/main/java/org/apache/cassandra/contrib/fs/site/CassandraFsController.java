package org.apache.cassandra.contrib.fs.site;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.ModelAndView;
import org.springframework.web.servlet.View;

@Controller
@RequestMapping("/*")
public class CassandraFsController {
	
	@Autowired
	protected ListView listView;
	@Autowired
	protected GetFileView getFileView;
	@Autowired
	protected PutFileView putFileView;
	@Autowired
	protected DeleteView deleteView;

	@RequestMapping(method=RequestMethod.GET)
	public ModelAndView getOrList(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
		View view;
		String action = getAction(req);
		if(action.equalsIgnoreCase("ls")) {
			view = listView;
		} else {
			view = getFileView;
		}
		ModelAndView mav = new ModelAndView(view);
		return mav;
	}
	
	@RequestMapping(method=RequestMethod.PUT)
	public ModelAndView put(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
		ModelAndView mav = new ModelAndView(putFileView);
		return mav;
	}
	
	@RequestMapping(method=RequestMethod.DELETE)
	public ModelAndView delete(HttpServletRequest req, HttpServletResponse response) throws ServletException, IOException {
		ModelAndView mav = new ModelAndView(deleteView);
		return mav;
	}
	
	private String getAction(HttpServletRequest request) {
		String action = request.getParameter("action");
		return action == null ? "" : action;
	}


	public void setListView(ListView listView) {
		this.listView = listView;
	}


	public void setGetFileView(GetFileView getFileView) {
		this.getFileView = getFileView;
	}


	public void setPutFileView(PutFileView putFileView) {
		this.putFileView = putFileView;
	}

	public void setDeleteView(DeleteView deleteView) {
		this.deleteView = deleteView;
	}
}
