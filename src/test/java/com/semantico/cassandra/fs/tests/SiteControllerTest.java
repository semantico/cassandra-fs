package com.semantico.cassandra.fs.tests;

import static org.junit.Assert.*;

import java.io.IOException;

import javax.servlet.ServletException;

import org.apache.cassandra.contrib.fs.site.CassandraFsController;
import org.apache.cassandra.contrib.fs.site.DeleteView;
import org.apache.cassandra.contrib.fs.site.GetFileView;
import org.apache.cassandra.contrib.fs.site.ListView;
import org.apache.cassandra.contrib.fs.site.PutFileView;
import org.junit.Before;
import org.junit.Test;
import org.springframework.mock.web.MockHttpServletRequest;
import org.springframework.mock.web.MockHttpServletResponse;
import org.springframework.web.servlet.ModelAndView;

public class SiteControllerTest extends AbstractCassandraFsTest {
	
	private MockHttpServletRequest mockRequest;
	private MockHttpServletResponse mockResponse;
	private CassandraFsController controller;
	
	@Before
	public void setUpServlet() {
		mockRequest = new MockHttpServletRequest();
		mockRequest.setContent(new byte[0]);
		mockResponse = new MockHttpServletResponse();
		controller = new CassandraFsController();
		controller.setDeleteView(new DeleteView());
		controller.setGetFileView(new GetFileView());
		controller.setListView(new ListView());
		controller.setPutFileView(new PutFileView());
	}
	
	@Test
	public void putGetFileTest() throws Exception {
		mockRequest.setContent("someContent".getBytes());
		mockRequest.setRequestURI("/test/test.txt");
		ModelAndView mav = controller.put(mockRequest, mockResponse);
		mav.getView().render(mav.getModel(), mockRequest, mockResponse);
		//System.out.println("Content: "+mockResponse.getContentAsString());
		mockRequest = new MockHttpServletRequest();
		mockResponse = new MockHttpServletResponse();
		mockRequest.setRequestURI("/test/test.txt");
		mav = controller.getOrList(mockRequest, mockResponse);
		mav.getView().render(mav.getModel(), mockRequest, mockResponse);
		assertEquals("someContent", mockResponse.getContentAsString());
	}
	
	@Test
	public void listTest() throws Exception {
		putGetFileTest();
		mockRequest = new MockHttpServletRequest();
		mockResponse = new MockHttpServletResponse();
		mockRequest.setRequestURI("/test/");
		mockRequest.setParameter("action", "ls");
		ModelAndView mav = controller.getOrList(mockRequest, mockResponse);
		mav.getView().render(mav.getModel(), mockRequest, mockResponse);
		assertEquals("<Paths dir=\"/test/\">\n<Path>test.txt</Path>\n</Paths>", mockResponse.getContentAsString());
	}
	
	@Test
	public void deleteTest() throws Exception {
		putGetFileTest();
		mockRequest = new MockHttpServletRequest();
		mockResponse = new MockHttpServletResponse();
		mockRequest.setRequestURI("/test/test.txt");
		ModelAndView mav = controller.delete(mockRequest, mockResponse);
		mav.getView().render(mav.getModel(), mockRequest, mockResponse);
		//System.out.println(mockResponse.getContentAsString());
		
		mockRequest = new MockHttpServletRequest();
		mockResponse = new MockHttpServletResponse();
		mockRequest.setRequestURI("/test/test.txt");
		mav = controller.getOrList(mockRequest, mockResponse);
		mav.getView().render(mav.getModel(), mockRequest, mockResponse);
		assertTrue(!"someContent".equals(mockResponse.getContentAsString()));
	}
	
	@Test
	public void viewIOExceptionTest1() throws Exception {
		mockRequest.setContent("someContent".getBytes());
		mockRequest.setRequestURI("/test/test.txt");
		mockResponse.getOutputStream().close();
		ModelAndView mav = controller.put(mockRequest, mockResponse);
		mav.getView().render(mav.getModel(), mockRequest, mockResponse);
	}
	
	@Test
	public void viewIOExceptionTest2() throws Exception {
		putGetFileTest();
		mockRequest = new MockHttpServletRequest();
		mockResponse = new MockHttpServletResponse();
		mockRequest.setRequestURI("/test/");
		mockRequest.setParameter("action", "ls");
		mockResponse.getOutputStream().close();
		ModelAndView mav = controller.getOrList(mockRequest, mockResponse);
		mav.getView().render(mav.getModel(), mockRequest, mockResponse);
	}
	
	@Test
	public void viewIOExceptionTest3() throws Exception {
		putGetFileTest();
		mockRequest = new MockHttpServletRequest();
		mockResponse = new MockHttpServletResponse();
		mockRequest.setRequestURI("/test/test.txt");
		mockResponse.getOutputStream().close();
		ModelAndView mav = controller.getOrList(mockRequest, mockResponse);
		mav.getView().render(mav.getModel(), mockRequest, mockResponse);
	}
	
	@Test
	public void viewIOExceptionTest4() throws Exception {
		putGetFileTest();
		mockRequest = new MockHttpServletRequest();
		mockResponse = new MockHttpServletResponse();
		mockRequest.setRequestURI("/test/test.txt");
		mockResponse.getOutputStream().close();
		ModelAndView mav = controller.delete(mockRequest, mockResponse);
		mav.getView().render(mav.getModel(), mockRequest, mockResponse);
	}
}
