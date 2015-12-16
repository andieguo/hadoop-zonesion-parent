package org.zonesion.hadoop.servlet;

import java.io.IOException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;

import org.zonesion.hadoop.util.RunJobThread;


public class RunWCJobServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	public RunWCJobServlet() {
		super();
	}

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		String input=request.getParameter("input");
	 	String output=request.getParameter("output");
	 	new Thread(new RunJobThread(new String[]{input,output})).start();
	 	HttpSession session=request.getSession();
	 	session.setAttribute("jobName", "word count by input-"+input);
	 //	String basePath=request.getContextPath();
	 	RequestDispatcher dispatcher = request.getRequestDispatcher(
	 			"/"+"bottom_print.jsp?map=0&reduce=0"); 
	 	dispatcher .forward(request, response); 
	// 	return;
	}

	@Override
	public void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {

		this.doGet(request, response);
	}

}
