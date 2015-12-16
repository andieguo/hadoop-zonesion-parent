package org.zonesion.hadoop.servlet;

import java.io.IOException;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.zonesion.hadoop.mr.WordCount;

public class GetMapRedInfoServlet extends HttpServlet {

	private static final long serialVersionUID = 1L;

	public GetMapRedInfoServlet() {
		super();
	}

	@Override
	public void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
		String map = "";
		String reduce = "";
		map = String.valueOf(WordCount.job.mapProgress());
		reduce = String.valueOf(WordCount.job.reduceProgress());
		RequestDispatcher dispatcher = request.getRequestDispatcher(// basePath+
				"/" + "bottom_print.jsp?map=" + map + "&reduce=" + reduce);
		dispatcher.forward(request, response);
		// return;
	}

	@Override
	public void doPost(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {

		this.doGet(request, response);
	}

}
