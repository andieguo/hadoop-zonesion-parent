<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>

<!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN">
<html>
  <head>
    
    
	<meta http-equiv="pragma" content="no-cache">
	<meta http-equiv="cache-control" content="no-cache">
	<meta http-equiv="expires" content="0">    
	<meta http-equiv="keywords" content="keyword1,keyword2,keyword3">
	<meta http-equiv="description" content="This is my page">
	<!--
	<link rel="stylesheet" type="text/css" href="styles.css">
	-->

  </head>
  
  <body>
  	<h3>Word Count Example:</h3>
    <form action="servlet/RunWCJobServlet">
    	&nbsp;&nbsp;&nbsp;&nbsp;Input &nbsp;Path:<input type="text" name="input" size="50"/>
    	<br>
    	&nbsp;&nbsp;&nbsp;&nbsp;Output Path:<input type="text" name="output" size="50" />
    	<br>
    	&nbsp;&nbsp;&nbsp;&nbsp;<input type="submit" value="Submit"/>
    	<input type="reset" value="Reset"/>
    </form>
  </body>
</html>
