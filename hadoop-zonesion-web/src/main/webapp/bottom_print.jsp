<%@ page language="java" import="java.util.*" pageEncoding="UTF-8"%>

<html>
  

  
  <%
	String mapPer=(String)request.getParameter("map");
	String redPer=(String)request.getParameter("reduce");
	double map,reduce;
	map=Double.parseDouble(mapPer);
	reduce=Double.parseDouble(redPer);
   %>
  
  
  <body>
  	 <h3>Map Reduce Progress</h3> <br>
    
    <table border="1">
    	<tr>
    		<th>Map Progress</th>
    		<td><%=map*100 %>%</td>
    	</tr>
    	<tr>
    		<th>Reduce Progress</th>
    		<td><%=reduce*100 %>%</td>
    	</tr>
    </table>
    
    
    
   <%
   if(reduce<1.0){  // 没有完成任务则进行跳转
   // 跳转到servlet 获取map和reduce执行情况 
  	 response.setHeader("refresh","3;URL=GetMapRedInfoServlet"); 
  	}else{
   %>
   <h3>&nbsp;&nbsp;&nbsp;&nbsp;The Job is done...</h3>
   <%} %>
  </body>
</html>
