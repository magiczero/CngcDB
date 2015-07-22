package com.yanghaipeng.bin;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class Start {
	private static final int _zero = 0;
	private static final int _one = 1;
	private static final int _stationid = 81;
	private static final int _columnid = 1239;
	private static final String _free = "free";
	
	private static Connection getMySqlConn() {
		Connection conn = null;
		try {
			Class.forName("com.mysql.jdbc.Driver");
			conn = DriverManager.getConnection("jdbc:mysql://192.168.10.11:3396/t9","sqlroot","aaa111");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("找不到mysql驱动程序类 ，加载驱动失败！");   
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conn;
	}
	
	private static Connection getSqlServerConn() {
		Connection conn = null;
		try {
			//Class.forName("net.sourceforge.jtds.jdbc.Driver");
			Class.forName("com.microsoft.jdbc.sqlserver.SQLServerDriver").newInstance();
			conn = DriverManager.getConnection("jdbc:microsoft:sqlserver://192.168.4.188:1433;DatabaseName=cngc2014","sa","123");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("找不到驱动程序类 ，加载驱动失败！");   
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			System.out.println("aaaaa");
			e.printStackTrace();
		} catch (InstantiationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalAccessException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return conn;
	}
	
	public static void main(String[] args) throws SQLException {
		// TODO Auto-generated method stub
		List<Map<String, String>> newsList = new LinkedList<Map<String, String>>();
		
		Statement stmt =null;
		Connection conSqlserver = getSqlServerConn();
		
		stmt = conSqlserver.createStatement();
		//String sqlstr = "select * from news where id in(select newsid from newsidcollection where typeid=100) and newsdate<'2014-11-17'";
		String sqlstr = "select * from news where id in(select newsid from topicnews where topicid=16)";
		//String sqlstr = "select * from news where id in(select newsid from newsidcollection where typeid=72)";
		//String sqlstr = "select * from news where typeid=65";
		//String sqlstr = "select * from news where id in(select newsid from topicnews where topicid=36)";
		ResultSet result = stmt.executeQuery(sqlstr);
		//ResultSet result = stmt.executeQuery("select top 10 * from news");
		
		while(result.next()) {
			Map<String, String> newsMap = new HashMap<String, String>();
			newsMap.put("id", String.valueOf(result.getInt("id")));
			newsMap.put("title", result.getString("title"));
			newsMap.put("subtitle", result.getString("subtitle"));
			newsMap.put("content", result.getString("content"));
			newsMap.put("newsdate", result.getDate("newsdate").toString());
			newsMap.put("hitnum", String.valueOf(result.getInt("hitcount")));
			newsMap.put("istop", String.valueOf(result.getBoolean("istop")));

			newsList.add(newsMap);
		}
		
		result.close();
		conSqlserver.close();
		
		if(newsList.size() > 0) {
			
			String sql = "INSERT INTO cms_content(content_name, content_title, content_date, STATION_ID, COLUMN_ID, content, create_id, create_time, content_type,content_status, content_top, content_index, BROWSE_COUNT, relative_id, ISPUB, COMMENT_STYLE, recommend_id, is_reivce, thumbnail, attach_url) values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) ";
			Connection conMysql = getMySqlConn();
			
			int count = 0;
			for(Map<String, String> news : newsList) {
				PreparedStatement pstmt = conMysql.prepareStatement(sql);
				
				//System.out.println("插入新闻：" + news.get("title"));
				
				pstmt.setString(1, news.get("title"));
				pstmt.setString(2, news.get("subtitle"));
				pstmt.setDate(3, Date.valueOf(news.get("newsdate")));
				pstmt.setInt(4,_stationid);
				pstmt.setInt(5, _columnid);
				pstmt.setString(6, news.get("content"));
				pstmt.setInt(7, _one);
				pstmt.setDate(8, new Date(System.currentTimeMillis()));
				pstmt.setInt(9,_one);
				pstmt.setInt(10, _zero);
				pstmt.setBoolean(11,Boolean.valueOf(news.get("istop")));
				pstmt.setInt(12, Integer.valueOf(news.get("id")));
				pstmt.setInt(13, Integer.valueOf(news.get("hitnum")));
				pstmt.setInt(14, _zero);
				pstmt.setInt(15, _zero);
				pstmt.setString(16, _free);
				pstmt.setInt(17, _zero);
				pstmt.setInt(18, _zero);
				pstmt.setString(19, "/cngc1030/attach/");
				pstmt.setString(20, "");
				
				pstmt.executeUpdate();
				pstmt.close();
				System.out.println(++count);
			}
			
			conMysql.close();
		}
		
		System.out.println("结束");
	}

}
