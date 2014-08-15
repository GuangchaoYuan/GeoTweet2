package edu.ncsu.mas.geocommunity.dbmanager;

import java.io.FileInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

public class SqlController {
	
	private Connection conn = null;
    private Statement stmt = null;
 
    private static String url = "";
    private static String serverName= "";
    private static String portNumber = "";
    private static String databaseName= "";
    private static String userName = "";
    private static String passWord = "";   
    private static int batchCount=0;
    private static int batchLimit = 10000;
    private static int fetchLimit = 10000;
    private static String propertyFile = "config/database.properties"; 
    private static boolean rollback = false;
    
    public SqlController(String db){
    	databaseName = db;
    }
    
    //Create a connection string in the form "jdbc:mysql://localhost:3306/databasename"
    private String getConnectionUrl(){
 	   Properties sqlProperties = new Properties();
 	   try {
 		   sqlProperties.load(new FileInputStream(propertyFile));
 	   } catch (Exception e) {
 		   e.printStackTrace();
 	   }
 	   url = sqlProperties.getProperty("url");
 	   serverName = sqlProperties.getProperty("serverName");
 	   portNumber = sqlProperties.getProperty("portNumber");
 	   userName = sqlProperties.getProperty("userName");
 	   passWord =sqlProperties.getProperty("passWord"); 
 	   batchLimit = Integer.parseInt(sqlProperties.getProperty("batchLimit"));
 	   fetchLimit = Integer.parseInt(sqlProperties.getProperty("fetchLimit"));
 	   return url+serverName+":"+portNumber+"/"+databaseName+"?UseUnicode=true&rewriteBatchedStatements=true";   
 	}
	
	public boolean createConnection()
	{
		try 
		{
           Class.forName("com.mysql.jdbc.Driver");              
        }
		catch(Exception ex) 
		{
            System.out.println("Cannot Load Driver!");
            ex.printStackTrace();
            return false;
        }
		try
		{		   
			conn = DriverManager.getConnection(getConnectionUrl(),userName,passWord);
			stmt = conn.createStatement(java.sql.ResultSet.TYPE_FORWARD_ONLY,  
	                java.sql.ResultSet.CONCUR_READ_ONLY);
			stmt.setFetchSize(fetchLimit);
		
        	if(stmt == null) 
        	{
        		throw new Exception("Creating statement failed!");
        	}
        	
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
			return false;
		}
		return true;
	}
	
	public ResultSet query(String query)
	{
		try 
		{
			ResultSet rs = stmt.executeQuery(query);
			return rs;
		} 
		catch (SQLException e) 
		{
			e.printStackTrace();
			return null;
		}
	}
	
	public PreparedStatement GetPreparedStatement(String sql){
		try {
			return conn.prepareStatement(sql);
		} catch (SQLException e) {
			e.printStackTrace();
			return null;
		}
	}
	
	public void addBatch(PreparedStatement prepStmt)
	{
		try
		{
			if(batchCount<batchLimit)
			{
				batchCount++;
				prepStmt.addBatch();
			}
			else
			{
				executeBatch(prepStmt);
				prepStmt.addBatch();
				batchCount = 1;
			}
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	public int[] executeBatch(Statement stmt)
	{
		try
		{
			int[] status = stmt.executeBatch();
			clearBatch(stmt);
			return status;
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
			return null;
		}
	}
	
	public void clearBatch(Statement stmt)
	{
		try
		{
			stmt.clearBatch();
			batchCount = 0;
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
	
	public void update(String query)
	{
		try
		{
			stmt.executeUpdate(query);
		}
		catch (Exception ex)
		{
		   //System.out.println(query);
			ex.printStackTrace();
		}
	}
	
	public void close()
	{
		try
		{
			stmt.close();
			conn.close();
		}
		catch (Exception ex)
		{
			
		}
	}

}
