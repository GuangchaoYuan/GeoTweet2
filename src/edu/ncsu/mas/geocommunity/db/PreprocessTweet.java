package edu.ncsu.mas.geocommunity.db;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashSet;

import edu.ncsu.mas.geocommunity.utils.Constant;

import edu.ncsu.mas.geocommunity.dbmanager.SqlController;


public class PreprocessTweet {
	private String dbName = "guppy";
	private SqlController sqlController;
	private SqlController sqlController2;
	private int fetchSize = 10000;
	private HashSet<String> userList = new HashSet<String>();
	public PreprocessTweet(){
		sqlController = new SqlController(dbName);
		sqlController2 = new SqlController(dbName);
	}
	
	//read the list of users whose number of tweets exceed five
	public void readUserList(){
		sqlController.createConnection();
		String inputFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\users.csv";
		String line = "";
		
		try {
			BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			while((line = reader.readLine())!=null){
				String[] words = line.split(Constant.SEPARATOR_COMMA);
				if(words.length != 2){
					System.out.println("There is an error in reading file");
					continue;
				}			
				userList.add(words[0]);
			}
			reader.close();
			System.out.println("Finish reading the user list");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
	
	//Delete those tweets whose authors are not in the user list
	public void deleteTweet(){
		int num = 0;
		int numDelete = 0;
		String select = "select tweet_id, user_id from tweet_details_new limit ?," + fetchSize;
		String delete = "delete from tweet_details_new where tweet_id = ?";
		long tweetId = 0L;
		long userId = 0L;
		String userString = "";
		try {
			sqlController.createConnection();
			sqlController2.createConnection();
			PreparedStatement prepStmt = sqlController.GetPreparedStatement(select);
			PreparedStatement prepStmt2 = sqlController2.GetPreparedStatement(delete);
			ResultSet rs = null;
			
			while(num % fetchSize ==0){
				prepStmt.setInt(1, num);
				rs = prepStmt.executeQuery();
				while(rs.next()){
					tweetId = rs.getLong(1);
					userId = rs.getLong(2);
					userString = String.valueOf(userId);
					
					//delete tweets of this user
					if(!userList.contains(userString)){
						prepStmt2.setLong(1, tweetId);
						sqlController2.addBatch(prepStmt2);
						numDelete++;
					}
					
					num++;
					if(num % 10000 == 0)
						System.out.println(num + " records have been processed!");

				}
			}
			
			sqlController2.executeBatch(prepStmt2);
			rs.close();
			System.out.println(numDelete + " records have been deleted!");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args){
		PreprocessTweet pt = new PreprocessTweet();
		pt.readUserList();
		pt.deleteTweet();
	}

}
