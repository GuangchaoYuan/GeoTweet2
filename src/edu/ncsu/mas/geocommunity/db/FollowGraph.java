package edu.ncsu.mas.geocommunity.db;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import edu.ncsu.mas.geocommunity.dbmanager.SqlController;
import edu.ncsu.mas.geocommunity.utils.Constant;

public class FollowGraph {
	private String dbName = "guppy";
	private SqlController sqlController;
	HashMap<String, ArrayList<String>> adj = new HashMap<String, ArrayList<String>>();
	private final int fetchSize = 1000;
	
	public FollowGraph(){
		sqlController = new SqlController(dbName);
	}
	
	public void createAdjacencyList(int threshold){
		String inputFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\follow\\follow_table.csv";
		String storeFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\follow\\follow_adj_list_"+String.valueOf(threshold)+".csv";
		BufferedReader reader = null;
		String line = "";
		int num = 0;
		try {
			reader = new BufferedReader(new FileReader(inputFile));
			while((line=reader.readLine())!=null && num < 10000){
				num++;
				String[] words = line.split(Constant.SEPARATOR_COMMA);
				if(words.length != 2){
					System.out.println("There is an error in reading file");
					continue;
				}
				String v1 =words[0];
				String v2 = words[1];
				
				//add neighbors
				addNeighbor(v1, v2);
				addNeighbor(v2, v1);
			}
			reader.close();
			this.printAdjacencyList(storeFile, threshold);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//add an edge into a graph
	private void addNeighbor(String v1, String v2){
		if(adj.containsKey(v1))
			adj.get(v1).add(v2);
		else{
			ArrayList<String> list = new ArrayList<String>();
			list.add(v2);
			adj.put(v1, list);
		}
	}
	
	//print the 2-hop nodes as well as their source node
	private void printAdjacencyList(String output, int threshold){
		String key = "";
		ArrayList<String> list = new ArrayList<String>();
		int num = 0;
		BufferedWriter writer  = null;
		try {
			writer = new BufferedWriter(new FileWriter(output));
			Iterator it = adj.entrySet().iterator();
			while(it.hasNext()){
				num++;
				Map.Entry<String, ArrayList<String>> pairs = (Map.Entry<String, ArrayList<String>>)it.next();
				key = pairs.getKey();
				list = pairs.getValue();
				//skip the source nodes whose degree is less than the threshold
				if(list.size()<threshold)
					continue;
				writer.append(key + Constant.SEPARATOR_COMMA + list.get(0));
				if(list.size()>1){
					for(int i = 1; i < list.size(); i++){
						writer.append(Constant.SEPARATOR_COMMA +list.get(i));
					}
				}
				writer.append("\n");
				writer.flush();
			}
			System.out.println(num + " sources nodes that have the adjacency list ");
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void observation(){
		String inputFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\follow\\follow_adj_list_10.csv";
		String storeFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\follow\\observation.csv";
		String select = "select tweet_id, creation_time from guppy.tweet_details_new where user_id = ?";
		String query = "select tweet_id, user_id from guppy.tweet_details_new where creation_time > ? and creation_time < ?";
		HashSet<String> adjList = new HashSet<String>();
		BufferedReader reader = null;
		BufferedWriter writer = null;
		String line = "";
		int num = 0;
		Long sourceId = 0L;
		Long targetId = 0L;
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Timestamp creationTime;
		Timestamp lowerTime;
		Timestamp upperTime;
		int year = 0;
		int month = 0;
		int day = 0;
		int hour = 0;
		sqlController.createConnection();
		PreparedStatement prepStmt = sqlController.GetPreparedStatement(select);
		PreparedStatement prepStmt2 = sqlController.GetPreparedStatement(query);
		ResultSet rs = null;
		ResultSet rs2 = null;
		try {
			reader = new BufferedReader(new FileReader(inputFile));
			writer = new BufferedWriter(new FileWriter(storeFile));
			while((line=reader.readLine())!=null){
				num++;
				String[] words = line.split(Constant.SEPARATOR_COMMA);
				sourceId = Long.valueOf(words[0]);
				for(int i = 1; i < words.length; i++)
					adjList.add(words[i]);
				
				try {
					prepStmt.setLong(1, sourceId);
					rs = prepStmt.executeQuery();
					while(rs.next()){
						Long sourceTweetId = rs.getLong(1);
						creationTime = rs.getTimestamp(2);
						//System.out.println("source-creation-time: " + creationTime);
						
						String timeString = creationTime.toString();
						//System.out.println("source-time-String: " + timeString);
						//Calendar cal = Calendar.getInstance();
						//cal.setTimeInMillis(creationTime.getTime());
						String[] times = timeString.split(Constant.SEPARATOR_SPACE);
						String[] dates = times[0].split("-");
						year = Integer.valueOf(dates[0]);
						month = Integer.valueOf(dates[1]);
						day = Integer.valueOf(dates[2]);
						String[] days = times[1].split(":");
						hour = Integer.valueOf(days[0]);
						//System.out.println("year: " + year + " month: " + month + " day: " + day + " hour: " + hour);
						Date lowerDate =  (Date) formatter.parse(String.valueOf(year)+"-"+String.valueOf(month)+"-"+String.valueOf(day)+" " + String.valueOf(hour)+":00:00");
						Date upperDate = (Date) formatter.parse(String.valueOf(year)+"-"+String.valueOf(month)+"-"+String.valueOf(day)+" " + String.valueOf(hour+1)+":00:00");
						
						lowerTime = new Timestamp(lowerDate.getTime());
						upperTime = new Timestamp(upperDate.getTime());
						//System.out.println("target-lowerTime: " + lowerTime);
						//System.out.println("target-upperTime: " + upperTime);
						//query tweets within a time frame
						prepStmt2.setTimestamp(1, lowerTime);
						prepStmt2.setTimestamp(2, upperTime);
						rs2 = prepStmt2.executeQuery();
						while(rs2.next()){
							Long targetTweetId = rs2.getLong(1);
							targetId = rs2.getLong(2);
							if(adjList.contains(String.valueOf(targetId))){
								writer.append(String.valueOf(sourceTweetId) +Constant.SEPARATOR_COMMA+ words[0]+Constant.SEPARATOR_COMMA + 
										timeString + Constant.SEPARATOR_COMMA + String.valueOf(targetTweetId)+Constant.SEPARATOR_COMMA +String.valueOf(targetId) );
								writer.append('\n');
								writer.flush();
							}
						}
					}
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				adjList.clear();
				System.out.println(num+"records have been processed");
				
			}
			writer.close();
			reader.close();
			sqlController.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	public static void main(String[] args){
		FollowGraph fg = new FollowGraph();
		//fg.createAdjacencyList(10);
		fg.observation();
	}

}
