package edu.ncsu.mas.geocommunity.db;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import edu.ncsu.mas.geocommunity.dbmanager.SqlController;
import edu.ncsu.mas.geocommunity.utils.Constant;

public class MentionGraph {
	private String dbName = "guppy";
	private SqlController sqlController;
	private final int fetchSize = 1000;
	private HashSet<String> userIdSet = new HashSet<String>();
	private HashMap<String, String> userIdMap = new HashMap<String, String>();
	private HashMap<String, Integer> mentionMap = new HashMap<String, Integer>();
	public MentionGraph(){
		sqlController = new SqlController(dbName);
	}
	
	private void readMapping(String inputFile){
		BufferedReader reader = null;
		String line = "";
		int num = 0;
		try {
			reader = new BufferedReader(new FileReader(inputFile));
			while((line = reader.readLine())!=null){
				num++;
				if(!userIdSet.contains(line))
					userIdSet.add(line);
				
				if(num % 1000 == 0)
					System.out.println(num + " records have been read!");
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public void userIdScreenNameMapping(){
		String inputFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\dic_uid_tweet.csv";
		String storeFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\userIdScreenNameMapping2.csv";
		this.readMapping(inputFile);
		BufferedWriter writer = null;
		long userId = 0L;
		String userString = "";
		String screenName = "";
		String select = "select user_id, screen_name from guppy.tweet_users limit ?," + fetchSize; 
		ResultSet rs = null;
		int num = 0;
		sqlController.createConnection();
		PreparedStatement prepStmt = sqlController.GetPreparedStatement(select);
		try {
			writer = new BufferedWriter(new FileWriter(storeFile));
			while(num % fetchSize ==0){
				prepStmt.setInt(1, num);
				rs = prepStmt.executeQuery();
				while(rs.next()){
					num++;
					userId = rs.getLong(1);
					userString = String.valueOf(userId);
					screenName = rs.getString(2);
					if(userIdSet.contains(userString))
					{
						writer.append(userString + Constant.SEPARATOR_COMMA + screenName);
						writer.append('\n');
						writer.flush();
						userIdSet.remove(userString);
					}
					if(userIdSet.size()==0)
						break;
				}
				System.out.println(num + " records have been processed!");
				if(userIdSet.size()==0)
					break;
			}
			writer.close();
			sqlController.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private void readUserIdScreenNameMapping(String inputFile){
		BufferedReader reader = null;
		String line = "";
		String userId = "";
		String screenName = "";
		int num = 0;
		try {
			reader = new BufferedReader(new FileReader(inputFile));
			while((line = reader.readLine())!=null){
				num++;
				String[] words = line.split(Constant.SEPARATOR_COMMA);
				userId = words[0];
				screenName = words[1];
				if(!userIdMap.containsKey(screenName))
					userIdMap.put(screenName, userId);
				
				if(num % 1000 == 0)
					System.out.println(num + " records have been read!");
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
	}
	
	public void createMentionGraph(){
		String inputFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\userIdScreenNameMapping.csv";
		String storeFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\mentionGraph.csv";
		this.readUserIdScreenNameMapping(inputFile);
		String select = "select text, user_id " +
				"from guppy.tweet_details_new limit ?,"+fetchSize;
		
		sqlController.createConnection();
		PreparedStatement prepStmt = sqlController.GetPreparedStatement(select);
		ArrayList<String> mentionNames;
		String text = "";
		Long source = 0L;
		String target = "";
		String sourceString = "";
		String mentionKey = "";
		int mentionWeight = 0;
		ResultSet rs = null;
		int num = 0;
		try {
			while(num % fetchSize ==0){
				prepStmt.setInt(1, num);
				rs = prepStmt.executeQuery();
				while(rs.next()){
					num++;
					text = rs.getString(1);
					source = rs.getLong(2);
					sourceString = String.valueOf(source);
					mentionNames = extractScreenNameFromMention(text);
					//put the mention-mapping into the hashmap
					for(int i = 0; i < mentionNames.size(); i++){
						String name = mentionNames.get(i);
						
						if(userIdMap.containsKey(name))
							target = userIdMap.get(name);
						else{
							System.out.println("num: " + num + " screenName: " + name + " isn't in the mapping");
							continue;
						}
						
						//skip the self-loop
						if(sourceString.equals(target))
							continue;
						mentionKey = sourceString + Constant.SEPARATOR_HYPHEN + target;
						if(mentionMap.containsKey(mentionKey)){
							mentionWeight = mentionMap.get(mentionKey);
							mentionMap.put(mentionKey, mentionWeight+1);
						}
						else
							mentionMap.put(mentionKey, 1);
					}
					
					mentionNames.clear();
				}
				System.out.println(num + " records have been processed!");
			}
			sqlController.close();
			this.printMap(mentionMap, storeFile);
				
		}  catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//print the hashmap to a file
	private void printMap(HashMap<String, Integer> map, String storeFile){
		BufferedWriter writer  = null;
		String key = "";
		int value = 0;
		int num = 0;
		try {
			writer = new BufferedWriter(new FileWriter(storeFile));
			Iterator it = map.entrySet().iterator();
			while(it.hasNext()){
				num++;
				Map.Entry<String, Integer> pairs = (Map.Entry<String, Integer>)it.next();
				key = pairs.getKey();
				value = pairs.getValue();
				String[] nodes = key.split(Constant.SEPARATOR_HYPHEN);
				writer.append(nodes[0] + Constant.SEPARATOR_COMMA + nodes[1]);
				writer.append(Constant.SEPARATOR_COMMA + Integer.toString(value));
				writer.append("\n");
				writer.flush();
				
				if(num % 10000 == 0)
					System.out.println(num + " records have been written!");
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private HashMap<String, Integer> readMentionTable(){
		String inputFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\mentionTable.csv";
		String line = "";
		String key = "";
		HashMap<String, Integer> mMap = new HashMap<String, Integer>();
		int num = 0;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			while((line = reader.readLine())!=null){
				num++;
				String[] words = line.split(Constant.SEPARATOR_COMMA);
				if(words.length != 3){
					System.out.println("There is an error in reading file");
					continue;
				}
				key = words[0]+Constant.SEPARATOR_HYPHEN+words[1];
				mMap.put(key, Integer.parseInt(words[2]));
			}
			reader.close();
			System.out.println("There are " + num + " records in the mention table");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		}
		
		return mMap;
	}
	
	//filter a mention graph based on the specified weight threshold
	public void filterMentionGraph(int weight){
		HashMap<String, Integer> mMap = new HashMap<String, Integer>();
		HashSet<String> edge = new HashSet<String>();
		mMap = readMentionTable();
		String inputFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\mentionTable.csv";
		String storeFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\mutual_mention_graph_DB_"+String.valueOf(weight)+".csv";
		String line = "";
		String key = "";
		int value = 0;
		int num = 0;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			BufferedWriter writer = new BufferedWriter(new FileWriter(storeFile));
			while((line = reader.readLine())!=null){
				String[] words = line.split(Constant.SEPARATOR_COMMA);
				if(words.length != 3){
					System.out.println("There is an error in reading file");
					continue;
				}
				
				//skip the record whose weight is less than the weight threshold
				value = Integer.parseInt(words[2]);
				if(value<weight)
					continue;
				
				//search for its reverse edge
				key = words[1]+Constant.SEPARATOR_HYPHEN+words[0];
				//avoid to process the reverse edge again: the relationship is symmetric
				if(!edge.contains(words[0]+Constant.SEPARATOR_HYPHEN+words[1])){
					if(mMap.containsKey(key) && (value=mMap.get(key))>=weight){
						num++;
						writer.append(words[0]);
						writer.append(Constant.SEPARATOR_COMMA);
						writer.append(words[1]);
						writer.append(Constant.SEPARATOR_COMMA);
						writer.append(words[2]);
						writer.append(Constant.SEPARATOR_COMMA);
						writer.append(String.valueOf(value));
						writer.append("\n");
						writer.flush();
						
						edge.add(key);
						if(num % 100 == 0)
							System.out.println(num + " mutal edges have been processed!");
					}
				}	
			}
			reader.close();
			writer.close();
			System.out.println("There are "+num + " mutal edges in total!");
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		}
		
		
	}
	
	private ArrayList<String> extractScreenNameFromMention(String s) {
		//String[] words = s.split(Constant.SEPARATOR_SPACE);
		
		ArrayList<String> names = new ArrayList<String>();
		
		Pattern pattern = Pattern.compile("@([A-Za-z0-9_]+)");
		Matcher matcher = null;
		int start = 0;
		int end = 0;
			
		matcher = pattern.matcher(s);
		while(matcher.find()){
			start = matcher.start()+1;
			end = matcher.end();
			names.add(s.substring(start, end));
		}
			
		return names;
	}
	
	//read nodes from a graph to construct a hashset
	private HashSet<String> readNodesFromGraph(String input) {
		String line = "";
		HashSet<String> nodes = new HashSet<String>();
		BufferedReader reader =  null;
		try {
			reader = new BufferedReader(new FileReader(input));
			while ((line = reader.readLine()) != null){
				String[] words = line.split(Constant.SEPARATOR_COMMA);
				if(words.length != 4){
					System.out.println("There is an error in reading file");
					continue;
				}
				nodes.add(words[0]);
				nodes.add(words[1]);
			}
			reader.close();
				
		}catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return nodes;
	}
	
	//print the node hashset (each element is a node)
	private void printNodeSet(HashSet<String> map, String output){
		BufferedWriter writer  = null;
		String storeFile = output;
		String key = "";
		int num = 0;
		try {
			writer = new BufferedWriter(new FileWriter(storeFile));
			Iterator it = map.iterator();
			while(it.hasNext()){
				num++;
				key = (String) it.next();
				writer.append(key);
				writer.append("\n");
				writer.flush();
			}
			writer.close();
			System.out.println(num + " nodes in the mutual-mention graph!");
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void findNodeSet(int weight){
		String inputFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\mutual_mention_graph_DB_"+String.valueOf(weight)+".csv";
		String storeFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\mentionGraphNodeList.csv";
		HashSet<String> nodes = this.readNodesFromGraph(inputFile);
		this.printNodeSet(nodes, storeFile);
	}
	
	public static void main(String[] args){
		MentionGraph mg = new MentionGraph();
		//mg.userIdScreenNameMapping();
		//mg.createMentionGraph();
		//mg.filterMentionGraph(1);
		mg.findNodeSet(1);
	}

}
