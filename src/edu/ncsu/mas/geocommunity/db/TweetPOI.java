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
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.json.JSONArray;
import org.json.JSONObject;

import edu.ncsu.mas.geocommunity.dbmanager.SqlController;
import edu.ncsu.mas.geocommunity.googleplace.GooglePoi;
import edu.ncsu.mas.geocommunity.googleplace.GpsCoords;
import edu.ncsu.mas.geocommunity.utils.Constant;
import edu.ncsu.mas.geocommunity.db.WorkQueue;

public class TweetPOI {
	private String dbName = "guppy";
	private String dbName2 = "google_places";
	private SqlController sqlController;
	private SqlController sqlController2;
	private int fetchSize = 10;
	protected HashMap<String, HashSet<String>> adjMap = new HashMap<String, HashSet<String>>();
	HashMap<String, HashSet<String>> activityMap = new HashMap<String, HashSet<String>>();
	public TweetPOI(){
		sqlController = new SqlController(dbName);
		sqlController2 = new SqlController(dbName2);
	}
	
	//query the POI for each tweet
	public void queryPOI(double distance){
		int num = 400000;
		long tweetId = 0L;
		long userId = 0L;
		String geoLocation = "";
		String s1 = "";
		String s2 = "";
		String select = "select tweet_id, AsText(geo_location), user_id from guppy.tweet_details_new limit ?," + fetchSize;
		String storeFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\tweetPOI.csv";
		BufferedWriter writer  = null;
		try {
			writer = new BufferedWriter(new FileWriter(storeFile));
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
		try {
			sqlController.createConnection();
			PreparedStatement prepStmt = sqlController.GetPreparedStatement(select);
			sqlController2.createConnection();
			ResultSet rs = null;
			while(num % fetchSize ==0 && num<1100000){
				prepStmt.setInt(1, num);
				rs = prepStmt.executeQuery();
				while(rs.next()){
					tweetId = rs.getLong(1);
					geoLocation = rs.getString(2);
					userId = rs.getLong(3);
					
					//process geoLocation String
					String[] geo = geoLocation.split(Constant.SEPARATOR_SPACE);
					int first = geo[1].indexOf(")");
					int second = geo[0].indexOf("(");
					s1 = geo[1].substring(0,first);
					s2 = geo[0].substring(second+1);
					
					GpsCoords gps = new GpsCoords(Double.valueOf(s1), Double.valueOf(s2));
					TreeSet<GooglePoi> TS = this.getNearbyPlaces(gps, distance);
					this.writeResultIntoFile(writer, tweetId, userId, TS);
					
					num++;
					if(num % 10000 == 0)
						System.out.println(num + " records have been processed!");

				}
			}
			rs.close();
			writer.close();
			sqlController.close();
			sqlController.close();
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//write the POI for each tweet from a file to DB
	public void writePOIIntoDB(){
		sqlController.createConnection();
		String inputFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\tweetPOI_part1.csv";
		String line = "";
		long tweetId = 0L;
		long userId = 0L;
		String poiName = "";
		String type = "";
		int num = 0;
		
		WorkQueue queue = new WorkQueue(2);
		try{
			BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			while((line = reader.readLine())!=null){
				num++;
				String[] words = line.split(Constant.SEPARATOR_COMMA);
				if(words.length != 4){
					System.out.println("There is an error in reading file in line: " + num);
					continue;
				}	
				tweetId = Long.parseLong(words[0]);
				userId = Long.parseLong(words[1]);
				poiName = words[2];
				type = words[3];
				queue.execute(new TweetPOIDBRunnable(num, tweetId, userId, poiName, type, sqlController));
			}
			
			Thread.sleep(Integer.MAX_VALUE);
			reader.close();
			sqlController.close();
			
		}
		catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch(NumberFormatException e){
			e.printStackTrace();
		} 
		
	}
	
	//clean a String
	private String cleanString(String s){
		String sNew = s;
		sNew = sNew.replaceAll("[^A-Za-z']", "");
		return sNew;
		
	}
	
	//clean a String set (a String with multiple spaces)
	private String cleanStringSet(String s){
		String[] sSet = s.split(Constant.SEPARATOR_SPACE);
		String sNew = "";
		for(int i = 0; i < sSet.length; i++){
			sSet[i] = sSet[i].replaceAll("[^A-Za-z0-9]", "");
			if(i==0)
				sNew = sSet[i];
			else
				sNew = sNew + Constant.SEPARATOR_SPACE + sSet[i];
		}
		
		return sNew;
	}
	
	//write the POI for each tweet into a file
	private void writeResultIntoFile(BufferedWriter writer, Long tweetId, Long userId, TreeSet<GooglePoi> pois){
		try {
			if(pois.size()==0){
				writer.append(tweetId + Constant.SEPARATOR_COMMA + userId + Constant.SEPARATOR_COMMA);
				writer.append("empty"+ Constant.SEPARATOR_COMMA + "empty");
				writer.append('\n');
			}
			else{
				for (GooglePoi poi : pois) {
					writer.append(tweetId + Constant.SEPARATOR_COMMA + userId + Constant.SEPARATOR_COMMA+
							cleanStringSet(poi.getName()) + Constant.SEPARATOR_COMMA);
					int size = poi.getTypes().size();
					if(size > 0){
						Iterator<String> it = poi.getTypes().iterator();
						writer.append(cleanString(it.next()));
						//System.out.println(it.next());
						while(it.hasNext()){
							writer.append(Constant.SEPARATOR_SPACE+cleanString(it.next()));
							//System.out.println(it.next());
						}
						
					}
					else
						writer.append("empty");
					
					writer.append('\n');
				}
			}
			writer.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
    /* Get POIs within a certain distance of position. The unit of distance is the
    * one used by Mysql (which I don't know). Approximately, 0.001 is 100m.
    */
	public TreeSet<GooglePoi> getNearbyPlaces(GpsCoords position, double distance) {
		String query ="select X(position_gp), Y(position_gp), poi_json_gp" +
			          " from google_places.google_poi_gp" +
			          " where MBRContains(Buffer(GeomFromText(?), ?), position_gp)";
		TreeSet<GooglePoi> googlePois = new TreeSet<GooglePoi>(new Comparator<GooglePoi>() {
	    @Override
	    public int compare(GooglePoi o1, GooglePoi o2) {
	    	Double dist1 = findDistance(o1.getQueryPosition(), o1.getPoiPosition());
	        Double dist2 = findDistance(o2.getQueryPosition(), o2.getPoiPosition());
	        if (dist1 != dist2) {
	          return dist1.compareTo(dist2);
	        } else {
	          return o1.getName().compareTo(o2.getName());
	        }
	      }
	    });
	
	    try {
	    	PreparedStatement selectPoiRadPrepdStmt = sqlController2.GetPreparedStatement(query);
	    	selectPoiRadPrepdStmt.setString(1,
	          "POINT(" + position.getLongitude() + " " + position.getLatitude() + ")");
	      selectPoiRadPrepdStmt.setDouble(2, distance);
	
	      try {
	    	  ResultSet rs = selectPoiRadPrepdStmt.executeQuery();
	    	  while (rs.next()) {
		          GooglePoi googlePoi = new GooglePoi();
		
		          googlePoi.setQueryPosition(position);
		          googlePoi.setPoiPosition(new GpsCoords(rs.getDouble(2), rs.getDouble(1)));
		
		          JSONObject poiJson = new JSONObject(rs.getString(3));
		          googlePoi.setName(poiJson.getString("name"));
		
		          JSONArray typesJaray = poiJson.getJSONArray("types");
		          for (int i = 0; i < typesJaray.length(); i++) {
		            googlePoi.addType(typesJaray.getString(i));
		          }
		
		          //googlePoi.setVicinity(poiJson.getString("vicinity"));
		
		          googlePois.add(googlePoi);
	        }
	    	rs.close();
	      } catch (SQLException e) {
	        e.printStackTrace();
	      }
	      
	    } catch (SQLException e) {
	      e.printStackTrace();
	    }

	
	    return googlePois;
	  }
	
	public static double findDistance(GpsCoords pos1, GpsCoords pos2) {
		return findDistance(pos1.getLatitude(), pos1.getLongitude(), pos2.getLatitude(),
				pos2.getLongitude());
		}
	  
    /** Returns distance between (lat1, lng1) and (lat2, lng2) in meters. 
    * @param lat1
    * @param lng1
    * @param lat2
    * @param lng2
    * @return Distance in meters between two pairs of coordinates.
    */
	public static double findDistance(double lat1, double lng1, double lat2, double lng2) {
		double earthRadius = 6371; // km
	    double dLat = Math.toRadians(lat2 - lat1);
	    double dLng = Math.toRadians(lng2 - lng1);
	    double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.cos(Math.toRadians(lat1))
	        * Math.cos(Math.toRadians(lat2)) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
	    double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
	    return earthRadius * c * 1000;
	}
	
	//read adjacency list
	private void readAdjMap(String input){
		BufferedReader reader = null;
		String line = "";
		int num = 0;
		String sourceId = "";
		
		try {
			reader = new BufferedReader(new FileReader(input));
			while((line=reader.readLine())!=null){
				num++;
				String[] words = line.split(Constant.SEPARATOR_COMMA);
				sourceId = words[0];
				HashSet<String> list = new HashSet<String>();
				for(int i = 1; i <words.length; i++)
					list.add(words[i]);
				if(!adjMap.containsKey(sourceId)){
					adjMap.put(sourceId, list);
					//System.out.println("sourceId: " + sourceId + " list size: " + list.size());
				}
			}
			reader.close();
			System.out.println(num + " records have been read");
			//System.out.println(" adjMap size after reading " + adjMap.size());
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
	//write the POI for source tweet of obseravtion into a file
	private void writeSourceResultIntoFile(BufferedWriter writer, int index, String userId, String tweetId, String time, TreeSet<GooglePoi> pois){
		try {
			writer.append(String.valueOf(index) + Constant.SEPARATOR_COMMA + userId + Constant.SEPARATOR_COMMA
					+ tweetId + Constant.SEPARATOR_COMMA + time + Constant.SEPARATOR_COMMA);
			if(pois.size()==0){
				writer.append("empty"+ Constant.SEPARATOR_COMMA + "empty");
				writer.append('\n');
			}
			else{
				int num = 0;
				Iterator<GooglePoi> poi = pois.iterator();
				while (poi.hasNext() && num<1) {
					num++;
					GooglePoi temp = poi.next();
					writer.append(cleanStringSet(temp.getName()) + Constant.SEPARATOR_COMMA);
					int size = temp.getTypes().size();
					
					if(size > 0){
						/*Iterator<String> it = temp.getTypes().iterator();
						writer.append(cleanString(it.next()));
						//System.out.println(it.next());
						while(it.hasNext()){
							writer.append(Constant.SEPARATOR_SPACE+cleanString(it.next()));
							//System.out.println(it.next());
						}*/
						String type = this.getActivity(temp.getTypes());
						writer.append(type);
						
					}
					else
						writer.append("empty");
					
					writer.append('\n');
				}
			}
			writer.flush();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//write the POI for target tweet of obseravtion into a file
	private void writeTargetResultIntoFile(BufferedWriter writer, int index, String userId, int friend, String tweetId, TreeSet<GooglePoi> pois){
		try {
			writer.append(String.valueOf(index) + Constant.SEPARATOR_COMMA + userId + Constant.SEPARATOR_COMMA 
					+ String.valueOf(friend) + Constant.SEPARATOR_COMMA+ tweetId + Constant.SEPARATOR_COMMA);
			if(pois.size()==0){
				writer.append("empty"+ Constant.SEPARATOR_COMMA + "empty");
				writer.append('\n');
			}
			else{
				int num = 0;
				Iterator<GooglePoi> poi = pois.iterator();
				while (poi.hasNext() && num<1) {
					num++;
					GooglePoi temp = poi.next();
					writer.append(cleanStringSet(temp.getName()) + Constant.SEPARATOR_COMMA);
					int size = temp.getTypes().size();
					if(size > 0){
						/*Iterator<String> it = temp.getTypes().iterator();
						writer.append(cleanString(it.next()));
						//System.out.println(it.next());
						while(it.hasNext()){
							writer.append(Constant.SEPARATOR_SPACE+cleanString(it.next()));
							//System.out.println(it.next());
						}*/
						String type = this.getActivity(temp.getTypes());
						writer.append(type);
					}
					else
						writer.append("empty");
					
					writer.append('\n');
				}
			}
			writer.flush();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void readActivity(String inputFile){
		BufferedReader reader = null;
		String line = "";
		int num = 0;
		try {
			reader = new BufferedReader(new FileReader(inputFile));
			while((line=reader.readLine())!=null){
				num++;
				String[] words = line.split(Constant.SEPARATOR_COMMA);
				System.out.println("line: " + num + " line size: " + words.length);
				String key = words[0];
				HashSet<String> actSet = new HashSet<String>();
				for(int i = 1; i < words.length; i++){
					actSet.add(words[i]);
				}
				activityMap.put(key, actSet);
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
	
	//decide the activty of the POI type
	private String getActivity(Set<String> type){
		String act = "other";
		Iterator<String> iter = type.iterator();
		String current = "";
		while(iter.hasNext()){
			current = iter.next();
			if(!current.equals("establishment")){
				Iterator it = activityMap.entrySet().iterator();
				while(it.hasNext()){
					Map.Entry<String, HashSet<String>> pairs= (Map.Entry<String, HashSet<String>>) it.next();
					HashSet<String> value = pairs.getValue();
					
					if(value.contains(current)){
						act = pairs.getKey();
						break;
					}
				}
			}
			if(!act.equals("other"))
				break;
		}
		//System.out.println("act: " + act);
		return act;
	}
	
	//search POI for each observation record: d is the query distance, threshold is the number of nonFriends results that needs to be written
	public void poiObservation(double d, int threshold){
		String inputFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\follow\\observation.csv";
		String storeFile1 = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\follow\\observation_poi_source_new.csv";
		String storeFile2= "D:\\Social_Media_Analytics\\Geo_community\\dataset\\follow\\observation_poi_target_new.csv";
		//String adjFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\follow\\follow_adj_list_10.csv";
		String activityFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\follow\\activity.csv";
		//read activity information
		this.readActivity(activityFile);
		//this.readAdjMap(adjFile);
		BufferedReader reader = null;
		BufferedWriter writer1 = null;
		BufferedWriter writer2 = null;
		long sourceTweetId = 0L;
		String line = "";
		int num = 0;
		String geoLocation = "";
		
		String select = "select AsText(geo_location) from guppy.tweet_details_new where tweet_id = ?";
		sqlController.createConnection();
		sqlController2.createConnection();
		PreparedStatement prepStmt = sqlController.GetPreparedStatement(select);
		ResultSet rs = null;
		try {
			reader = new BufferedReader(new FileReader(inputFile));
			writer1 = new BufferedWriter(new FileWriter(storeFile1));
			writer2 = new BufferedWriter(new FileWriter(storeFile2));
			while((line=reader.readLine())!=null){
				num++;
				String[]words = line.split(Constant.SEPARATOR_COMMA);
				if(words.length!=5){
					System.out.println("There is an error in line: " + num);
					continue;
				}
				
				sourceTweetId = Long.valueOf(words[0]);
				
				//query the geo-coordinate of source_tweet
				prepStmt.setLong(1, sourceTweetId);
				rs = prepStmt.executeQuery();
				while(rs.next()){
					geoLocation = rs.getString(1);
					//process geoLocation String
					GpsCoords gps = extractGeoCoordinate(geoLocation);
					TreeSet<GooglePoi> TS = this.getNearbyPlaces(gps, d);
					if(TS.size()==0)
						continue;
					//write source node information into a file
					this.writeSourceResultIntoFile(writer1,num, words[1], words[0], words[2], TS);
					
					//query others' observation
					this.queryOthersObservation(words[1], words[2], words[3], threshold, num, d, writer2);
				}
				
				System.out.println(num + " records have been processed");
			}
			reader.close();
			writer1.close();
			writer2.close();
			sqlController.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
    //extract geo-coordinate from geoLocation String
	private GpsCoords extractGeoCoordinate(String geoLocation) {
		String[] geo = geoLocation.split(Constant.SEPARATOR_SPACE);
		int first = geo[1].indexOf(")");
		int second = geo[0].indexOf("(");
		String s1 = geo[1].substring(0,first);
		String s2 = geo[0].substring(second+1);
		
		GpsCoords gps = new GpsCoords(Double.valueOf(s1), Double.valueOf(s2));
		return gps;
	}
	
	private void queryOthersObservation(String sourceId, String timeString, String friendTweetId, int threshold, int index, double distance, BufferedWriter writer){
		int year = 0;
		int month = 0;
		int day = 0;
		int hour = 0;
		long tweetId = 0L;
		long userId = 0L;
		int friend = 0; //1 means friend and 0 means nonFriends
		int numNonFriend = 0;
		int numFriend = 0;
		HashSet<String> list = new HashSet<String>();
		String geoLocation = "";
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		Timestamp creationTime;
		Timestamp lowerTime;
		Timestamp upperTime;
		String query = "select tweet_id, user_id, AsText(geo_location) from guppy.tweet_details_new where creation_time > ? and creation_time < ?";
		PreparedStatement prepStmt2 = sqlController.GetPreparedStatement(query);
		ResultSet rs2 = null;
		
		String[] times = timeString.split(Constant.SEPARATOR_SPACE);
		String[] dates = times[0].split("-");
		year = Integer.valueOf(dates[0]);
		month = Integer.valueOf(dates[1]);
		day = Integer.valueOf(dates[2]);
		String[] days = times[1].split(":");
		hour = Integer.valueOf(days[0]);
		
		try {
			Date lowerDate = (Date) formatter.parse(String.valueOf(year)+"-"+String.valueOf(month)+"-"+String.valueOf(day)+" " + String.valueOf(hour)+":00:00");
			Date upperDate = (Date) formatter.parse(String.valueOf(year)+"-"+String.valueOf(month)+"-"+String.valueOf(day)+" " + String.valueOf(hour+1)+":00:00");
			
			lowerTime = new Timestamp(lowerDate.getTime());
			upperTime = new Timestamp(upperDate.getTime());
			
			prepStmt2.setTimestamp(1, lowerTime);
			prepStmt2.setTimestamp(2, upperTime);
			rs2 = prepStmt2.executeQuery();
			while(rs2.next()){
				tweetId = rs2.getLong(1);
				userId = rs2.getLong(2);
				geoLocation = rs2.getString(3);
				GpsCoords gps = extractGeoCoordinate(geoLocation);
				TreeSet<GooglePoi> TS = this.getNearbyPlaces(gps, distance);
				
				//search for friend list
				friend = 0;
				if(friendTweetId.equals(String.valueOf(tweetId)))
					friend = 1;
				/*if(adjMap.containsKey(sourceId)){
					list = adjMap.get(sourceId);
					//System.out.println("sourceId: " + sourceId + " list size: " + list.size());
					if(list.contains(String.valueOf(userId))){
						friend = 1;
						System.out.println("sourceId: " + sourceId + " userId: " + String.valueOf(userId));
					}
					//System.out.println("sourceId: " + sourceId + " userId: " + String.valueOf(userId));
				}*/
				if(friend==0)
					numNonFriend++;
				else
					numFriend++;
				
				if(numNonFriend<=threshold || friend==1)
					this.writeTargetResultIntoFile(writer, index, String.valueOf(userId), friend, String.valueOf(tweetId), TS);
				if(numNonFriend>threshold && numFriend==1)
					break;
			}
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public static void main(String[] args){
		TweetPOI tp = new TweetPOI();
		//tp.queryPOI(0.0005);
		//tp.writePOIIntoDB();
		tp.poiObservation(0.0015, 5);
	}

}
