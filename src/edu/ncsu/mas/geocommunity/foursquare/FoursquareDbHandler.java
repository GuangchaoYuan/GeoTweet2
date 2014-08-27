package edu.ncsu.mas.geocommunity.foursquare;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.Writer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

import com.mysql.jdbc.Statement;

import edu.ncsu.mas.geocommunity.db.TweetPOIDBRunnable;
import edu.ncsu.mas.geocommunity.db.WorkQueue;
import edu.ncsu.mas.geocommunity.dbmanager.SqlController;
import edu.ncsu.mas.geocommunity.utils.Constant;
import fi.foyt.foursquare.api.entities.Category;
import fi.foyt.foursquare.api.entities.CompactVenue;
import fi.foyt.foursquare.api.entities.Location;
import fi.foyt.foursquare.api.entities.VenuesSearchResult;

public class FoursquareDbHandler {
	private String dbName = "guppy";
	private SqlController sqlController;
	public static final double R = 6371; // In kilometers
	private int numTweet = 0;
	private int numNonRecord = 0;
	
	private ArrayList<GeoCoordinate> mainEntryList = new ArrayList<GeoCoordinate>();
	private HashMap<String, ArrayList<String>> categoryMap = new HashMap<String, ArrayList<String>>();
	
	public FoursquareDbHandler(){
		sqlController = new SqlController(dbName);
	}
	
	class GeoCoordinate {
		double latitude = 0;
		double longitude = 0;
		
		public GeoCoordinate(double lat, double lon){
			latitude = lat;
			longitude = lon;
		}
	}
	
	public Map<double[], VenuesSearchResult> readStatusObjects(int offset, int fetchSize) {
		Map<double[], VenuesSearchResult> resultMap = new HashMap<double[], VenuesSearchResult>();
		String select = "select * from fs_venue_search_result_object limit "
	            + offset + ", " + fetchSize;
	    sqlController.createConnection();
	    PreparedStatement prepStmt = sqlController.GetPreparedStatement(select);
	    ResultSet rs;
		try {
			rs = prepStmt.executeQuery(select);
			while (rs.next()) {
		    	double latitude = rs.getDouble("latitude");
		    	double longitude = rs.getDouble("longitude");
		    	byte[] buf = rs.getBytes("result_object");
		    	if (buf != null) {
		    		ObjectInputStream objectIn = new ObjectInputStream(new ByteArrayInputStream(buf));
		    		VenuesSearchResult result = (VenuesSearchResult) objectIn.readObject();
		    		resultMap.put(new double[] { latitude, longitude }, result);
		    	}
		    }
			sqlController.close();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch(IOException e){
			e.printStackTrace();
		} catch(ClassNotFoundException e){
			e.printStackTrace();
		}
	    
	    			
	    return resultMap;
	}
	
	public void readPOIObjects(int offset, int fetchSize) {
		String select = "select * from fs_venue_poi limit "
	            + offset + ", " + fetchSize;
	    sqlController.createConnection();
	    PreparedStatement prepStmt = sqlController.GetPreparedStatement(select);
	    ResultSet rs;
	    long tweetId = 0L;
	   
		try {
			rs = prepStmt.executeQuery(select);
			while (rs.next()) {
				tweetId = rs.getLong(1);
		    	double latitude = rs.getDouble("latitude");
		    	double longitude = rs.getDouble("longitude");
		    	byte[] buf = rs.getBytes("poi_object");
		    	if (buf != null) {
		    		ObjectInputStream objectIn = new ObjectInputStream(new ByteArrayInputStream(buf));
		    		POIResult result = (POIResult) objectIn.readObject();
		    		for(int i = 0; i< result.getResultSize(); i++){
						System.out.println("id: " + tweetId + " size: " +result.getResultSize() + "name: " + result.getName(i) + " category: " + result.getCategory(i) 
								+ " parent: " + result.getParent(i) + " distance: " + result.getDistance(i));
					}
		    	}
		    }
			sqlController.close();
			
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch(IOException e){
			e.printStackTrace();
		} catch(ClassNotFoundException e){
			e.printStackTrace();
		}
	    

	}
	
	public void gridDensity(){
		String inputFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\foursquare\\grid.txt";
		String storeFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\foursquare\\gridFrequency.txt";
		String line = "";
		HashMap<Integer, Integer> gridFrequency = new HashMap<Integer, Integer>();
		int num=0;
		int value = 0;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			while((line = reader.readLine())!=null){
				num++;
				String[] words = line.split(Constant.SEPARATOR_COMMA);
				int freq = words.length/3;
				if(gridFrequency.containsKey(freq)){
					value = gridFrequency.get(freq);
					value++;
					gridFrequency.put(freq, value);
				}
				else
					gridFrequency.put(freq, 1);
				
				if(num%1000 == 0)
					System.out.println(num + " records have been processed");
			}
			reader.close();
			System.out.println("There are " + num + " grids in total");
			
			//print write the results into a file
			printSortedMap(gridFrequency, storeFile);
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
	
	private void printMap(TreeMap<Integer, Integer> sortedMap, String storeFile){
		BufferedWriter writer  = null;
	    Iterator it = sortedMap.entrySet().iterator();
		try {
			writer = new BufferedWriter(new FileWriter(storeFile));
			while(it.hasNext()){
				Map.Entry<Integer, Integer> pairs = (Map.Entry<Integer, Integer>)it.next();
				String key = pairs.getKey().toString();
				String value = pairs.getValue().toString();
				writer.append(key + " " + value);
				writer.newLine();
				writer.flush();
			}
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
	}
	
	private void printSortedMap(HashMap<Integer, Integer> map, String storeFile){
		ValueComparator  vc = new ValueComparator(map);
		TreeMap<Integer, Integer> sortedMap = new TreeMap<Integer, Integer>(vc);
		sortedMap.putAll(map);
		printMap(sortedMap, storeFile);
	}
	
	
	private class ValueComparator implements Comparator<Integer> {

        HashMap<Integer, Integer> base;

        ValueComparator(HashMap<Integer, Integer> map) {
            this.base = map;
        }

        @Override
        public int compare(Integer a,Integer b) {
            if (base.get(a) < base.get(b)) {
                return -1;
            } else if(base.get(a) == base.get(b)){
            	return 0;
            }
            else {
                return 1;
            }
        }
    }
	
	
	class DistanceComparator implements Comparator<CompactVenue> {

        HashMap<CompactVenue, Double> base;

        DistanceComparator(HashMap<CompactVenue, Double> map) {
            this.base = map;
        }

        @Override
        public int compare(CompactVenue a,CompactVenue b) {
            if (base.get(a) < base.get(b)) {
                return -1;
            } else if(base.get(a) == base.get(b)){
            	return 0;
            }
            else {
                return 1;
            }
        }
    }
	
	private TreeMap<CompactVenue, Double> sortVenueByDistance(HashMap<CompactVenue, Double> map){
		System.out.println("Sort venues by distance");
		DistanceComparator  vc = new DistanceComparator(map);
		TreeMap<CompactVenue, Double> sortedMap = new TreeMap<CompactVenue, Double>(vc);
		sortedMap.putAll(map);
		return sortedMap;
	}
	
	//write the top ranked records into DB
	private void writeIntoDB(BufferedWriter writer, PreparedStatement prepStmt, TreeMap<CompactVenue, Double> map, int top, long tweetId, double lat, double lon){
		System.out.println("Write results into DB");
		
		int num = 0;
		String name = "";
		String category = "";
		String parent = "";
		
		POIResult result = new POIResult();
		Iterator it = map.entrySet().iterator();
		while(it.hasNext() && num<top){
			num++;
			Map.Entry<CompactVenue, Double> pairs = (Map.Entry<CompactVenue, Double>)it.next();
			CompactVenue key = pairs.getKey();
			double dist = pairs.getValue();
			name = key.getName();
			Category[] cat = key.getCategories();
			for(int i = 0; i < cat.length; i++){
				if(cat[i].getPrimary()){
					category = cat[i].getName();
					//I need to get the parent of the category
					parent = getParent(category);
					result.addElement(name, category, parent, dist);
					break;
				}
			}
			//write result into a file
			this.writePOIIntoFile(writer, tweetId, lat, lon, name, category, parent, dist);
			
		}
		
		//write POI result into DB
		try {
			prepStmt.setLong(1, tweetId);
			prepStmt.setDouble(2, lat);
			prepStmt.setDouble(3, lon);
			prepStmt.setObject(4, result);
			sqlController.addBatch(prepStmt);
			//sqlController.executeBatch(prepStmt);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private String getParent(String category){
		String parent = "";
		boolean found = false;
		if(category.contains("Restaurant"))
			parent = "Food";
		else{
			Iterator it = categoryMap.entrySet().iterator();
			while(it.hasNext()){
				Map.Entry<String, ArrayList<String>> pairs = (Map.Entry<String, ArrayList<String>>)it.next();
				
				String key = pairs.getKey();
				ArrayList<String> value = pairs.getValue();
				for(int i = 0; i < value.size(); i++){
					if(value.get(i).equals(category))
					{
						parent = key;
						found = true;
						break;
					}
				}
				if(found)
					break;
			}
			if(!found)
				parent = category;
		}
		
		return parent;
	}
	
	//read category information
	private void readCategoryHierarchy(String inputFile){
		String line = "";
		BufferedReader reader;
		try {
			reader = new BufferedReader(new FileReader(inputFile));
			while((line = reader.readLine())!=null){
				String[] words = line.split(Constant.SEPARATOR_COMMA);
				String key = words[0];
				ArrayList<String> list = new ArrayList<String>();
				for(int i = 1; i < words.length; i++)
					list.add(words[i]);
				categoryMap.put(key, list);
			}
			reader.close();
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		}
	}

	public void queryTweetPOI(double threshold, int top) {
		String inputFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\foursquare\\grid.txt";
		String catFile = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\foursquare\\category.csv";
		String output = "D:\\Social_Media_Analytics\\Geo_community\\dataset\\foursquare\\POI.csv";
		String insert = "insert into "
		        + "fs_venue_poi (tweet_id, latitude, longitude, poi_object) values (?, ?, ?, ?) ON DUPLICATE KEY UPDATE tweet_id = tweet_id";
		
		String line = "";
		int num = 0;
		//int numTweet = 0;
		//int numNonRecord = 0;
		long mainTweetId = 0L;
		long tweetId = 0L;
		double lat = 0;
		double lon = 0;
		GeoCoordinate main;
		HashMap<GeoCoordinate, CompactVenue> mainMap;
		this.readGeoMainEntry(inputFile);
		this.readCategoryHierarchy(catFile);
		
		WorkQueue queue = new WorkQueue(2);
		try {
			sqlController.createConnection();
			PreparedStatement prepStmt = sqlController.GetPreparedStatement(insert);
			BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			BufferedWriter writer = new BufferedWriter(new FileWriter(output, true));
			
			while((line = reader.readLine())!=null){
				if(num==0){
					num++;
					continue;
				}
				queue.execute(new FoursquareDbRunnable(num, line, threshold, top, writer, prepStmt));
				/*main = mainEntryList.get(num);
				mainMap = mainEntryResult(main.latitude, main.longitude);
				
				String[] words = line.split(Constant.SEPARATOR_COMMA);
				for(int i = 0; i < words.length; i++){
					if((i+1)%3 == 1){
						numTweet++;
						tweetId = Long.parseLong(words[i]);
						lat = Double.parseDouble(words[i+1]);
						lon = Double.parseDouble(words[i+2]);
						
						if(i == 0)
							mainTweetId = tweetId;
						
						//skip the entry whose id is the same with the mainTweetId
						if( i> 0 && tweetId == mainTweetId)
							continue;
						
						System.out.println("tweetId: " + tweetId + " lat: " + lat + " lon: " + lon);
						HashMap<CompactVenue, Double> poiMap = new HashMap<CompactVenue, Double>();
						
						//only choosing venue whose distance is within the threshold
						System.out.println("Filter the venues within a threshold");
						Iterator it = mainMap.entrySet().iterator();
						while(it.hasNext()){
							Map.Entry<GeoCoordinate, CompactVenue> pairs = (Map.Entry<GeoCoordinate, CompactVenue>)it.next();
							GeoCoordinate key = pairs.getKey();
							CompactVenue value = pairs.getValue();
							double dist = this.haversine(lat, lon, key.latitude, key.longitude);
							if(dist<=threshold){
								poiMap.put(value, dist);
							}
						}
						
						//skip the tweets which doesn't have a POI within 500 radius
						if(poiMap.size()==0){
							numNonRecord++;
							System.out.println(numNonRecord + " records don't have a POI within 500 radius");
							continue;
						}
						//sort the poiMap based on distance and store the top five into DB
						TreeMap<CompactVenue, Double> sortedPOIMap = sortVenueByDistance(poiMap);
						this.writeIntoDB(writer, prepStmt, sortedPOIMap, top, tweetId, lat, lon);
					}
				}*/
				
				num++;
				if(num%1000 == 0)
					System.out.println(num + " main entries have been read");
				
				if(numTweet%1000 == 0)
					System.out.println(numTweet + " tweets have been processed");
			}
			
			Thread.sleep(Integer.MAX_VALUE);
			
			reader.close();
			writer.close();
			sqlController.executeBatch(prepStmt);
			sqlController.close();
			
			System.out.println(numTweet + " in total");
			System.out.println(numNonRecord + " tweets that don't have a POI within 500 radius in total");
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		} catch (InterruptedException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	//store nearest POI around a main entry
	private HashMap<GeoCoordinate, CompactVenue> mainEntryResult(double lat, double lon){
		System.out.println("Query nearest POI for the main entry");
		
		HashMap<GeoCoordinate, CompactVenue> resultMap = new HashMap<GeoCoordinate, CompactVenue>();
		String select = "select * from fs_venue_search_result_object where latitude = " + lat
				+" and longitude = " + lon;
		PreparedStatement prepStmt = sqlController.GetPreparedStatement(select);
		ResultSet rs = null;
		try {
			rs = prepStmt.executeQuery();	
			while(rs.next()){
		    	byte[] buf = rs.getBytes("result_object");
		    	if (buf != null) {
		    		ObjectInputStream objectIn = new ObjectInputStream(new ByteArrayInputStream(buf));
		    		VenuesSearchResult result = (VenuesSearchResult) objectIn.readObject();
		    		for (CompactVenue venue : result.getVenues()) {
						Location loc = venue.getLocation();
						GeoCoordinate gc = new GeoCoordinate(loc.getLat(), loc.getLng());
						resultMap.put(gc, venue);
					}
		    		
		    	}
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch(IOException e){
			e.printStackTrace();
		} catch(ClassNotFoundException e){
			e.printStackTrace();
		}
		
		return resultMap;	
	}
	
	private void readGeoMainEntry(String inputFile){
		String line = "";
		int num = 0;
		try {
			BufferedReader reader = new BufferedReader(new FileReader(inputFile));
			while((line = reader.readLine())!=null){
				num++;
				String[] words = line.split(Constant.SEPARATOR_COMMA);
				double lat = Double.parseDouble(words[1]);
				double lon = Double.parseDouble(words[2]);
				GeoCoordinate gc = new GeoCoordinate(lat, lon);
				mainEntryList.add(gc);
				
				if(num%1000 == 0)
					System.out.println(num + " main entries have been read");
			}
			reader.close();
			System.out.println("There are " + num + " grids in total");
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e){
			e.printStackTrace();
		}
	}
	
	//compute haversine distance
	public static double haversine(double lat1, double lon1, double lat2, double lon2) {
        double dLat = Math.toRadians(lat2 - lat1);
        double dLon = Math.toRadians(lon2 - lon1);
        lat1 = Math.toRadians(lat1);
        lat2 = Math.toRadians(lat2);
 
        double a = Math.sin(dLat / 2) * Math.sin(dLat / 2) + Math.sin(dLon / 2) * Math.sin(dLon / 2) * Math.cos(lat1) * Math.cos(lat2);
        double c = 2 * Math.asin(Math.sqrt(a));
        return R * c;
    }
	
	private void writePOIIntoFile(BufferedWriter writer, long tweetId, double lat, double lon, String name, String category, 
			String parent, double distance){
		try {
			writer.append(String.valueOf(tweetId) + Constant.SEPARATOR_COMMA);
			writer.append(String.valueOf(lat) + Constant.SEPARATOR_COMMA);
			writer.append(String.valueOf(lon) + Constant.SEPARATOR_COMMA);
			writer.append(name + Constant.SEPARATOR_COMMA);
			writer.append(category + Constant.SEPARATOR_COMMA);
			writer.append(parent + Constant.SEPARATOR_COMMA);
			writer.append(String.valueOf(distance));
			writer.newLine();
			writer.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private class FoursquareDbRunnable implements Runnable{
		
		private long tweetId;
		private long mainTweetId;
		private double lat;
		private double lon;
		private GeoCoordinate main;
		private HashMap<GeoCoordinate, CompactVenue> mainMap;
		
		private final int num;
		private final String line;
		private final double threshold;
		private final int top;
		private final BufferedWriter writer;
		private final PreparedStatement prepStmt;
		
		public FoursquareDbRunnable(int num, String line, double threshold, int top, 
				BufferedWriter writer, PreparedStatement prepStmt){
			this.num = num;
			this.line = line;
			this.threshold = threshold;
			this.top = top;
			this.writer = writer;
			this.prepStmt = prepStmt;
		}
		
		@Override
		public void run() {
			// TODO Auto-generated method stub
			main = mainEntryList.get(num);
			mainMap = mainEntryResult(main.latitude, main.longitude);
			
			String[] words = line.split(Constant.SEPARATOR_COMMA);
			for(int i = 0; i < words.length; i++){
				if((i+1)%3 == 1){
					numTweet++;
					tweetId = Long.parseLong(words[i]);
					lat = Double.parseDouble(words[i+1]);
					lon = Double.parseDouble(words[i+2]);
					
					if(i == 0)
						mainTweetId = tweetId;
					
					//skip the entry whose id is the same with the mainTweetId
					if( i> 0 && tweetId == mainTweetId)
						continue;
					
					System.out.println("tweetId: " + tweetId + " lat: " + lat + " lon: " + lon);
					HashMap<CompactVenue, Double> poiMap = new HashMap<CompactVenue, Double>();
					
					//only choosing venue whose distance is within the threshold
					System.out.println("Filter the venues within a threshold");
					Iterator it = mainMap.entrySet().iterator();
					while(it.hasNext()){
						Map.Entry<GeoCoordinate, CompactVenue> pairs = (Map.Entry<GeoCoordinate, CompactVenue>)it.next();
						GeoCoordinate key = pairs.getKey();
						CompactVenue value = pairs.getValue();
						double dist = haversine(lat, lon, key.latitude, key.longitude);
						if(dist<=threshold){
							poiMap.put(value, dist);
						}
					}
					
					//skip the tweets which doesn't have a POI within 500 radius
					if(poiMap.size()==0){
						numNonRecord++;
						System.out.println(numNonRecord + " records don't have a POI within 500 radius");
						continue;
					}
					//sort the poiMap based on distance and store the top five into DB
					TreeMap<CompactVenue, Double> sortedPOIMap = sortVenueByDistance(poiMap);
					writeIntoDB(writer, prepStmt, sortedPOIMap, top, tweetId, lat, lon);
				}
			}
		}
		
	}
	public static void main(String[] args){
		FoursquareDbHandler fdb = new FoursquareDbHandler();
		/*Map<double[], VenuesSearchResult> resultMap = fdb.readStatusObjects(0, 3);
		for (double[] latlng : resultMap.keySet()) {
			System.out.println(latlng[0] + ", " + latlng[1] + ": ");
			VenuesSearchResult result = resultMap.get(latlng);
			System.out.println("Venue size: " + result.getVenues().length);
			for (CompactVenue venue : result.getVenues()) {
				//Category[] cat = venue.getCategories();
				Location loc = venue.getLocation();
				double dist = haversine(latlng[0], latlng[1], loc.getLat(), loc.getLng());
				System.out.print("name: " + venue.getName() + " distance: " + loc.getDistance() +" dist: " + dist);
				System.out.println();
				
			}
		}*/
	    fdb.queryTweetPOI(0.5, 5);
	
		//fdb.readPOIObjects(0,1);
		//fdb.gridDensity();
	}

}
