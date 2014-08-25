package edu.ncsu.mas.geocommunity.foursquare;

import java.io.Serializable;
import java.util.ArrayList;

public class POIResult implements Serializable{
	private ArrayList<Venue> venueList = new ArrayList<Venue>();
	
	private class Venue implements Serializable{
		private String name;
		private String category; //primary category
		private String parent; //parent of the primary category
		private double distance;//distance between this venue and the location of tweet
		
		public Venue(String name, String category, String parent, double distance){
			this.name = name;
			this.category = category;
			this.parent = parent;
			this.distance = distance;
		}
		public String getName(){
			return name;
		}
		
		public String getCategory(){
			return category;
		}
		
		public String getParent(){
			return parent;
		}
		
		public double getDistance(){
			return distance;
		}
	}
	
	public void addElement(String name, String category, String parent, double distance){
		Venue vn = new Venue(name, category, parent, distance);
		venueList.add(vn);
	}
	
	public ArrayList<Venue> getVenueList(){
		return venueList;
	}

}
