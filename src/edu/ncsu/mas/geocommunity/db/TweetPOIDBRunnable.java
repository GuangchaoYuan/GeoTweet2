package edu.ncsu.mas.geocommunity.db;
import edu.ncsu.mas.geocommunity.dbmanager.SqlController;

public class TweetPOIDBRunnable implements Runnable {
	
	private final int num ;
	private final long tweetId;
	private final long userId;
	private final String poiName;
	private final String type;
	private final SqlController sqlController;
	
	public TweetPOIDBRunnable (int num, long tweetId, long userId, String poiName, String type, SqlController sqlController){
		this.num = num;
		this.tweetId = tweetId;
		this.userId = userId;
		this.poiName = poiName;
		this.type = type;
		this.sqlController = sqlController;
	}

	@Override
	public void run() {
		// TODO Auto-generated method stub
		try{
			//System.out.println("tweetId: " + tweetId);
			String insert = "Insert into guppy.tweet_poi values ("+this.tweetId+","+this.userId+",'"+
		this.poiName+"','"+this.type+"') ON DUPLICATE KEY UPDATE tweet_id = " + this.tweetId;
			sqlController.update(insert);
		}
		catch(Exception e){
			e.printStackTrace();
		}
		
		if(num % 1000 == 0)
			System.out.println(num + " records have been inserted!");
	}

}
