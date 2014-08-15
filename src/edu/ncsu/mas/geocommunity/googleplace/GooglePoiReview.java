package edu.ncsu.mas.geocommunity.googleplace;

public class GooglePoiReview {
	  private String author;
	  
	  private long reviewTime;
	  
	  private String reviewText;
	  
	  private int overallRating;

	  public String getAuthor() {
	    return author;
	  }

	  public void setAuthor(String author) {
	    this.author = author;
	  }

	  public String getReviewText() {
	    return reviewText;
	  }

	  public void setReviewText(String reviewText) {
	    this.reviewText = reviewText;
	  }

	  /**
	   * A user's overall rating for this Place. This is a whole number, ranging
	   * from 1 to 5.
	   * 
	   * @return
	   */
	  public int getOverallRating() {
	    return overallRating;
	  }

	  public void setOverallRating(int overallRating) {
	    this.overallRating = overallRating;
	  }

	  public long getReviewTime() {
	    return reviewTime;
	  }

	  public void setReviewTime(long reviewTime) {
	    this.reviewTime = reviewTime;
	  }

}
