package edu.ncsu.mas.geocommunity.googleplace;

import java.util.HashSet;
import java.util.Set;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;


public class GooglePoi {
	  private GpsCoords queryPosition;

	  private GpsCoords poiPosition;
	
	  private String name;
	
	  private final Set<String> types = new HashSet<String>();
	
	  private final Set<GooglePoiReview> reviews = new HashSet<GooglePoiReview>();
	  
	  private String vicinity;
	
	  public GpsCoords getQueryPosition() {
	    return queryPosition;
	  }
	
	  public void setQueryPosition(GpsCoords position) {
	    this.queryPosition = position;
	  }
	
	  public GpsCoords getPoiPosition() {
	    return poiPosition;
	  }
	
	  public void setPoiPosition(GpsCoords position) {
	    this.poiPosition = position;
	  }
	
	  public String getName() {
	    return name;
	  }
	
	  public void setName(String name) {
	    this.name = name;
	  }
	
	  public Set<String> getTypes() {
	    return types;
	  }
	  
	  public void addType(String type) {
	    this.types.add(type);
	  }
	  
	  public Set<GooglePoiReview> getReviews() {
	    return reviews;
	  }
	  
	  public void addReview(GooglePoiReview review) {
	    this.reviews.add(review);
	  }
	
	  public String getVicinity() {
	    return vicinity;
	  }
	
	  public void setVicinity(String vicinity) {
	    this.vicinity = vicinity;
	  }
	
	  @Override
	  public int hashCode(){
	    return new HashCodeBuilder(1319, 1319).append(poiPosition).append(name).toHashCode();
	  }
	  
	  @Override
	  public boolean equals(Object obj) {
	    if (obj == null)
	      return false;
	    if (obj == this)
	      return true;
	    if (!(obj instanceof GooglePoi))
	      return false;
	
	    GooglePoi rhs = (GooglePoi) obj;
	    
	    // if deriving: appendSuper(super.equals(obj)).
	    return new EqualsBuilder().append(poiPosition, rhs.poiPosition).append(name, rhs.name)
	        .isEquals();
	  }

}
