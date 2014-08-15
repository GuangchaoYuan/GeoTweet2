package edu.ncsu.mas.geocommunity.googleplace;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

public class GpsCoords {
	  private double latitude;
	  private double longitude;

	  public GpsCoords(double lattiude, double longitude) {
	    this.latitude = lattiude;
	    this.longitude = longitude;
	  }
	  
	  @Override
	  public boolean equals(Object obj) {
	    if (obj == null)
	      return false;
	    if (obj == this)
	      return true;
	    if (!(obj instanceof GpsCoords))
	      return false;

	    GpsCoords rhs = (GpsCoords) obj;
	    
	    // if deriving: appendSuper(super.equals(obj)).
	    return new EqualsBuilder().append(latitude, rhs.latitude).append(longitude, rhs.longitude)
	        .isEquals();
	  }
	  
	  @Override
	  public int hashCode() {
	    // you pick a hard-coded, randomly chosen, non-zero, odd number
	    // ideally different for each class
	    return new HashCodeBuilder(1317, 1337).append(latitude).append(longitude).toHashCode();
	  }

	  public double getLatitude() {
	    return latitude;
	  }

	  public void setLatitude(double latitude) {
	    this.latitude = latitude;
	  }

	  public double getLongitude() {
	    return longitude;
	  }

	  public void setLongitude(double longitude) {
	    this.longitude = longitude;
	  }
}
