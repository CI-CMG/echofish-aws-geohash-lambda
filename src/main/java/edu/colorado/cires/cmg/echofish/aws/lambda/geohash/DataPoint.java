package edu.colorado.cires.cmg.echofish.aws.lambda.geohash;

import java.util.Objects;

public class DataPoint {

  private final double longitude;
  private final double latitude;
  private final long time;


  public DataPoint(double longitude, double latitude, long time) {
    this.longitude = longitude;
    this.latitude = latitude;
    this.time = time;
  }

  public double getLongitude() {
    return longitude;
  }

  public double getLatitude() {
    return latitude;
  }

  public long getTime() {
    return time;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    DataPoint point = (DataPoint) o;
    return Double.compare(point.longitude, longitude) == 0 && Double.compare(point.latitude, latitude) == 0 && time == point.time;
  }

  @Override
  public int hashCode() {
    return Objects.hash(longitude, latitude, time);
  }

  @Override
  public String toString() {
    return "DataPoint{" +
        "longitude=" + longitude +
        ", latitude=" + latitude +
        ", time=" + time +
        '}';
  }
}
