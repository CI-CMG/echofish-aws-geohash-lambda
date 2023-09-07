package edu.colorado.cires.cmg.echofish.aws.lambda.geohash;

import java.util.ArrayList;
import java.util.List;

public class IndexFile {

  private String path;

  private List<Record> indexRecords = new ArrayList<>();

  public String getPath() {
    return path;
  }

  public void setPath(String path) {
    this.path = path;
  }

  public List<Record> getIndexRecords() {
    return indexRecords;
  }

  public void setIndexRecords(List<Record> indexRecords) {
    this.indexRecords = indexRecords;
  }

  public static class Record {
    private long index;
    private double longitude;
    private double latitude;

    public long getIndex() {
      return index;
    }

    public void setIndex(long index) {
      this.index = index;
    }

    public double getLongitude() {
      return longitude;
    }

    public void setLongitude(double longitude) {
      this.longitude = longitude;
    }

    public double getLatitude() {
      return latitude;
    }

    public void setLatitude(double latitude) {
      this.latitude = latitude;
    }
  }

}
