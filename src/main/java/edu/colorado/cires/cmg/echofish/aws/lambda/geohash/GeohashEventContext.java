package edu.colorado.cires.cmg.echofish.aws.lambda.geohash;

import java.util.Objects;

public class GeohashEventContext {

  private final String s3BucketName;
  private final String shipName;
  private final String cruiseName;
  private final String sensorName;
  private final int maxUploadBuffers;

  public GeohashEventContext(String s3BucketName, String shipName, String cruiseName, String sensorName, int maxUploadBuffers) {
    this.s3BucketName = s3BucketName;
    this.shipName = shipName;
    this.cruiseName = cruiseName;
    this.sensorName = sensorName;
    this.maxUploadBuffers = maxUploadBuffers;
  }

  public String getS3BucketName() {
    return s3BucketName;
  }

  public String getShipName() {
    return shipName;
  }

  public String getCruiseName() {
    return cruiseName;
  }

  public String getSensorName() {
    return sensorName;
  }

  public int getMaxUploadBuffers() {
    return maxUploadBuffers;
  }


  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GeohashEventContext that = (GeohashEventContext) o;
    return maxUploadBuffers == that.maxUploadBuffers && Objects.equals(s3BucketName, that.s3BucketName) && Objects.equals(shipName,
        that.shipName) && Objects.equals(cruiseName, that.cruiseName) && Objects.equals(sensorName, that.sensorName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(s3BucketName, shipName, cruiseName, sensorName, maxUploadBuffers);
  }

  @Override
  public String toString() {
    return "GeohashEventContext{" +
        "s3BucketName='" + s3BucketName + '\'' +
        ", shipName='" + shipName + '\'' +
        ", cruiseName='" + cruiseName + '\'' +
        ", sensorName='" + sensorName + '\'' +
        ", maxUploadBuffers=" + maxUploadBuffers +
        '}';
  }
}
