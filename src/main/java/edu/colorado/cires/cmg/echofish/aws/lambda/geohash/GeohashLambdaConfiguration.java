package edu.colorado.cires.cmg.echofish.aws.lambda.geohash;

import java.util.Objects;

public class GeohashLambdaConfiguration {

  private final String zarrBucketName;
  private final int maxUploadBuffers;


  public GeohashLambdaConfiguration(String zarrBucketName, int maxUploadBuffers) {
    this.zarrBucketName = zarrBucketName;
    this.maxUploadBuffers = maxUploadBuffers;
  }

  public String getZarrBucketName() {
    return zarrBucketName;
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
    GeohashLambdaConfiguration that = (GeohashLambdaConfiguration) o;
    return maxUploadBuffers == that.maxUploadBuffers && Objects.equals(zarrBucketName, that.zarrBucketName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(zarrBucketName, maxUploadBuffers);
  }

  @Override
  public String toString() {
    return "GeohashLambdaConfiguration{" +
        "zarrBucketName='" + zarrBucketName + '\'' +
        ", maxUploadBuffers=" + maxUploadBuffers +
        '}';
  }
}
