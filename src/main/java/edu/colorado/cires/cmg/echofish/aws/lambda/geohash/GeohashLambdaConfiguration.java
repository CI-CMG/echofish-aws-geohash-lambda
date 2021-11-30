package edu.colorado.cires.cmg.echofish.aws.lambda.geohash;

import java.util.Objects;

public class GeohashLambdaConfiguration {

  private final String zarrBucketName;
  private final int maxUploadBuffers;
  private final String geohashS3BucketName;


  public GeohashLambdaConfiguration(String zarrBucketName, int maxUploadBuffers, String geohashS3BucketName) {
    this.zarrBucketName = zarrBucketName;
    this.maxUploadBuffers = maxUploadBuffers;
    this.geohashS3BucketName = geohashS3BucketName;
  }

  public String getZarrBucketName() {
    return zarrBucketName;
  }

  public int getMaxUploadBuffers() {
    return maxUploadBuffers;
  }

  public String getGeohashS3BucketName() {
    return geohashS3BucketName;
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
    return maxUploadBuffers == that.maxUploadBuffers && Objects.equals(zarrBucketName, that.zarrBucketName) && Objects.equals(
        geohashS3BucketName, that.geohashS3BucketName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(zarrBucketName, maxUploadBuffers, geohashS3BucketName);
  }

  @Override
  public String toString() {
    return "GeohashLambdaConfiguration{" +
        "zarrBucketName='" + zarrBucketName + '\'' +
        ", maxUploadBuffers=" + maxUploadBuffers +
        ", geohashS3BucketName='" + geohashS3BucketName + '\'' +
        '}';
  }
}
