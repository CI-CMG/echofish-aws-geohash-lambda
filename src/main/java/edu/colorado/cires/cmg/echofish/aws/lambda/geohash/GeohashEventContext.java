package edu.colorado.cires.cmg.echofish.aws.lambda.geohash;

import java.util.Objects;

public class GeohashEventContext {

  private final String s3BucketName;
  private final String survey;
  private final int maxUploadBuffers;
  private final String geohashS3BucketName;

  public GeohashEventContext(String s3BucketName, String survey, int maxUploadBuffers, String geohashS3BucketName) {
    this.s3BucketName = s3BucketName;
    this.survey = survey;
    this.maxUploadBuffers = maxUploadBuffers;
    this.geohashS3BucketName = geohashS3BucketName;
  }

  public String getS3BucketName() {
    return s3BucketName;
  }

  public String getSurvey() {
    return survey;
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
    GeohashEventContext that = (GeohashEventContext) o;
    return maxUploadBuffers == that.maxUploadBuffers && Objects.equals(s3BucketName, that.s3BucketName) && Objects.equals(survey,
        that.survey) && Objects.equals(geohashS3BucketName, that.geohashS3BucketName);
  }

  @Override
  public int hashCode() {
    return Objects.hash(s3BucketName, survey, maxUploadBuffers, geohashS3BucketName);
  }

  @Override
  public String toString() {
    return "GeohashEventContext{" +
        "s3BucketName='" + s3BucketName + '\'' +
        ", survey='" + survey + '\'' +
        ", maxUploadBuffers=" + maxUploadBuffers +
        ", geohashS3BucketName='" + geohashS3BucketName + '\'' +
        '}';
  }
}
