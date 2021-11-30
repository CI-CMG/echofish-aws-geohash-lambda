package edu.colorado.cires.cmg.echofish.aws.lambda.geohash;

import java.util.Objects;

public class SnsMessage {

  private String survey;

  public String getSurvey() {
    return survey;
  }

  public void setSurvey(String survey) {
    this.survey = survey;
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    SnsMessage that = (SnsMessage) o;
    return Objects.equals(survey, that.survey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(survey);
  }

  @Override
  public String toString() {
    return "SnsMessage{" +
        "survey='" + survey + '\'' +
        '}';
  }
}
