package edu.colorado.cires.cmg.echofish.aws.lambda.geohash;

import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicSessionCredentials;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.SNSEvent;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import edu.colorado.cires.cmg.awszarr.AwsS3ClientWrapper;
import edu.colorado.cires.cmg.echofish.data.model.CruiseProcessingMessage;
import edu.colorado.cires.cmg.echofish.data.model.jackson.ObjectMapperCreator;
import edu.colorado.cires.cmg.echofish.data.s3.S3OperationsImpl;
import java.util.Objects;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.AwsCredentialsProvider;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;

public class GeohashLambda implements RequestHandler<SNSEvent, Void> {

  private static final Logger LOGGER = LoggerFactory.getLogger(GeohashLambda.class);
  private static final ObjectMapper OBJECT_MAPPER = ObjectMapperCreator.create();

  private static final AwsCredentialsProvider creds = StaticCredentialsProvider.create(AwsBasicCredentials.create(
      Objects.requireNonNull(System.getenv("ZARR_BUCKET_ACCESS_KEY")),
      Objects.requireNonNull(System.getenv("ZARR_BUCKET_SECRET_ACCESS_KEY"))
  ));
  private static final S3Client s3 = S3Client.builder()
      .credentialsProvider(creds)
      .region(Region.of(System.getenv("BUCKET_REGION")))
      .build();

  private static final GeohashLambdaHandler HANDLER = new GeohashLambdaHandler(
      AwsS3ClientWrapper.builder().s3(s3).build(),
      new GeohashLambdaConfiguration(
          Objects.requireNonNull(System.getenv("ZARR_BUCKET_NAME")),
          Integer.parseInt(System.getenv("S3_UPLOAD_BUFFERS"))
      ));

  @Override
  public Void handleRequest(SNSEvent snsEvent, Context context) {

    LOGGER.info("Received event: {}", snsEvent);

    CruiseProcessingMessage cruiseProcessingMessage;
    try {
      cruiseProcessingMessage = OBJECT_MAPPER.readValue(snsEvent.getRecords().get(0).getSNS().getMessage(), CruiseProcessingMessage.class);
    } catch (JsonProcessingException e) {
      throw new IllegalArgumentException("Unable to parse SNS notification", e);
    }

    HANDLER.handleRequest(cruiseProcessingMessage);

    return null;
  }
}
