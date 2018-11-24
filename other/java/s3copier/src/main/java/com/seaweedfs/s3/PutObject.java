package com.seaweedfs.s3;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.ClientConfiguration;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.client.builder.AwsClientBuilder;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.util.IOUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.IOException;

/**
 * Hello world!
 */
public class PutObject {

    private static Log log = LogFactory.getLog(PutObject.class);

    public static void main(String[] args) {

        AWSCredentials credentials = new BasicAWSCredentials("ANY-ACCESSKEYID", "ANY-SECRETACCESSKEY");
        ClientConfiguration clientConfiguration = new ClientConfiguration();
        clientConfiguration.setSignerOverride("AWSS3V4SignerType");

        AmazonS3 s3Client = AmazonS3ClientBuilder
            .standard()
            .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(
                "http://localhost:8333", Regions.US_WEST_1.name()))
            .withPathStyleAccessEnabled(true)
            .withClientConfiguration(clientConfiguration)
            .withCredentials(new AWSStaticCredentialsProvider(credentials))
            .build();

        String bucketName = "javabucket";
        String stringObjKeyName = "strObject2";
        String fileObjKeyName = "fileObject2";
        String fileName = args[0];

        String stringContent = "Uploaded String Object v3";
        try {

            // Upload a text string as a new object.
            s3Client.putObject(bucketName, stringObjKeyName, stringContent);

            // Upload a file as a new object with ContentType and title specified.
            PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(fileName));
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("plain/text");
            metadata.addUserMetadata("x-amz-meta-title", "someTitle");
            request.setMetadata(metadata);
            s3Client.putObject(request);

            // test reads
            S3Object written = s3Client.getObject(bucketName, stringObjKeyName);
            try {
                String expected = IOUtils.toString(written.getObjectContent());

                if (!stringContent.equals(expected)){
                    System.out.println("Failed to put and get back the content!");
                }

            } catch (IOException e) {
                throw new SdkClientException("Error streaming content from S3 during download");
            } finally {
                IOUtils.closeQuietly(written, log);
            }

            // test deletes
            s3Client.deleteObject(bucketName, stringObjKeyName);


            // delete bucket
            String tmpBucket = "tmpbucket";
            s3Client.createBucket(tmpBucket);
            s3Client.putObject(tmpBucket, stringObjKeyName, stringContent);
            s3Client.deleteBucket(tmpBucket);

        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process
            // it, so it returned an error response.
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        }

        System.out.println("Hello World!");
    }
}
