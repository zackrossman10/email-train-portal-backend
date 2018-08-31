/**
 * Handles all interaction with AWS resources (S3, Lambda, Comprehend
 */

package com.lambda;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.comprehend.AmazonComprehend;
import com.amazonaws.services.comprehend.AmazonComprehendClientBuilder;
import com.amazonaws.services.comprehend.model.DetectEntitiesRequest;
import com.amazonaws.services.comprehend.model.DetectEntitiesResult;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

public class Wrapper {
	public final String s3InputBucket = "emailtraininginput";
	public final String s3OutputBucket = "emailtrainingoutput";
	public AWSCredentialsProvider awsCreds = DefaultAWSCredentialsProviderChain.getInstance();
	public AmazonS3 s3Client = AmazonS3ClientBuilder.standard().withCredentials(awsCreds).withRegion("us-east-1")
			.build();
	public AmazonComprehend comprehendClient = AmazonComprehendClientBuilder.standard().withCredentials(awsCreds)
			.withRegion("us-east-1").build();
	public Normalizer normalizer = new Normalizer();
	public Parser parser = new Parser();

	/**
	 * Main function called by Lambda after SQS trigger
	 * 
	 * @param input
	 *            message from SQS which triggered lambda
	 * 
	 * @param context
	 *            required by AWS
	 */
	public String handleRequest(Map<String, Map<String, Object>[]> arg0, Context arg1) {
		String body = (String) arg0.get("Records")[0].get("body");
		JSONArray records = new JSONObject(body).getJSONArray("Records");
		String s3InputKey = records.getJSONObject(0).getJSONObject("s3").getJSONObject("object").getString("key")
				.replace("+", " ").replace("%2C", ",");
		String fileExtension = s3InputKey.substring(s3InputKey.length() - 4, s3InputKey.length()).toLowerCase();
		if (fileExtension.equals(".txt")) {
			if (s3Client.doesObjectExist(s3InputBucket, s3InputKey)) {
				File tempTxtFile = getS3Data(s3InputKey);
				ArrayList<String> text = parser.readTxt(tempTxtFile);
				List<String> sentences = parser.stanfordParse(text);
				File outputCsv = parser.sentencesToCsv(sentences);
				String filePrefix = s3InputKey.substring(0, s3InputKey.length() - 4);
				s3Client.putObject(s3OutputBucket, filePrefix + ".csv", outputCsv);
			} else {
				File errorFile = parser.createTempFile("error", ".txt");
				s3Client.putObject(s3OutputBucket, s3InputKey + "_NOT_FOUND", errorFile);
			}
		}
		return "Finished .csv creation";
	}

	/**
	 * Read data from an S3 .txt file into a local, temporary .txt file
	 * 
	 * @return the local temporary file
	 */
	public File getS3Data(String s3InputKey) {
		S3Object o = s3Client.getObject(s3InputBucket, s3InputKey);
		S3ObjectInputStream s3is = o.getObjectContent();
		FileOutputStream fos = null;
		File tempTxtFile = parser.createTempFile("input", ".txt");
		try {
			fos = new FileOutputStream(tempTxtFile);
			byte[] readBuf = new byte[1024];
			int readLen = 0;
			while ((readLen = s3is.read(readBuf)) > 0) {
				fos.write(readBuf, 0, readLen);
			}
		} catch (AmazonServiceException e) {
			System.err.println(e.getErrorMessage());
		} catch (FileNotFoundException e) {
			System.err.println(e.getMessage());
		} catch (IOException e) {
			System.err.println(e.getMessage());
		} finally {
			if (fos != null) {
				try {
					fos.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
			try {
				s3is.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("** S3 Data transferred to .txt **");
		return tempTxtFile;
	}
	
	
}
