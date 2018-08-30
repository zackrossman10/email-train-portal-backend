package com.lambda;

import java.io.File;

import java.util.List;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;

import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;

public class Wrapper {
	public final String s3InputBucket = "emailtraininginput";
	public final String s3OutputBucket = "emailtrainingoutput";
    public static AmazonS3 s3Client = new AmazonS3Client(DefaultAWSCredentialsProviderChain.getInstance());
	public Parser parser;
	
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
		parser = new Parser();
		String body = (String) arg0.get("Records")[0].get("body");
		JSONArray records = new JSONObject(body).getJSONArray("Records");
		String s3InputKey = records.getJSONObject(0).getJSONObject("s3").getJSONObject("object").getString("key")
				.replace("+", " ").replace("%2C", ",");
		String fileExtension = s3InputKey.substring(s3InputKey.length() - 4, s3InputKey.length()).toLowerCase();
		if (fileExtension.equals(".txt")) {
			if (s3Client.doesObjectExist(s3InputBucket, s3InputKey)) {
				File tempTxtFile = parser.getS3Data(s3InputKey);
				String text = parser.readTxt(tempTxtFile);
				List<String> sentences = parser.stanfordParse(text);
				File outputCsv = parser.sentencesToCsv(sentences);
				s3Client.putObject(s3OutputBucket, "sentences.csv", outputCsv);
			} else {
				File errorFile = parser.createTempFile("error", ".txt");
				s3Client.putObject(s3OutputBucket, s3InputKey + "_NOT_FOUND", errorFile);
			}
		}
		return "Finished .csv creation";
	}
}
