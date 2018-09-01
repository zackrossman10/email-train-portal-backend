package com.lambda;

import java.util.List;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.DefaultAWSCredentialsProviderChain;
import com.amazonaws.services.comprehend.AmazonComprehend;
import com.amazonaws.services.comprehend.AmazonComprehendClientBuilder;
import com.amazonaws.services.comprehend.model.DetectEntitiesRequest;
import com.amazonaws.services.comprehend.model.DetectEntitiesResult;
import com.amazonaws.services.comprehend.model.Entity;

public class Normalizer {
	public AWSCredentialsProvider awsCreds = DefaultAWSCredentialsProviderChain.getInstance();
	public AmazonComprehend comprehendClient = AmazonComprehendClientBuilder.standard().withCredentials(awsCreds)
			.withRegion("us-east-1").build();

	public static void main(String[] args) {
		Normalizer normalizer = new Normalizer();
		// normalize the text with both AWS Comprehend and search/replace
		String normalizedText = normalizer.comprehendNormalize(
				"Hello Robert, my name's Zack. My 20th birthday is on May 18th, 1998. I'm going to be 20. you're you'll you've won't ");
		System.out.println("Coprehend normalized: " + normalizedText);
		normalizedText = normalizer.searchReplaceNormalize(normalizedText);
		System.out.println("** Fully normalized: " + normalizedText + " **");
	}

	/**
	 * Wrapper method for normalization
	 * 
	 * @param originalText
	 *            the text to be normalized
	 * @return the normalized string
	 */
	public String normalize(String originalText) {
		String normalizedText = comprehendNormalize(originalText);
		normalizedText = searchReplaceNormalize(normalizedText);
		System.out.println("** Sentences fully normalized **");
		return normalizedText;
	}

	/**
	 * Replace names and dates with normalized terms as identified by AWS Comprehend
	 * 
	 * @param originalText
	 *            the text to normalize with AWS Comprehend
	 * @return the normalized string
	 */
	public String comprehendNormalize(String originalText) {
		DetectEntitiesRequest detectEntitiesRequest = new DetectEntitiesRequest().withText(originalText)
				.withLanguageCode("en");
		DetectEntitiesResult detectSentimentResult = comprehendClient.detectEntities(detectEntitiesRequest);
		List<Entity> results = detectSentimentResult.getEntities();
		String normalizedText = originalText;
		// replace "PERSON" and "DATE" entities in the normalized string
		for (Entity entity : results) {
			String type = entity.getType();
			if (type.equals("PERSON") || type.equals("DATE")) {
				String text = entity.getText();
				normalizedText = normalizedText.replaceAll(text, "[" + type.toLowerCase() + "]");
			}
		}
		System.out.println("** Sentences normalized by AWS Comprehend **");
		return normalizedText;
	}

	/**
	 * Replace slang, abbreviations, pronouns, numbers with normalized terms using
	 * regexes, search/replace
	 * 
	 * @param originalText
	 *            the text to normalize with regexes
	 * @return the normalized string
	 */
	public String searchReplaceNormalize(String originalText) {
		originalText = originalText.replaceAll("\'s", " is");
		originalText = originalText.replaceAll("\'m", " am");
		originalText = originalText.replaceAll("'re", " are");
		originalText = originalText.replaceAll("'ll", " will");
		originalText = originalText.replaceAll("'ve", " have");
		originalText = originalText.replaceAll("(?i)don't", " do not");
		originalText = originalText.replaceAll("(?i)won't", " will not");
		originalText = originalText.replaceAll("(?i)doesn't", " does not");
		originalText = originalText.replaceAll("'d", " would");
		originalText = originalText.replaceAll("(?i)what's", "what is");
		originalText = originalText.replaceAll("[0-9]+", "[number]");
		originalText = originalText.replaceAll("(?i)(why |who |where |what |when )", "[wwhh] ");
		originalText = originalText.replaceAll("(?i)(after|during|before)", "[aaafter]");
		// careful not to overmatch, "*" is end-of=line character
		originalText = originalText.replaceAll("(?i)( we | you[ *]| he | she | they )", " [pronoun-subject] ");
		originalText = originalText.replaceAll("(I |We |You |He |She |They )", "[pronoun-subject] ");
		originalText = originalText.replaceAll("( me | us | them )", " [pronoun-object] ");
		System.out.println("** Sentences normalized by regexes **");
		return originalText;
	}
}
