package com.lambda;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.SentenceUtils;
import edu.stanford.nlp.process.DocumentPreprocessor;

public class Parser {
	public final String s3InputBucket = "emailtraininginput";
	public final String s3OutputBucket = "emailtrainingoutput";
	public Normalizer normalizer = new Normalizer();

	public static void main(String[] args) {
		Wrapper wrapper = new Wrapper();
		Parser parser = new Parser();
		File tempTxtFile = wrapper.getS3Data("testInput8.30.txt");
		ArrayList<String> text = parser.readTxt(tempTxtFile);
		List<String> sentences = parser.stanfordParse(text);
		File outputCsv = parser.sentencesToCsv(sentences);
		wrapper.s3Client.putObject(wrapper.s3OutputBucket, "sentences.csv", outputCsv);
	}

	/**
	 * Read data from a .txt file, acculumulate into a string
	 * 
	 * @param tempTxtFile
	 *            input text
	 * @return the string accumulator
	 */
	public ArrayList<String> readTxt(File tempTxtFile) {
		BufferedReader br = null;
		// group strings into chunks ('/n') to help the sentence parser
		// ASSUMPTION: TEXT ON DIFFERENT LINES WILL NEVER BELONG TO SAME SENTENCE
		ArrayList<String> lineAcc = new ArrayList<String>();
		try {
			br = new BufferedReader(new FileReader(tempTxtFile));
			try {
				String line;
				while ((line = br.readLine()) != null) {
					lineAcc.add(line);
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		System.out.println("** .txt data transferred to string **");
		return lineAcc;
	}

	public List<String> stanfordParse(ArrayList<String> input) {
		List<String> sentenceList = new ArrayList<String>();
		for (String line : input) {
			Reader reader = new StringReader(line);
			DocumentPreprocessor dp = new DocumentPreprocessor(reader);
			for (List<HasWord> sentence : dp) {
				// SentenceUtils not Sentence
				String sentenceString = SentenceUtils.listToString(sentence);
				sentenceList.add(sentenceString);
			}
		}
		System.out.println("** Email body parsed by Stanford NLP **");
		return sentenceList;
	}

	public File sentencesToCsv(List<String> sentences) {
		File tempCsv = createTempFile("sentences", ".csv");
		StringBuilder sb = new StringBuilder();
		// headers for .csv file
		sb.append("classification, original_email_body, normalized_email_body, date_binary, question_binary\n");
		FileWriter pw = null;
		try {
			pw = new FileWriter(tempCsv, true);
			for (String sentence : sentences) {
				// local vars for fields of training data
				String normalizedSentence = normalizer.normalize(sentence);
				String dateBinary = normalizedSentence.contains("[date]") ? "1" : "0";
				String questionBinary = normalizedSentence.contains("?") ? "1" : "0";
				// leave the first column blank for manual classification
				sb.append(',');
				sb.append("\"" + sentence + "\",");
				sb.append("\"" + normalizedSentence + "\",");
				sb.append("\"" + dateBinary + "\",");
				sb.append("\"" + questionBinary + "\"\n");
			}
			pw.write(sb.toString());
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				pw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		System.out.println("** Sentences written to .csv **");
		return tempCsv;
	}

	/**
	 * Create a temporary file
	 * 
	 * @param name
	 *            of the temp file
	 * @param extension
	 *            of the temp file
	 * @return the temp
	 */
	public File createTempFile(String name, String extension) {
		File output = null;
		try {
			output = File.createTempFile(name, extension);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return output;
	}
}