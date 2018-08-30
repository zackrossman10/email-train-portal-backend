package com.lambda;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;

import edu.stanford.nlp.ling.HasWord;
import edu.stanford.nlp.ling.SentenceUtils;
import edu.stanford.nlp.process.DocumentPreprocessor;

public class Parser {
	public final String s3InputBucket = "emailtraininginput";
	public final String s3OutputBucket = "emailtrainingoutput";
	
	public static void main(String[] args) {
		Wrapper wrapper = new Wrapper();
		Parser parser = new Parser();
		File tempTxtFile = parser.getS3Data("testInput8.29.txt");
		String text = parser.readTxt(tempTxtFile);
		List<String> sentences = parser.stanfordParse(text);
		File outputCsv = parser.sentencesToCsv(sentences);
		Wrapper.s3Client.putObject(wrapper.s3OutputBucket, "sentences.csv", outputCsv);
	}

	/**
	 * Read data from a .txt file, acculumulate into a string
	 * 
	 * @param tempTxtFile
	 *            input text
	 * @return the string accumulator
	 */
	public String readTxt(File tempTxtFile) {
		BufferedReader br = null;
		String accumulator = "";
		try {
			br = new BufferedReader(new FileReader(tempTxtFile));
			String line;
			try {
				while ((line = br.readLine()) != null) {
					accumulator += line;
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
		return accumulator;
	}
	
	/**
	 * Read data from an S3 .txt file into a local, temporary .txt file
	 * 
	 * @return the local temporary file
	 */
	public File getS3Data(String s3InputKey) {
		S3Object o = Wrapper.s3Client.getObject(s3InputBucket, s3InputKey);
		S3ObjectInputStream s3is = o.getObjectContent();
		FileOutputStream fos = null;
		File tempTxtFile = createTempFile("input", ".txt");
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

	public List<String> stanfordParse(String input) {
		Reader reader = new StringReader(input);
		DocumentPreprocessor dp = new DocumentPreprocessor(reader);
		List<String> sentenceList = new ArrayList<String>();
		for (List<HasWord> sentence : dp) {
			// SentenceUtils not Sentence
			String sentenceString = SentenceUtils.listToString(sentence);
			sentenceList.add(sentenceString);
		}

		for (String sentence : sentenceList) {
			System.out.println(sentence);
		}
		System.out.println("** Email body parsed by Stanford NLP **");
		return sentenceList;
	}

	public File sentencesToCsv(List<String> sentences) {
		File tempCsv = createTempFile("sentences", ".csv");
		StringBuilder sb = new StringBuilder();
		// headers for .csv file
		sb.append("classification, email_body, date_binary, question_mark_binary, \n");
		FileWriter pw = null;
		try {
			pw = new FileWriter(tempCsv, true);
			for (String sentence : sentences) {
				// only fill second column with the email sentence
				sb.append(", " + sentence + ", , ,\n");
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