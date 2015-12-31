package main.java;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import com.opencsv.CSVWriter;

public class DataGenerator {

	
	private CSVWriter writer = null;
	
	public DataGenerator(){}
	
	public void generateData(String fileName, int numOfRows){ 
		String filePath = "src/main/resources/" + fileName;
		File file = new File(filePath);
		try {
			file.createNewFile();
			writer = new CSVWriter(new FileWriter(file));
		} catch (IOException e) { 
			e.printStackTrace();
		}
		writeRows(numOfRows);
		System.out.println("file \"" + fileName + "\" has been created!"); 
	}

	private void writeRows(int numOfRows){
		for(int i = 0; i < numOfRows; i++){
			writeRow(i);
		}
		try {
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private void writeRow(int index){
		String[] dataRow = generateDataRow(index);
		writer.writeNext(dataRow);
	}
	
	private String[] generateDataRow(int index){
		String[] dataRow = new String[10];
		Random rng = new Random();
		dataRow[0] = String.valueOf(index); 												//index	   //col1
		dataRow[1] = generateString(rng, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",5);	   //col2
		dataRow[2] = generateString(rng, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",10);   //col3
		dataRow[3] = generateString(rng, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",20);   //col4
		dataRow[4] = generateString(rng, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",40);   //col5
		dataRow[5] = generateString(rng, "0123456789", 5);					 						   //col6			
		dataRow[6] = generateString(rng, "0123456789", 10);					                           //col7
		dataRow[7] = generateRandomValue(rng, 0, 1, 2);						                           //col8
		dataRow[8] = generateRandomValue(rng, 0, 100, 5);					                           //col9
		dataRow[9] = generateRandomValue(rng, 0, 1000, 10);					                           //col10	
		return dataRow;
	}
	
	private String generateString(Random rng, String characters, int length){
		//From http://stackoverflow.com/questions/2863852/how-to-generate-a-random-string-in-java
		char[] text = new char[length];
	    for (int i = 0; i < length; i++)
	    {
	        text[i] = characters.charAt(rng.nextInt(characters.length()));
	    }
	    return new String(text);
	}
	
	private String generateRandomValue(Random random, int lowerBound, int upperBound, int decimalPlaces){
		//From http://stackoverflow.com/questions/4143304/how-do-you-generate-a-random-number-with-decimal-places
		if(lowerBound < 0 || upperBound <= lowerBound || decimalPlaces < 0){
	        throw new IllegalArgumentException("Put error message here");
	    }
	    final double dbl =
	        ((random == null ? new Random() : random).nextDouble() //
	            * (upperBound - lowerBound))
	            + lowerBound;
	    return String.format("%." + decimalPlaces + "f", dbl);
	}
	
	public static void main(String[] args){
		DataGenerator dataGenerator = new DataGenerator();
		System.out.println("Generating test data....");
		//dataGenerator.generateData("TestFile_OneHundred.txt", 100); 		//one hundred
		//dataGenerator.generateData("TestFile_TenThousands.txt", 10000);		//ten thousands	
		//dataGenerator.generateData("TestFile_OneMillion.txt", 1000000); 	//one million
		dataGenerator.generateData("TestFile_TenMillion.txt", 10000000);    //ten million
		System.out.println("Data generation is completed."); 
	}

}

