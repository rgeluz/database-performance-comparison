package main.java;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

import com.opencsv.CSVWriter;

import main.java.database.Tests;

public class DataGenerator {
	private static final int ONE_HUNDRED = 100; 
	private static final int ONE_HUNDRED_THOUSAND = 100000;
	private static final int ONE_MILLION = 1000000;
	private static final int TEN_MILLION = 10000000; //NOTE: warning this may take awhile 
	private static final String MATCHING_NAME = "DatabaseComparisonTest";
	private final String fileName;
	private final String testType;
	private int numOfRows;
	private int matchingNameCounter = 0;
	private int rowCounter = 0;
	
	private CSVWriter writer = null;
	
	public DataGenerator(String fileName, String testType, int numOfRows){
		this.fileName = fileName;
		this.testType = testType;
		this.numOfRows = numOfRows;
	}
	
	public void generateData(){  
		String filePath = "src/main/resources/" + this.fileName;
		File file = new File(filePath);
		try {
			file.createNewFile();
			writer = new CSVWriter(new FileWriter(file));
		} catch (IOException e) { 
			e.printStackTrace();
		}
		writeRows(numOfRows);
		System.out.println("file \"" + this.fileName + "\" has been created!"); 
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
		rowCounter++;
	}
	
	private String[] generateDataRow(int index){
		if(this.testType.equals(Tests.SELECT_ALL_ROW_10_COL_TEST)){
			return generate10ColDataRow(index);
		} else if (this.testType.equals(Tests.SELECT_100_ROW_10_COL_TEST)){
			//Need to add the matching name value to the name columns 
			//for a total of 100 records of the 10 million records. 
			//The data row will have 10 columns. 
			return generateDataRowWithNameCol(index, 10);
		} else if (this.testType.equals(Tests.SELECT_100_ROW_120_COL_TEST)){
			//Need to add the matching name value to the name columns 
			//for a total of 100 records of the 10 million records.
			//The data row will have 120 columns.
			return generateDataRowWithNameCol(index, 120); 
		} else {
			return null;
		}
	}
	
	/**
	 * Used for the "Select All Rows Test" 
	 * @param index
	 * @return
	 */
	private String[] generate10ColDataRow(int index){
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
	
	private String[] generateDataRowWithNameCol(int index, int numOfCol){
		String[] dataRow = new String[numOfCol];
		Random random = new Random();
		dataRow[0] = String.valueOf(index);
		
		//Name column
		dataRow[1] = addMatchingName() ? MATCHING_NAME : generateString(random, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",7); //arbitrarily chose 7 characters wide.
		
		//Remaining columns
		for(int i=2; i<numOfCol; i++){
			dataRow[i] = String.valueOf(generateInteger(random));  
		}
		return dataRow;
	}
	

 	
	/**
	 * randomly generate a row with “matching-name” in the name column at random times during generation, 
	 * exactly 100 times so the size of the result set is the same.
	 * @return
	 */
	private Boolean addMatchingName(){
		//Run this at least for every 1000 row and no more than 100 times.
		if(matchingNameCounter<100){
			int increment = 1000;
			if((rowCounter%increment)==0){
				if(coinFlip()){
					matchingNameCounter++;
					return true;
				}
			}	
		}
		return false;
	}
	
	private Boolean coinFlip(){
		//From http://stackoverflow.com/questions/24174078/simple-coin-toss-using-random-class-in-java-the-do-while-loop-doesnt-seem-to-g
		Random randomNumber = new Random();
		int result = randomNumber.nextInt(2);
		if(result==0){
			return true;
		} else {
			return false; 
		}			
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
	
	private int generateInteger(Random random){
		//From http://stackoverflow.com/questions/8236125/get-random-integer-in-range-x-y
		return random.nextInt(10) + 1;   
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
		
		System.out.println("Generating test data....");
		
		//Data for Test#1 "Select all with different size data sets."
		/*DataGenerator dataGeneratorOneHundredRows = new DataGenerator("TestFile_OneHundred.txt", 
				                                                      Tests.SELECT_ALL_ROW_10_COL_TEST, 
				                                                      ONE_HUNDRED);
		dataGeneratorOneHundredRows.generateData(); 		
		
		DataGenerator dataGeneratorTenThousandsRows = new DataGenerator("TestFile_TenThousands.txt", 
				                                                        Tests.SELECT_ALL_ROW_10_COL_TEST, 
				                                                        ONE_HUNDRED_THOUSAND);
		dataGeneratorTenThousandsRows.generateData();	
		
		DataGenerator dataGeneratorOneMillionRows = new DataGenerator("TestFile_OneMillion.txt", 
				                                                      Tests.SELECT_ALL_ROW_10_COL_TEST, 
				                                                      ONE_MILLION);
		dataGeneratorOneMillionRows.generateData(); 
		
		DataGenerator dataGeneratorTenMillionRows = new DataGenerator("TestFile_TenMillion.txt", 
				                                                      Tests.SELECT_ALL_ROW_10_COL_TEST,
				                                                      TEN_MILLION);
		dataGeneratorTenMillionRows.generateData();   */
		
		
		//Data for Test #2 "Select 100 rows based on where clause with different number of columns."
		/*DataGenerator dataGenerator100MillRow10Col = new DataGenerator("TestFile_10MillionRows_10Columns.txt", 
				                                        				Tests.SELECT_100_ROW_10_COL_TEST,
				                                        				TEN_MILLION);
		dataGenerator100MillRow10Col.generateData();
		
		DataGenerator dataGenerator100MillRow120Col = new DataGenerator("TestFile_10MillionRows_120Columns.txt",
																		Tests.SELECT_100_ROW_120_COL_TEST,
																		TEN_MILLION);
		
		dataGenerator100MillRow120Col.generateData();
		
		System.out.println("Data generation is completed."); */
	}

}

