package main.java;

import java.text.DateFormat;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;

import main.java.database.AbstractDatabase;
import main.java.database.Databases;
import main.java.database.Tests;

import java.util.Map.Entry;

public class PerformanceComparison {
	
	private final String fileName;
	private final String testType;
	private final String tableName;
	private final String whereClause;
	 
	PerformanceComparison(String fileName , String testType, String tableName, String whereClause){
		this.fileName = fileName; 
		this.testType = testType;
		this.tableName = tableName;
		this.whereClause = whereClause;
	}

	public void execute(final int loops) throws Exception {
		Map<String, Long[]> stats = new HashMap<String, Long[]>();
		for(AbstractDatabase database : Databases.list()){
			Long[] times = new Long[loops];
			Arrays.fill(times, -1L);
			stats.put(database.getName(), times);
		}	
		for(int i=0; i<loops; i++){
			for(AbstractDatabase database : Databases.list()){
				try {
					System.out.print("Loop " + (i + 1) + " executing \"" + database.getName() + "\"..."); 
					Map<String,Object> testResult = database.runTest(this.fileName, this.testType, this.tableName, this.whereClause);    
					long time = (long) testResult.get("time");
					int rowCount = (int) testResult.get("rowCount");
					System.out.println(" took \"" + database.getName() + "\" " + time + " ms to read " +
										insertCommas(rowCount) + " rows. \n"); 
					stats.get(database.getName())[i] = time;
				} catch (Throwable ex) {
					System.out.println(" Database \"" + database.getName() + "\" threw exception " + ex.getMessage());
				}
				System.gc();
				Thread.sleep(500);
			}
			System.out.println(""); 
		}
		printResults(loops, stats);
	}

	private static String insertCommas(Integer number){
		return NumberFormat.getNumberInstance(Locale.US).format(number); 
	}
	
	private void printResults(int loops, Map<String, Long[]> stats) { 
		System.out.println("===============\n");
		System.out.println(" AVERAGES ");
		System.out.println("===============\n"); 
		Map<Long, String> averages = orderByAverageTime(loops, stats);
		long bestTime = 0;
		for(Entry<Long, String> average : averages.entrySet()){
			long time = average.getKey(); 
			String database = average.getValue();
			System.out.println("| " + database + " \t | " + time + " ms "); 
			
			if(time == -1) {
				System.out.println("Could not execute."); 
			}
			
			if (bestTime != 0) {
				long increasePercentage = time * 100 / bestTime - 100;
				System.out.print(" \t | " + increasePercentage + "% ");
			} else{
				bestTime = time;
				System.out.print(" \t | Best time! ");
			}

			long best = getBestTime(stats.get(database));
			long worst = getWorstTime(stats.get(database));

			System.out.println(" \t | " + best + " ms \t | " + worst + " ms |");
		}
	}
	
	private TreeMap<Long, String> orderByAverageTime(int loops, Map<String, Long[]> stats) {
		TreeMap<Long, String> averages = new TreeMap<Long, String>();
		for (Entry<String, Long[]> databaseTimes: stats.entrySet()) {
			Long[] times = databaseTimes.getValue();
			long average = 0L;
			//we are discarding the first recorded time here to take into account JIT optimizations
			for (int i = 1; i < times.length; i++) {
				average = average + times[i];
			}
			average = average / (loops - 1);
			averages.put(average, databaseTimes.getKey());
		}
		return averages;
	}

	private long getBestTime(Long[] times) {
		long best = times[1];
		for (int i = 1; i < times.length; i++) {
			if (times[i] < best) {
				best = times[i];
			}
		}
		return best;
	}

	private long getWorstTime(Long[] times) {
		long worst = times[1];
		for (int i = 1; i < times.length; i++) {
			if (times[i] > worst) {
				worst = times[i];
			}
		}
		return worst;
	}

	public static void main (String[] args) throws Exception {		
		System.out.println("======================================");
		System.out.println("=== Database Performance Comparison ===");   
		System.out.println("=== Date/Time: " + getCurrentDate() + " ===");
		System.out.println("======================================\n");
	
		//Number of test to run per data set. Must be greater than one.
		int numberOfTest = 5;
		long start = System.currentTimeMillis();
		
		/*//100 row dataset
		System.out.println(">>>Executing Performance Comparison of querying 100 rows:");
		String testFile1 = "TestFile_OneHundred.txt";
		PerformanceComparison oneHundredRowTest = new PerformanceComparison(testFile1, Tests.SELECT_ALL_ROW_10_COL_TEST,  "TestData", "");
		oneHundredRowTest.execute(numberOfTest);
		System.out.println("");
			
		//10,000 row dataset
		System.out.println(">>>Executing Performance Comparison of querying 10,000 rows:");
		String testFile2 = "TestFile_TenThousands.txt"; 
		PerformanceComparison tenThousandRowTest = new PerformanceComparison(testFile2, Tests.SELECT_ALL_ROW_10_COL_TEST, "TestData", "");
		tenThousandRowTest.execute(numberOfTest);
		System.out.println("");
			
		//1,000,000 row dataset
		System.out.println(">>>Executing Performance Comparison of querying 1,000,000 rows:");
		String testFile3 = "TestFile_OneMillion.txt";
		PerformanceComparison oneMillionRowTest = new PerformanceComparison(testFile3, Tests.SELECT_ALL_ROW_10_COL_TEST, "TestData", "");
		oneMillionRowTest.execute(numberOfTest);
		System.out.println(""); 
		
		//10,000,000 row dataset
		System.out.println(">>>Executing Performance Comparison of querying 10,000,000 rows:");
		String testFile4 = "TestFile_TenMillion.txt";
		PerformanceComparison tenMillionRowTest = new PerformanceComparison(testFile4, Tests.SELECT_ALL_ROW_10_COL_TEST, "TestData", "");
		tenMillionRowTest.execute(numberOfTest);
		System.out.println("");*/
		
		//TODO include more if needed
		//100 row result set out of 10,000,000 row and 10 col dataset
		System.out.println(">>>Executing Performance Comparison of querying with where condition.");
		System.out.println(">>>and recieve 100 row result set from a 10 million size table with 10 columns."); 
		String testFile5 = "TestFile_10MillionRows_10Columns.txt";
		PerformanceComparison tenMillionRow10ColTest = new PerformanceComparison(testFile5, Tests.SELECT_100_ROW_10_COL_TEST, "TestData_10Col", "WHERE name='DatabaseComparisonTest'");
		tenMillionRow10ColTest.execute(numberOfTest);
		
		//100 row result set out of 10,000,000 row and 120 col dataset
		System.out.println(">>>Executing Performance Comparison of querying with where condition.");
		System.out.println(">>>and recieve 100 row result set from a 10 million size table with 120 columns.");
		String testFile6 = "TestFile_10MillionRows_120Columns.txt";
		PerformanceComparison tenMillionRow120ColTest = new PerformanceComparison(testFile6, Tests.SELECT_100_ROW_120_COL_TEST, "TestData_120Col", "WHERE name='DatabaseComparisonTest'");
		tenMillionRow120ColTest.execute(numberOfTest);  

		long time = (System.currentTimeMillis() - start);
		System.out.println("Database Performance Comparison Test completed."); 
		System.out.println("Database Performance Comparison Test took " + time + " ms to complete."); 
	}
	
	private static String getCurrentDate(){
		DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
		Date date = new Date();
		return dateFormat.format(date);
	}
}
