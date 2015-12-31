package main.java.database;

import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

import org.postgresql.copy.*;
import org.postgresql.core.BaseConnection;

import main.java.DataGenerator;

public class PostgreSQLDatabase extends AbstractDatabase {
	private final static boolean DEBUG_MODE = false; 
	private static final int ONE_HUNDRED = 100;
	private static final int ONE_HUNDRED_THOUSAND = 100000;
	private static final int ONE_MILLION = 1000000;
	private static final int TEN_MILLION = 100000000; //NOTE: warning this may take awhile 
	private String tableName = "TestData";
	private Connection conn = null;
	private Statement stmt = null;
	private ResultSet rs = null;

	protected PostgreSQLDatabase(String name) {
		super(name);
	}

	@Override
	protected Boolean connectToDB() { 
		Boolean connected = false;
		log("Connecting to " + name + " database.");
		try {
			Class.forName("org.postgresql.Driver");
			conn = DriverManager
					.getConnection("jdbc:postgresql://localhost:5432/testdb",
								   "postgres", "postgres1235813");
			connected = true;	
			log("Opened database successfully.");
		} catch (Exception e) {
			e.printStackTrace();
			System.err.println(e.getClass().getName()+": "+e.getMessage());
			System.exit(0);
			connected = false;
		}
		return connected;
	}
	
	@Override
	protected void closeConnection(){
		try {
			rs.close(); 
			stmt.close();
			conn.close();
		} catch (SQLException e) { 
			e.printStackTrace();
		}
	}
	
	@Override
	protected void createTable() {
		log("Creating table " + this.tableName);
		try {
			String sql = "CREATE TABLE " + this.tableName + " " +
					"(col1 integer, " + 
					"col2 text, " + 
					"col3 text, " + 
					"col4 text, " + 
					"col5 text, " + 
					"col6 integer, " +  
					"col7 bigint, " + 
					"col8 real, " + 
					"col9 real, " + 
					"col10 real)";
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
			log("Table created successfully.\n");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void deleteTable() {
		log("Deleting table " + this.tableName);
		try {
			stmt = conn.createStatement();
			String sql = "DROP TABLE IF EXISTS " + this.tableName;
			stmt.execute(sql);
			log("Table deleted successfully.\n");
		} catch (SQLException e) {
			e.printStackTrace();
		}  
	}
	
	private String reverseSlash(String value) {
		return value.replace("\\", "/");
	}

	@Override
	protected void loadData(String filePath) {
		log("Loading data into table " + this.tableName);
		try {
			conn.setAutoCommit(false);		
			/*
			 * https://wiki.postgresql.org/wiki/COPY
			 * You need to use an absolute pathname with COPY.
			 */
			String sql = "COPY " + this.tableName + " FROM " +
					 "'" + reverseSlash(filePath) + "'" + " DELIMITER ',' CSV;";
			log("sql: " + sql);
			stmt = conn.createStatement();
			stmt.execute(sql); 
			conn.commit();
			log("Loaded data into table " + this.tableName + " successfully. \n");
		} catch(SQLException e){ 
			e.printStackTrace();
		}	
	}

	@Override
	protected void getData() {  
		log("Retrieving data.");
		try {
			conn.setAutoCommit(false);
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
										ResultSet.CONCUR_READ_ONLY); 
			String sql = "SELECT * FROM " + this.tableName + ";";
			rs = stmt.executeQuery(sql); 
			
			//NOTE: use only for debugging. Will effect timing of test if enabled
			//displayResultsInTable(); 

			log("Retrieved data successfully.");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	} 
	
	private void displayResultsInTable(){  
		System.out.println(" col1 | col2 | col3 | col4 | col5 | col6 | col7 | col8 | col9 | col10 ");
		System.out.println("---------------------------------------------------------------------------");
		try {
			while(rs.next()){
				String col1 = String.valueOf(rs.getInt("col1"));
				String col2 = rs.getString("col2");
				String col3 = rs.getString("col3");
				String col4 = rs.getString("col4");
				String col5 = rs.getString("col5");
				String col6 = String.valueOf(rs.getInt("col6"));
				String col7 = String.valueOf(rs.getLong("col7")); 
				String col8 = String.valueOf( rs.getDouble("col8"));
				String col9 = String.valueOf(rs.getDouble("col9")); 
				String col10 = String.valueOf(rs.getDouble("col10")); 
				System.out.println(col1 + " | " +
								   col2 + " | " +
								   col3 + " | " +
								   col4 + " | " +
								   col5 + " | " +
								   col6 + " | " +
								   col7 + " | " +
								   col8 + " | " +
								   col9 + " | " +
								   col10);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	private int getRowCount(ResultSet resultSet){ 
		//See "Get a record count with a SQL Statement" 
		//http://www.rgagnon.com/javadetails/java-0292.html
		int rowCount = 0;
		try {
			if(resultSet.last()){ //move the cursor to the last row
				rowCount = resultSet.getRow(); //get the number of rows
				resultSet.beforeFirst(); //back to initial state
			}
		} catch (SQLException e) { 
			e.printStackTrace();
		}
		return rowCount;
	}
	
	@Override
	public Map<String, Object> runTest(String fileName) {
		long time = 0;
		int rowCount = 0;		
		File testData = new File("src/main/resources/" + fileName);
		if(!testData.exists()){
			System.out.println("The file \"" + fileName + "\" does not exists! Will create a new one....");
			DataGenerator dataGenerator = new DataGenerator();
			int numOfRows = ONE_HUNDRED; 
			dataGenerator.generateData(fileName, numOfRows);
		}	
		this.connectToDB();
		this.deleteTable();
		this.createTable(); 
		this.loadData(testData.getAbsolutePath()); 
		//only time the data retrieval
		long start = System.currentTimeMillis();
		this.getData(); 
		time = (System.currentTimeMillis() - start); 
		rowCount = getRowCount(rs);
		this.deleteTable();
		this.closeConnection();
		Map<String,Object> testResult = new HashMap<String,Object>(); 
		testResult.put("time", time);
		testResult.put("rowCount", rowCount);
		return testResult;
	}
	
	public static void main (String[] args) {
		System.out.println("Test PostgreSQLDatabase: ");
		String fileName = "TestData.txt" ;		  
		PostgreSQLDatabase postgreSQLDB = new PostgreSQLDatabase("PostgreSQL"); 
		System.out.println(" executing \"" + postgreSQLDB.getName() +"\"..."); 
		Map<String,Object> testResult = postgreSQLDB.runTest(fileName);   
		long time = (long) testResult.get("time");
		int rowCount = (int) testResult.get("rowCount"); 
		System.out.println(" took \"" + postgreSQLDB.getName() + "\" " + time + " ms to read " +
							insertCommas(rowCount) + " rows. ");  
		System.out.println("Completed PostgreSQLDatabase Test.");
	}
	
	private static String insertCommas(Integer number){
		return NumberFormat.getNumberInstance(Locale.US).format(number); 
	}
	
	//For Debugging
	private void log(String message){
		if(DEBUG_MODE)
			System.out.println(message); 
	}

}
