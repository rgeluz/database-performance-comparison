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
	private final static boolean DEBUG_MODE = true;   
	private final static boolean RECREATE_DATA = false;
	private static final int ONE_HUNDRED = 100;
	private static final int ONE_HUNDRED_THOUSAND = 100000;
	private static final int ONE_MILLION = 1000000;
	private static final int TEN_MILLION = 100000000; //NOTE: warning this may take awhile 
	private String tableName;
	private Connection conn = null;
	private Statement stmt = null;
	private ResultSet rs = null;

	protected PostgreSQLDatabase(String name) {
		super(name);
	}

	@Override
	protected Boolean connectToDB() { 
		Boolean connected = false;
		log("Connecting to " + name + " database....");
		try {
			Class.forName("org.postgresql.Driver");
			conn = DriverManager
					.getConnection("jdbc:postgresql://localhost:5432/testdb",
								   "postgres", "postgres1235813");
			connected = true;	
			log("Opened database successfully. \n");
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
	protected void createTable(String testType) {
		if(testType.equals(Tests.SELECT_ALL_ROW_10_COL_TEST)){
			create10ColTable();
		} else if (testType.equals(Tests.SELECT_100_ROW_10_COL_TEST)){
			createTableWithNameColumn(10);
		} else if (testType.equals(Tests.SELECT_100_ROW_120_COL_TEST)){
			createTableWithNameColumn(120);
		} else {
			System.out.println("Could not create table for test" + testType + "\n"); 
		}
	}
	
	@Override
	protected void create10ColTable(){   
		log("Creating table " + this.tableName + "....");
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
			log("sql: " + sql);
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
			log("Table created successfully.\n");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

	@Override
	protected void createTableWithNameColumn(int numOfCols) {   
		log("Creating " + numOfCols + " column table " + this.tableName + "....");  
		try {			
			String sql = "CREATE TABLE " + this.tableName + " " +
					"(col1 integer, " + 
					"name text, ";  
			sql += addColumns(numOfCols);
			sql += ")";
			log("sql: " + sql);
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
			log("Table created successfully.\n");
		} catch (SQLException e) {
			e.printStackTrace();
		}	
	}

	private String addColumns(int numOfCols){
		String columnList = ""; 
		for(int i=3; i<=numOfCols; i++ ){
			if(i==numOfCols){   
				columnList += ("col"+ i + " integer");
			} else {
				columnList += ("col"+ i + " integer, ");
			}
		}
		return columnList;
	}
	
	@Override
	protected void createIndexOnTable(String indexName, String colName) {
		log("Creating index on table " + this.tableName + "....");
		try {
			long start = System.currentTimeMillis(); 
			String sql = "CREATE INDEX " + indexName + " ON " + this.tableName + "(" + colName + ")";
			log("sql: " + sql);
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
			long time = (System.currentTimeMillis() - start); 
			log("Index created successfully in " + time + " ms.\n");
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	

	@Override
	protected void deleteTable() {
		log("Deleting table " + this.tableName + "....");
		try {		
			String sql = "DROP TABLE IF EXISTS " + this.tableName;
			log("sql: " + sql);
			stmt = conn.createStatement();
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
		log("Loading data into table " + this.tableName + "....");
		try {
			long start = System.currentTimeMillis(); 
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
			long time = (System.currentTimeMillis() - start); 
			log("Loaded data into table " + this.tableName + " successfully in " + time + " ms. \n");
		} catch(SQLException e){ 
			e.printStackTrace();
		}	
	}

	@Override
	protected long getData(String whereClause) {  
		log("Retrieving data....");
		long start = System.currentTimeMillis();
		long time = 0;
		try {
			conn.setAutoCommit(false);
			stmt = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
										ResultSet.CONCUR_READ_ONLY); 
			String sql = "SELECT * FROM " + this.tableName;
			if(whereClause.isEmpty()){ 
				sql += ";";
			} else {
				sql += " " + whereClause + ";";
			}
			log("sql: " + sql);
			rs = stmt.executeQuery(sql); 
			
			//NOTE: use only for debugging. Will effect timing of test if enabled. Only works for "SELECT_ALL_ROW_10_COL_TEST" test.
			//displayResultsInTable(); 
			time = (System.currentTimeMillis() - start); 
			log("Retrieved data successfully in " + time + " ms. \n");
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return time;
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
		log("Retrieving row count...."); 
		//See "Get a record count with a SQL Statement" 
		//http://www.rgagnon.com/javadetails/java-0292.html
		int rowCount = 0;
		try {
			if(resultSet.last()){ //move the cursor to the last row
				rowCount = resultSet.getRow(); //get the number of rows
				resultSet.beforeFirst(); //back to initial state
			}
			log("Retrieved row count successfully. \n"); 
		} catch (SQLException e) { 
			e.printStackTrace();
		}
		return rowCount;
	}
	
	@Override
	public Map<String, Object> runTest(String fileName, String testType, String tableName, String whereClause) {
		this.tableName = tableName;
		long time = 0;
		int rowCount = 0;		
		File testData = new File("src/main/resources/" + fileName);
		if(!testData.exists()){
			System.out.println("The file \"" + fileName + "\"does not exists! Cannot conduct test."); 			
			/*System.out.println("The file \"" + fileName + "\" does not exists! Will create a new one....");
			DataGenerator dataGenerator = new DataGenerator(fileName, 
					                                        Tests.SELECT_ALL_ROW_10_COL_TEST, 
					                                        ONE_HUNDRED);
			dataGenerator.generateData();*/
		}
		
		this.connectToDB();
		if(RECREATE_DATA){
			this.deleteTable();
			this.createTable(testType);
			if(testType.equals(Tests.SELECT_100_ROW_10_COL_TEST) || testType.equals(Tests.SELECT_100_ROW_120_COL_TEST)){
				this.createIndexOnTable(this.tableName + "_name_index", "name");  //only works for the name column table test
			}
			this.loadData(testData.getAbsolutePath()); 
		}	
		time = this.getData(whereClause); 	//only time the data retrieval	
		rowCount = getRowCount(rs);
		//this.deleteTable();
		this.closeConnection();
		
		Map<String,Object> testResult = new HashMap<String,Object>(); 
		testResult.put("time", time);
		testResult.put("rowCount", rowCount);
		return testResult;
	}

	public static void main (String[] args) {
		System.out.println("Test PostgreSQLDatabase: "); 	  
		PostgreSQLDatabase postgreSQLDB = new PostgreSQLDatabase("PostgreSQL"); 
		System.out.println("Executing \"" + postgreSQLDB.getName() +"\"....");  
		//Map<String,Object> testResult = postgreSQLDB.runTest("TestFile_OneHundred.txt", Tests.SELECT_ALL_ROW_10_COL_TEST, "TestData", ""); 
		//Map<String,Object> testResult = postgreSQLDB.runTest("TestFile_10MillionRows_10Columns.txt", Tests.SELECT_100_ROW_10_COL_TEST, "TestData_10Col", "WHERE name='DatabaseComparisonTest'");   
		Map<String,Object> testResult = postgreSQLDB.runTest("TestFile_10MillionRows_120Columns.txt", Tests.SELECT_100_ROW_120_COL_TEST, "TestData_120Col", "WHERE name='DatabaseComparisonTest'");   
		long time = (long) testResult.get("time");
		int rowCount = (int) testResult.get("rowCount"); 
		System.out.println("Took \"" + postgreSQLDB.getName() + "\" " + time + " ms to read " +
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
