package main.java.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

public class HiveHDFSDatabase extends AbstractDatabase {
	private final static boolean DEBUG_MODE = false; 
	private String tableName = "TestData";
	private Connection conn = null;
	private Statement stmt = null;
	private ResultSet rs = null;
	
	protected HiveHDFSDatabase(String name) {
		super(name);
	}

	@Override
	protected Boolean connectToDB() {
		Boolean connected = false;
		log("Connecting to " + name + " database.");
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver"); 
			conn = DriverManager
					.getConnection("jdbc:hive2://localhost:10000/default", 
								   "hive", "");
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
	protected void closeConnection() {
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
		/*
		 * The default field terminator in Hive is ^A. 
		 * You need to explicitly mention in your create table statement 
		 * that you are using a different field separator.
		 * http://stackoverflow.com/questions/13379299/getting-null-values-while-loading-the-data-from-flat-files-into-hive-tables
		 */
		log("Creating table " + this.tableName);
		try {
			String sql = "CREATE TABLE " + this.tableName + " " +
					"(col1 INT, " + 
					"col2 STRING, " + 
					"col3 STRING, " + 
					"col4 STRING, " + 
					"col5 STRING, " + 
					"col6 INT, " +  
					"col7 BIGINT, " + 
					"col8 DOUBLE, " + 
					"col9 DOUBLE, " + 
					"col10 DOUBLE) " +
					"ROW FORMAT DELIMITED FIELDS TERMINATED BY ','"; //See comments above.
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

	@Override
	protected void loadData(String filePath) { //TODO not finished yet
		log("Loading data into table " + this.tableName);
		try {
			String sql = "load data local inpath '" + filePath + "' into table " + this.tableName; 
			log("sql: " + sql);
			stmt = conn.createStatement();
			stmt.execute(sql); 
			log("Loaded data into table " + this.tableName + " successfully. \n");
		} catch(SQLException e){ 
			e.printStackTrace();
		}	
	}

	@Override
	protected void getData() {   
		log("Retrieving data.");
		try {
			stmt = conn.createStatement();  
			String sql = "SELECT * FROM " + this.tableName; 
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

	private int getRowCount(){ 
		log("Retrieving row count."); 
		try {
			stmt = conn.createStatement();  
			String sql = "SELECT COUNT(1) FROM " + this.tableName;  
			rs = stmt.executeQuery(sql); 
			while (rs.next()){
				return rs.getInt(1);
			}
			log("Retrieved row count successfully."); 
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return 0;
	}
	
	@Override
	public Map<String, Object> runTest(String fileName) {		  
		long time = 0;
		int rowCount = 0;
		this.connectToDB();
		this.deleteTable();
		this.createTable();
		this.loadData("/usr/tmp/"+fileName);    
		//only time the data retrieval
		long start = System.currentTimeMillis();
		this.getData(); 
		time = (System.currentTimeMillis() - start);  
		rowCount = getRowCount();
		this.deleteTable();
		this.closeConnection();
		Map<String,Object> testResult = new HashMap<String,Object>(); 
		testResult.put("time", time);
		testResult.put("rowCount", rowCount);
		return testResult;
	}
	
	public static void main (String[] args){
		System.out.println("Test HiveHDFSDatabase: ");
		String fileName = "TestFile_OneHundred.txt";
		HiveHDFSDatabase hiveHDFSDatabase = new HiveHDFSDatabase("Hive");
		System.out.println(" executing \"" + hiveHDFSDatabase.getName() + "\"...");
		Map<String,Object> testResult = hiveHDFSDatabase.runTest(fileName);
		long time = (long) testResult.get("time");
		int rowCount = (int)testResult.get("rowCount");
		System.out.println(" took \"" + hiveHDFSDatabase.getName() + "\" " + time + " ms to read " +
							insertCommas(rowCount) + " rows. ");
		System.out.println("Completed HiveHDFSDatabase Test."); 
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
