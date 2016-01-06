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
	private final static boolean DEBUG_MODE = true;  
	private String tableName; 
							
	private Connection conn = null;
	private Statement stmt = null;
	private ResultSet rs = null;
	
	protected HiveHDFSDatabase(String name) {
		super(name);
	}

	@Override
	protected Boolean connectToDB() {
		Boolean connected = false;
		log("Connecting to " + name + " database....");
		try {
			Class.forName("org.apache.hive.jdbc.HiveDriver"); 
			conn = DriverManager
					.getConnection("jdbc:hive2://localhost:10000/default", 
								   "hive", "");
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
	protected void create10ColTable() {
		/*
		 * The default field terminator in Hive is ^A. 
		 * You need to explicitly mention in your create table statement 
		 * that you are using a different field separator.
		 * http://stackoverflow.com/questions/13379299/getting-null-values-while-loading-the-data-from-flat-files-into-hive-tables
		 */
		log("Creating table " + this.tableName + "....");
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
					"(col1 STRING, " +  
					"name STRING, ";  
			sql += addColumns(numOfCols);
			sql += ") ";
			sql += "ROW FORMAT DELIMITED FIELDS TERMINATED BY ','";
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
				columnList += ("col"+ i + " STRING");
			} else {
				columnList += ("col"+ i + " STRING, ");
			}
		}
		return columnList;
	}
	
	@Override
	protected void createIndexOnTable(String indexName, String colName) {
		//See http://www.tutorialspoint.com/hive/hive_views_and_indexes.htm
		//See http://maheshwaranm.blogspot.com/2013/09/hive-indexing.html
		//See http://www.dummies.com/how-to/content/improving-your-hive-queries-with-indexes.html
		log("Creating index on table " + this.tableName + "....");
		try {
			long start = System.currentTimeMillis();
			String sql = "CREATE INDEX " + indexName + " ON TABLE " + this.tableName + 
					     " (" + colName + ")" +
					     " AS 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' WITH DEFERRED REBUILD";     
			log("sql: " + sql);
			stmt = conn.createStatement();
			stmt.executeUpdate(sql);
			
			sql = "ALTER INDEX " + indexName + " ON " + this.tableName + " REBUILD";
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

	@Override
	protected void loadData(String filePath) { //TODO not finished yet
		log("Loading data into table " + this.tableName + "....");
		try {
			long start = System.currentTimeMillis(); 
			String sql = "load data local inpath '" + filePath + "' into table " + this.tableName; 
			log("sql: " + sql);
			stmt = conn.createStatement();
			stmt.execute(sql); 
			long time = (System.currentTimeMillis() - start);
			log("Loaded data into table " + this.tableName + " successfully " + time + " ms. \n");
		} catch(SQLException e){ 
			e.printStackTrace();
		}	
	}

	@Override
	protected void getData(String whereClause) {   
		log("Retrieving data....");
		try {
			stmt = conn.createStatement();  
			String sql = "SELECT * FROM " + this.tableName; 
			if(!whereClause.isEmpty()){	
				/*
				 * Unfortunately, have to override whereClause with quick&dirty work around below.
				 * For some reason Ambari added double quotes to all of the values in hive.
				 */
				whereClause = "WHERE name = '\"DatabaseComparisonTest\"'";		  
				sql += " " + whereClause;
			}
			log("sql: " + sql);
			rs = stmt.executeQuery(sql); 
			
			//NOTE: use only for debugging. Will effect timing of test if enabled. only works for SELECT_ALL_ROW_10_COL_TEST test
			//displayResultsInTable();

			log("Retrieved data successfully. \n");
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

	private int getRowCount(ResultSet resultSet, String whereClause){  
		log("Retrieving row count...."); 
		try {
			stmt = conn.createStatement();  
			String sql = "SELECT COUNT(1) FROM " + this.tableName;  
			if(!whereClause.isEmpty()){
				/*
				 * Unfortunately, have to override whereClause with quick&dirty work around below.
				 * For some reason Ambari added double quotes to all of the values in hive.
				 */
				whereClause = "WHERE name = '\"DatabaseComparisonTest\"'";	   
				sql += " " + whereClause;
			}
			log("sql: " + sql);
			rs = stmt.executeQuery(sql); 
			while (rs.next()){
				return rs.getInt(1);
			}
			log("Retrieved row count successfully. \n"); 
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return 0; 
		
		/*log("Retrieving row count...."); 
		int count = 0;
		try {
			while(resultSet.next()){
				System.out.println(resultSet.getString(1));  
				count++;
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block  
			e.printStackTrace();
		}
		return count;*/
	}
	
	@Override
	public Map<String, Object> runTest(String fileName, String testType, String tableName, String whereClause) {		  
		this.tableName = tableName;
		long time = 0;
		int rowCount = 0;
		this.connectToDB();
		//this.deleteTable();
		//this.createTable(testType);
		/*if(testType.equals(Tests.SELECT_100_ROW_10_COL_TEST) || testType.equals(Tests.SELECT_100_ROW_120_COL_TEST)){
			this.createIndexOnTable("name_index", "name");  //only works for the name column table test
		}*/
		//this.loadData("/usr/tmp/"+fileName);    
		//only time the data retrieval
		long start = System.currentTimeMillis();
		this.getData(whereClause); 
		time = (System.currentTimeMillis() - start);  
		rowCount = getRowCount(rs, whereClause);
		//this.deleteTable();
		this.closeConnection();
		Map<String,Object> testResult = new HashMap<String,Object>(); 
		testResult.put("time", time);
		testResult.put("rowCount", rowCount);
		return testResult;
	} 
	
	public static void main (String[] args){
		System.out.println("Test HiveHDFSDatabase: ");
		HiveHDFSDatabase hiveHDFSDatabase = new HiveHDFSDatabase("Hive"); 
		System.out.println("Executing \"" + hiveHDFSDatabase.getName() + "\"....");
		//Map<String,Object> testResult = hiveHDFSDatabase.runTest("TestFile_OneHundred.txt", Tests.SELECT_ALL_ROW_10_COL_TEST, "TestData",""); 
		//Map<String,Object> testResult = hiveHDFSDatabase.runTest("TestFile_10MillionRows_10Columns.txt", Tests.SELECT_100_ROW_10_COL_TEST, "TestData_10Col","WHERE name = '\"DatabaseComparisonTest\"'"); 
		Map<String,Object> testResult = hiveHDFSDatabase.runTest("TestFile_10MillionRows_120Columns.txt", Tests.SELECT_100_ROW_120_COL_TEST, "TestData_120Col","WHERE name = '\"DatabaseComparisonTest\"'"); 
		long time = (long) testResult.get("time");
		int rowCount = (int)testResult.get("rowCount");
		System.out.println("Took \"" + hiveHDFSDatabase.getName() + "\" " + time + " ms to read " +
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
