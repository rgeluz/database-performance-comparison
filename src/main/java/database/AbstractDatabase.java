package main.java.database;

import java.util.Map;

public abstract class AbstractDatabase {
	protected final String name;

	protected AbstractDatabase(String name){
		this.name = name;
	}
	
	public String getName() {
		return name;
	}

	protected abstract Boolean connectToDB(); 
	
	protected abstract void closeConnection();
	
	protected abstract void createTable(String testType);
	
	protected abstract void create10ColTable();
	
	protected abstract void createTableWithNameColumn(int numOfCols); 

	protected abstract void createIndexOnTable(String indexName, String colName);
	
	protected abstract void deleteTable();
	
	protected abstract void loadData(String filePath);  
	
	protected abstract long getData(String whereClause); 
	
	public abstract Map<String,Object> runTest(String fileName, String testType, String tableName, String whereClause);      
}
