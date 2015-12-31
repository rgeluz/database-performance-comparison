package main.java.database;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class Databases {
	private static final List<AbstractDatabase> databases = Arrays.asList(
			new PostgreSQLDatabase("PostgreSQL"),
			new HiveHDFSDatabase("Hive")
			//add more later
			);
	private Databases() { }
	public static List<AbstractDatabase> list() {
		return Collections.unmodifiableList(databases);
	}
}
