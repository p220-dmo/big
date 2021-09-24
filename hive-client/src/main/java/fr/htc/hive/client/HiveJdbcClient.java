package fr.htc.hive.client;

import java.sql.SQLException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.sql.DriverManager;

public class HiveJdbcClient {
	
	private static String driverName = "org.apache.hadoop.hive.jdbc.HiveDriver";

	public static void main(String[] args) throws SQLException {
		try {
			Class.forName(driverName);
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
			System.exit(1);
		}
		if(args.length < 3) {
			System.out.println("Table name is mandatory !!!!");
			System.exit(1);
		}
		String userName = args[0];
		String password = args[1];
		String tableName = args[2];
		
		Connection con = getHiveConnection(userName, password);
		
		
		Statement stmt = init(tableName, con);
		
		
		createAndShowTable(tableName, stmt);
		DescribeTable(tableName, stmt);

		loadDataOnTable(tableName, stmt);

		selectAllAndPrint(tableName, stmt);

		// regular hive query
		tableSize(tableName, stmt);
	}

	/**
	 * 
	 * @param tableName
	 * @param stmt
	 * @throws SQLException
	 */
	private static void tableSize(String tableName, Statement stmt) throws SQLException {
		ResultSet res;
		String sql;
		sql = "select count(1) from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1));
		}
	}

	/**
	 * 
	 * @param tableName
	 * @param stmt
	 * @throws SQLException
	 */
	private static void selectAllAndPrint(String tableName, Statement stmt) throws SQLException {
		ResultSet res;
		String sql;
		// select * query
		sql = "select * from " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(String.valueOf(res.getInt(1)) + "\t" + res.getString(2));
		}
	}

	/**
	 * 
	 * @param tableName
	 * @param stmt
	 * @throws SQLException
	 */
	private static void loadDataOnTable(String tableName, Statement stmt) throws SQLException {
		String sql;
		// Charger le données  doive être interne au noeud ou tourve hive
		
		String filepath = "/data-input/data.csv";
		sql = "load data local inpath '" + filepath + "' into table " + tableName;
		System.out.println("Running: " + sql);
		ResultSet res = stmt.executeQuery(sql);
	}

	/**
	 * 
	 * @param tableName
	 * @param stmt
	 * @throws SQLException
	 */
	private static void DescribeTable(String tableName, Statement stmt) throws SQLException {
		ResultSet res;
		String sql;
		// describe table
		sql = "describe " + tableName;
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		while (res.next()) {
			System.out.println(res.getString(1) + "\t" + res.getString(2));
		}
	}
	
	/**
	 * 
	 * @param tableName
	 * @param stmt
	 * @throws SQLException
	 */
	private static void createAndShowTable(String tableName, Statement stmt) throws SQLException {
		ResultSet res = stmt.executeQuery("create table " + tableName + " (key int, value string)");
		// show tables
		String sql = "show tables '" + tableName + "'";
		System.out.println("Running: " + sql);
		res = stmt.executeQuery(sql);
		if (res.next()) {
			System.out.println(res.getString(1));
		}
	}
	
	/**
	 * 
	 * @param tableName
	 * @param con
	 * @return
	 * @throws SQLException
	 */
	private static Statement init(String tableName, Connection con) throws SQLException {
		Statement stmt = con.createStatement();
		
		stmt.executeQuery("drop table " + tableName);
		return stmt;
	}
	/**
	 * 
	 * @param userName
	 * @param password
	 * @return
	 * @throws SQLException
	 */
	private static Connection getHiveConnection(String userName, String password) throws SQLException {
		Connection con = DriverManager.getConnection("jdbc:hive://localhost:10000/default", userName, password);
		return con;
	}
}