package org.example;

import com.datastax.oss.driver.api.core.CqlSession;
import org.apache.calcite.adapter.cassandra.CassandraSchema;
import org.apache.calcite.adapter.java.ReflectiveSchema;
import org.apache.calcite.jdbc.CalciteConnection;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Scanner;

public class Main {

    public static void main(String[] args) throws SQLException, ClassNotFoundException, URISyntaxException {

        // Schema Definition file("model-cassandra.json")
        URL res = Main.class.getClassLoader().getResource("model-cassandra.json");
        File file = Paths.get(res.toURI()).toFile();
        System.out.println(res);
        String absolutePath = file.getAbsolutePath();

        // Calcite JDBC Driver
        Class.forName("org.apache.calcite.jdbc.Driver");

        Scanner sc = new Scanner(System.in);
        System.out.println("Please write a query statement.");
        String query = sc.nextLine();
        while(!query.equals("q")){
            try {
                Connection connection = DriverManager.getConnection("jdbc:calcite:model=" + absolutePath);
                CalciteConnection calciteConnection = (CalciteConnection) connection;
                Statement statement = calciteConnection.createStatement();
                ResultSet rs = statement.executeQuery(query);

                System.out.println("succeeded.");

                ResultSetMetaData rsmd = rs.getMetaData();
                List<String> columns = new ArrayList<String>(rsmd.getColumnCount());

                System.out.println("\n==== Column names of test_table_ex1====\n");

                for (int i = 1; i <= rsmd.getColumnCount(); i++) {
                    // Print column names
                    System.out.println(rsmd.getColumnName(i));
                    columns.add(rsmd.getColumnName(i));
                }

                System.out.println("\n=== Result of the query ===\n");
                while (rs.next()) {
                    for (String col : columns) {
                        System.out.println("" + col + " : " + rs.getString(col));
                    }
                    System.out.println("---------");
                }


                System.out.println("\n\nPlease write a query statement.");
                query = sc.nextLine();
            }catch (SQLException e){
                System.out.println(e.getMessage());
                System.out.println("\n\nPlease correct the query statement.");
                query = sc.nextLine();
                continue;
            }
        }

    }
}