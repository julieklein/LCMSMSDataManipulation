package test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class Main_test {
	public Main_test() throws SQLException, IOException {
		
		BufferedReader bReader = createBufferedreader("//Users/julieklein/Documents/MosaiquesDatabase/ML40_01112011.txt");
		String line;
		bReader.readLine();
		while ((line = bReader.readLine()) != null) {
			
		}
		
		int rsSize = 0;
		String queryPeptide = "SELECT * FROM LCMSMSCOMPLETEDATA WHERE Sequence = 'EAGRDGNpGNDGPpGRDGQpGHKGER'";
		Connection connection = getConn();
		Statement s = connection.createStatement();
		try {
			ResultSet result = s.executeQuery(queryPeptide);
			rsSize = getResultSetSize(result);

			while (result.next()) {
				double Xcorr = result.getDouble("Xcorr");
				System.out.println(Xcorr);
			}
			result.close();
			s.close();
			connection.close();
		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + queryPeptide);
			ignore.printStackTrace();
		}
		
	}
	public static void main(String[] args) throws IOException, SQLException {
		// TODO code application logic here
		Main_test Main_test = new Main_test();
	}

	private Connection getConn() {
		Connection conn = null;

		try {
//			String host = "jdbc:mysql://localhost:3306/";
//			String dbName = "FORTESTING";
//			String usermame = "root";
//			String pwd = "kschoicesql";

			 String host = "jdbc:mysql://srvW2008R2.samba:3306/";
			 String dbName = "LCMSMSDATABASE";
			 String usermame = "jklein";
			 String pwd = "JK32485c";

			conn = DriverManager.getConnection(host + dbName + "?user="
					+ usermame + "&password=" + pwd);

			System.out.println("Connection Success");

		} catch (Exception e) {

			// error
			System.err.println("Mysql Connection Error: ");

			// for debugging error
			e.printStackTrace();
		}

		return conn;
	}

	private int getResultSetSize(ResultSet resultSet) {
		int size = -1;

		try {
			resultSet.last();
			size = resultSet.getRow();
			resultSet.beforeFirst();
		} catch (SQLException e) {
			return size;
		}

		return size;
	}
	private BufferedReader createBufferedreader(String datafilename)
			throws FileNotFoundException {
		BufferedReader bReader = new BufferedReader(
				new FileReader(datafilename));
		return bReader;

	}

}
