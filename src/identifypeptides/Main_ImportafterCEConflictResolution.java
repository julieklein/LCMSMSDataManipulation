package identifypeptides;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;

public class Main_ImportafterCEConflictResolution {
	public Main_ImportafterCEConflictResolution() throws IOException,
			SQLException {
		BufferedReader bReader = createBufferedreader("//Users/julieklein/Desktop/CEMSMSConflictNew.txt");
		String line;
		while ((line = bReader.readLine()) != null) {
			String splitarray[] = line.split("\t");
			String Resolution = splitarray[14];
			String info = splitarray[5];
			info = info.replaceAll("\"", "");

			String insertpeptide = "INSERT INTO CEMSMSCONFLICT VALUES('"
					+ splitarray[0] + "', " + Double.parseDouble(splitarray[1])
					+ ", " + Double.parseDouble(splitarray[2]) + ", '"
					+ splitarray[3] + "', '" + splitarray[4] + "', '" + info
					+ "', " + Double.parseDouble(splitarray[6]) + ","
					+ Double.parseDouble(splitarray[7]) + ","
					+ Double.parseDouble(splitarray[8]) + ", "
					+ Double.parseDouble(splitarray[9]) + ", "
					+ Integer.parseInt(splitarray[10]) + ", "
					+ Double.parseDouble(splitarray[11]) + " , "
					+ Integer.parseInt(splitarray[12]) + ", '" + splitarray[13]
					+ "', '" + splitarray[14] + "')";
			Connection connection4 = getConn();
			Statement s4 = connection4.createStatement();
			try {
				int result4 = s4.executeUpdate(insertpeptide);
				s4.close();
				connection4.close();
			} catch (Throwable ignore) {
				System.err.println("Mysql Statement Error: " + insertpeptide);
				ignore.printStackTrace();
			}

		}

	}

	public static void main(String[] args) throws IOException, SQLException {
		// TODO code application logic here
		Main_ImportafterCEConflictResolution Main_ImportafterCEConflictResolution = new Main_ImportafterCEConflictResolution();
	}

	private Connection getConn() {
		Connection conn = null;

		try {
			// String host = "jdbc:mysql://localhost:3306/";
			// String dbName = "MSMSDatabase";
			// String usermame = "root";
			// String pwd = "kschoicesql";
			String host = "jdbc:mysql://srvW2008R2.samba:3306/";
			String dbName = "LCMSMSDatabase";
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

	private BufferedReader createBufferedreader(String datafilename)
			throws FileNotFoundException {
		BufferedReader bReader = new BufferedReader(
				new FileReader(datafilename));
		return bReader;

	}

}
