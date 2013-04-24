package transferdatatonewdb;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.net.URL;
import java.sql.Array;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.Ref;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.RowId;
import java.sql.SQLException;
import java.sql.SQLWarning;
import java.sql.SQLXML;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Calendar;
import java.util.Map;

public class Main_TransferData {
	public Main_TransferData() throws SQLException {

		int rsSize = 0;
		int minLimit = 0;
		String queryProtease = "";

		try {

			// for (int i = 1; i<2281; i++) {

			// Connection connection = getConn();
			// Statement s = connection.createStatement();
			// // queryProtease =
			// "SELECT * FROM LCMSMSCOMPLETEDATA  where spectrum_File = '10302-GvHD-Auftrag-Urin-24459-2427-B.raw'";
			// queryProtease = "SELECT * FROM CEMSMSconflict";
			// ResultSet result = s.executeQuery(queryProtease);
			// rsSize = getResultSetSize(result);
			// System.out.println(queryProtease);
			// System.out.println(rsSize);
			//
			// while (result.next()) {
			// String insertpeptide = "INSERT INTO CEMSMSconflict VALUES('"
			// + result.getString("Muster_ID") + "', "
			// + result.getDouble("Muster_Exp_Mass") + ", "
			// + result.getDouble("Muster_CE_t") + ", '"
			// + result.getString("Muster_Old_Sequence") + "', '"
			// + result.getString("Sequence") + "', '"
			// + result.getString("Protein_Info") + "', "
			// + result.getDouble("TheoriticalMass_Da") + ", "
			// + result.getDouble("Average_ExperimentalMass_H_Da") + ", "
			// + result.getDouble("Average_m_z_Da") + ", "
			// + result.getDouble("Average_Xcorr") + ", "
			// + result.getInt("Nb_Basic_Aa") + ", "
			// + result.getDouble("Average_Calibrated_CE_t_min") + ", "
			// + result.getInt("Occurrences") + ", '"
			// + result.getString("Confidence") + "', '"
			// + result.getString("CE_Valid_Sequence") + "', '"
			// + result.getString("Status") + "')";

			// String insertpeptide = "INSERT INTO musterlist40 VALUES("
			// + result.getInt("idMuster") + ", "
			// + result.getDouble("ExperimentalMass_Da") + ", "
			// + result.getDouble("CE_t_min") + ", "
			// + result.getDouble("Amplitude") + ", "
			// + result.getInt("Frequency") + ", "
			// + result.getDouble("Std_Dev_Mass") + ", "
			// + result.getDouble("Std_Dev_CE_t") + ", '"
			// + result.getString("Old_Sequence") + "', '"
			// + result.getString("New_Sequence") + "')";
			File f = new File(
					"//Users/julieklein/Documents/MosaiquesDatabase/LCMSMSdata/textfiles/confidence/fait/fait");
			File[] files = f.listFiles();
			for (File file : files) {
				String filepath = "/" + file.getPath();
				if (!filepath.contains("DS_Store")
						&& !filepath.contains("Extrapolated")) {
					BufferedReader bReader = createBufferedreader(filepath);
					System.out.println(filepath);
					String line;
					bReader.readLine();
					String insertpeptide = "";
					int i=0;
					while ((line = bReader.readLine()) != null) {
						System.out.println(i);
						i++;
						String splitarray[] = line.split("\t");
						int confCX = Integer.parseInt(splitarray[1]);
						int confHp = Integer.parseInt(splitarray[2]);
						if (confCX != 0 && confHp != 0) {
							insertpeptide = "INSERT IGNORE INTO LCMSMSCOMPLETEDATA_wocxHp VALUES("
									+ Double.parseDouble(splitarray[0])
									+ ", "
									+ Integer.parseInt(splitarray[1])
									+ ", "
									+ Integer.parseInt(splitarray[2])
									+ ", "
									+ Integer.parseInt(splitarray[3])
									+ ", "
									+ Double.parseDouble(splitarray[4])
									+ ", '"
									+ splitarray[5]
									+ "', '"
									+ splitarray[6]
									+ "', '"
									+ splitarray[7]
									+ "', '"
									+ splitarray[8]
									+ "', '"
									+ splitarray[9]
									+ "', "
									+ Double.parseDouble(splitarray[10])
									+ ", "
									+ Double.parseDouble(splitarray[11])
									+ ", "
									+ Double.parseDouble(splitarray[12])
									+ ", "
									+ Double.parseDouble(splitarray[13])
									+ ", "
									+ Double.parseDouble(splitarray[14])
									+ ", "
									+ Integer.parseInt(splitarray[15])
									+ ", "
									+ Double.parseDouble(splitarray[16])
									+ ", "
									+ Double.parseDouble(splitarray[17])
									+ ", "
									+ Double.parseDouble(splitarray[18])
									+ ", "
									+ Double.parseDouble(splitarray[19])
									+ ", '"
									+ splitarray[20]
									+ "', "
									+ Integer.parseInt(splitarray[21])
									+ ", "
									+ Integer.parseInt(splitarray[22])
									+ ", '"
									+ splitarray[23]
									+ "', '"
									+ splitarray[24]
									+ "', '"
									+ splitarray[25]
									+ "', '"
									+ splitarray[26]
									+ "', '"
									+ splitarray[27]
									+ "', '"
									+ splitarray[28]
									+ "', '"
									+ splitarray[29]
									+ "', "
									+ Double.parseDouble(splitarray[30])
									+ ", "
									+ Double.parseDouble(splitarray[31])
									+ ", "
									+ Double.parseDouble(splitarray[32]) + ")";
						} else {
							insertpeptide = "INSERT IGNORE INTO LCMSMSCOMPLETEDATA_cxHp VALUES("
									+ Double.parseDouble(splitarray[0])
									+ ", "
									+ Integer.parseInt(splitarray[1])
									+ ", "
									+ Integer.parseInt(splitarray[2])
									+ ", "
									+ Integer.parseInt(splitarray[3])
									+ ", "
									+ Double.parseDouble(splitarray[4])
									+ ", '"
									+ splitarray[5]
									+ "', '"
									+ splitarray[6]
									+ "', '"
									+ splitarray[7]
									+ "', '"
									+ splitarray[8]
									+ "', '"
									+ splitarray[9]
									+ "', "
									+ Double.parseDouble(splitarray[10])
									+ ", "
									+ Double.parseDouble(splitarray[11])
									+ ", "
									+ Double.parseDouble(splitarray[12])
									+ ", "
									+ Double.parseDouble(splitarray[13])
									+ ", "
									+ Double.parseDouble(splitarray[14])
									+ ", "
									+ Integer.parseInt(splitarray[15])
									+ ", "
									+ Double.parseDouble(splitarray[16])
									+ ", "
									+ Double.parseDouble(splitarray[17])
									+ ", "
									+ Double.parseDouble(splitarray[18])
									+ ", "
									+ Double.parseDouble(splitarray[19])
									+ ", '"
									+ splitarray[20]
									+ "', "
									+ Integer.parseInt(splitarray[21])
									+ ", "
									+ Integer.parseInt(splitarray[22])
									+ ", '"
									+ splitarray[23]
									+ "', '"
									+ splitarray[24]
									+ "', '"
									+ splitarray[25]
									+ "', '"
									+ splitarray[26]
									+ "', '"
									+ splitarray[27]
									+ "', '"
									+ splitarray[28]
									+ "', '"
									+ splitarray[29]
									+ "', "
									+ Double.parseDouble(splitarray[30])
									+ ", "
									+ Double.parseDouble(splitarray[31])
									+ ", "
									+ Double.parseDouble(splitarray[32]) + ")";

							// String insertpeptide =
							// "INSERT INTO LCMSMSCOMPLETEDATA VALUES("
							// + result.getDouble("ConfTotal") + ", "
							// + result.getDouble("ConfCystein_X") + ", "
							// + result.getDouble("ConfOxidation") + ", "
							// + result.getDouble("ConfDeltaMass") + ", "
							// + result.getDouble("Rank") + ", '"
							// + result.getString("Sequence") + "', '"
							// + result.getString("Protein_Accession") + "', '"
							// + result.getString("Protein_Symbol") + "', '"
							// + result.getString("Protein_Name") + "', '"
							// + result.getString("Modifications") + "', "
							// + result.getDouble("Xcorr") + ", "
							// + result.getDouble("m_z_Da") + ", "
							// + result.getDouble("ExperimentalMass_H_Da") +
							// ", "
							// + result.getDouble("TheoriticalMass_Da") + ", "
							// + result.getDouble("DeltaMass_ppm") + ", "
							// + result.getInt("Nb_Basic_Aa") + ", "
							// + result.getDouble("CE_t_min") + ", "
							// + result.getDouble("Calibrated_CE_t_min") + ", "
							// + result.getDouble("First_Scan") + ", "
							// + result.getDouble("Last_Scan") + ", '"
							// + result.getString("MS_Order") + "', "
							// + result.getInt("Ions_Matched") + ", "
							// + result.getInt("Ions_Total") + ", '"
							// + result.getString("Spectrum_File") + "', '"
							// + result.getString("Confidence_Icon") + "', '"
							// + result.getString("Nb_Proteins") + "', '"
							// + result.getString("Nb_Protein_Groups") + "', '"
							// + result.getString("Activation_Type") + "', '"
							// + result.getString("Probability") + "', '"
							// + result.getString("Delta_Score") + "', "
							// + result.getDouble("Area") + ", "
							// + result.getDouble("Intensity") + ", "
							// + result.getDouble("Charge") + ")";
						}
						Connection connection4 = getConn2();
						Statement s4 = connection4.createStatement();
						try {
							int result4 = s4.executeUpdate(insertpeptide);
							s4.close();
							connection4.close();
						} catch (Throwable ignore) {
							System.err.println("Mysql Statement Error: "
									+ insertpeptide);
							ignore.printStackTrace();
						}
					}

				}
				file.delete();
			}
			// result.close();
			// s.close();
			// connection.close();
			// minLimit = 1000*i + 1;
			// }

		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + queryProtease);
			ignore.printStackTrace();
		}

	}

	public static void main(String[] args) throws IOException, SQLException {
		// TODO code application logic here
		Main_TransferData Main_TransferData = new Main_TransferData();
	}

	private Connection getConn() {
		Connection conn = null;

		try {
			String host = "jdbc:mysql://localhost:3306/";
			String dbName = "MSMSDatabase";
			String usermame = "root";
			String pwd = "kschoicesql";
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

	private Connection getConn2() {
		Connection conn = null;

		try {
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
