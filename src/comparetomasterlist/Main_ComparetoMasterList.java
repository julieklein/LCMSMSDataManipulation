package comparetomasterlist;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main_ComparetoMasterList {
	public Main_ComparetoMasterList() throws SQLException {

		LinkedList<Output> outputList = new LinkedList<Output>();
		PrintStream csvWriter = null;

		int rsSize = 0;
		int id = 1;
		String queryPeptide = "SELECT * FROM CEMSMSSELECTED WHERE Confidence = 'High'";
		Connection connection = getConn();
		Statement s = connection.createStatement();
		try {
			ResultSet result = s.executeQuery(queryPeptide);
			rsSize = getResultSetSize(result);

			while (result.next()) {
				Data xampp = new Data();
				String sequence = result.getString("Sequence");
				xampp.sequence = sequence;
				String accession = result.getString("Protein_Accession");
				xampp.accession = accession;
				String symbol = result.getString("Protein_Symbol");
				xampp.symbol = symbol;
				double mass = result.getDouble("Average_ExperimentalMass_H");
				mass = mass - 1.007276;
				xampp.mass = mass;
				
				double ppm = 0;
				if (mass <= 800) {
					ppm = 50;
				} else if (mass>=1500) {
					ppm = 75;
				} else {
					ppm = 0.0018 * mass + 48.592;
				}
				double minmass = mass - mass * ppm / 1000000;
				double maxmass = mass + mass * ppm / 1000000;
				
				
				String Xcorr = result.getString("Xcorr");
				xampp.xcorr = Xcorr;
				
				
				double AverageRT = result.getDouble("Average_CalibratedRT");
				xampp.rt = AverageRT;
				
				double delta = 0;
				if (AverageRT <= 19) {
					delta = 1;
				} else if (AverageRT >= 50) {
					delta = 2.5;
				} else {
					delta = 0.0484 * AverageRT + 0.0806;
				}
				double minrt = AverageRT - delta;
				double maxrt = AverageRT + delta;
				
				xampp.id = id;

				BufferedReader bReader = createBufferedreader("//Users/julieklein/Documents/MosaiquesDatabase/ML40_01112011.txt");
				String line;
				bReader.readLine();
				while ((line = bReader.readLine()) != null) {
					String splitarray[] = line.split("\t");
					int id2 = Integer.parseInt(splitarray[0]);
					double mass2 = Double.parseDouble(splitarray[1].replaceAll(
							",", "."));
					double rt2 = Double.parseDouble(splitarray[2].replaceAll(
							",", "."));
					String sequence2 = "";
					if (!sequence2.equals("-")) {
						sequence2 = splitarray[7];
					}

					if (rt2 <= maxrt && rt2 >= minrt && mass2 <= maxmass
							&& mass2 >= minmass) {
						Output output = new Output();
						Data ml40 = new Data();
						ml40.id = id2;
						ml40.mass = mass2;
						ml40.rt = rt2;
						ml40.sequence = sequence2;
						if (sequence2.equals("-")) {
							output.equivalent = "";
						} else if (sequence.equalsIgnoreCase(sequence2)) {
							output.equivalent = "ok";
						} else {
							output.equivalent = "no";
						}
						output.setMl40(ml40);
						output.setXampp(xampp);
						outputList.add(output);
					}
				}
				id++;
			}
			result.close();
			s.close();
			connection.close();
		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + queryPeptide);
			ignore.printStackTrace();
		}

		try {
			System.out.println("-----------------");
			csvWriter = new PrintStream("comparisonCEMSMS_CEMS_High.txt");
			populateHeaders(csvWriter);
			for (Output output : outputList) {
				populateData(csvWriter, output);
			}

		} catch (FileNotFoundException ex) {
			Logger.getLogger(Main_ComparetoMasterList.class.getName()).log(
					Level.SEVERE, null, ex);
		} finally {
			csvWriter.close();
		}

	}

	public static void main(String[] args) throws IOException, SQLException {
		// TODO code application logic here
		Main_ComparetoMasterList Main_ComparetoMasterList = new Main_ComparetoMasterList();
	}

	private Connection getConn() {
		Connection conn = null;

		try {
			String host = "jdbc:mysql://localhost:3306/";
			String dbName = "FORTESTING";
			String usermame = "root";
			String pwd = "kschoicesql";

			// String host = "jdbc:mysql://srvW2008R2:3306/";
			// String dbName = "LCMSMSDATABASE";
			// String usermame = "jklein";
			// String pwd = "JK32485c";

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

	private void populateHeaders(PrintStream csvWriter) {
		csvWriter.print("MSMSid");
		csvWriter.print("\t");
		csvWriter.print("MSMSaccession");
		csvWriter.print("\t");
		csvWriter.print("MSMSSymbol");
		csvWriter.print("\t");
		csvWriter.print("MSMSmass");
		csvWriter.print("\t");
		csvWriter.print("MSMSrt");
		csvWriter.print("\t");
		csvWriter.print("MSMSsequence");
		csvWriter.print("\t");
		csvWriter.print("MSMSxcorr");
		csvWriter.print("\t");
		csvWriter.print("MSid");
		csvWriter.print("\t");
		csvWriter.print("MSmass");
		csvWriter.print("\t");
		csvWriter.print("MSrt");
		csvWriter.print("\t");
		csvWriter.print("MSsequence");
		csvWriter.print("\t");
		csvWriter.print("Status");
		csvWriter.print("\n");
	}

	private void populateData(PrintStream csvWriter, Output output) {
		csvWriter.print(output.xampp.id);
		csvWriter.print("\t");
		csvWriter.print(output.xampp.accession);
		csvWriter.print("\t");
		csvWriter.print(output.xampp.symbol);
		csvWriter.print("\t");
		csvWriter.print(output.xampp.mass);
		csvWriter.print("\t");
		csvWriter.print(output.xampp.rt);
		csvWriter.print("\t");
		csvWriter.print(output.xampp.sequence);
		csvWriter.print("\t");
		csvWriter.print(output.xampp.xcorr);
		csvWriter.print("\t");
		csvWriter.print(output.ml40.id);
		csvWriter.print("\t");
		csvWriter.print(output.ml40.mass);
		csvWriter.print("\t");
		csvWriter.print(output.ml40.rt);
		csvWriter.print("\t");
		csvWriter.print(output.ml40.sequence);
		csvWriter.print("\t");
		csvWriter.print(output.equivalent);
		csvWriter.print("\n");
	}

}
