package identifypeptides;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.swing.ListCellRenderer;

import org.apache.tools.ant.types.CommandlineJava.SysProperties;

import applyconfidence.Output;

public class Main_Identifypeptides_mixed {
	public Main_Identifypeptides_mixed() throws SQLException {

		// Find highest number of occurences/highest Xcorr in
		// CEMSMSSELECTED/CEMSMSCONFLICT and LCMSMSSELECTED/LCMSMSCONFLICT
		List<Integer> CEOcc = new ArrayList<Integer>();
		List<Double> CEXcorr = new ArrayList<Double>();
		List<Integer> LCOcc = new ArrayList<Integer>();
		List<Double> LCXcorr = new ArrayList<Double>();

		int iCEOcc = 0;
		double dCEXcorr = 0;
		int iLCOcc = 0;
		double dLCXCorr = 0;

		String queryCEMSMSSELECTED = "SELECT * FROM CEMSMSSELECTED";
		String queryCEMSMSCONFLICT = "SELECT * FROM CEMSMSCONFLICT";
		String queryLCMSMSSELECTED = "SELECT * FROM LCMSMSSELECTED";
		String queryLCMSMSCONFLICT = "SELECT * FROM LCMSMSCONFLICT";

		CEOcc = countOccurence(queryCEMSMSSELECTED, iCEOcc, CEOcc);
		CEOcc = countOccurence(queryCEMSMSCONFLICT, iCEOcc, CEOcc);
		CEXcorr = countXCorr(queryCEMSMSSELECTED, dCEXcorr, CEXcorr);
		CEXcorr = countXCorr(queryCEMSMSCONFLICT, dCEXcorr, CEXcorr);

		LCOcc = countOccurence(queryLCMSMSSELECTED, iLCOcc, LCOcc);
		LCOcc = countOccurence(queryLCMSMSCONFLICT, iLCOcc, LCOcc);
		LCXcorr = countXCorr(queryLCMSMSSELECTED, dLCXCorr, LCXcorr);
		LCXcorr = countXCorr(queryLCMSMSCONFLICT, dLCXCorr, LCXcorr);

		Collections.sort(CEOcc);
		Collections.sort(CEXcorr);
		Collections.sort(LCOcc);
		Collections.sort(LCXcorr);

		int size1 = CEOcc.size();
		int size2 = CEXcorr.size();
		int size3 = LCOcc.size();
		int size4 = LCXcorr.size();

		int maxCEOcc = CEOcc.get(size1 - 1);
		double maxCEXcorr = CEXcorr.get(size2 - 1);
		int maxLCOcc = LCOcc.get(size3 - 1);
		double maxLCXcorr = LCXcorr.get(size4 - 1);
		System.out.println(maxCEOcc);
		System.out.println(maxCEXcorr);
		System.out.println(maxLCOcc);
		System.out.println(maxLCXcorr);

		// FOREACH MusterID in CEMSMSSELECTED
		Connection connection = getConn();
		Statement s = connection.createStatement();
		try {
			ResultSet result = s.executeQuery(queryCEMSMSSELECTED);
			while (result.next()) {
				String MusterID = "";
				double MusterMass = 0;
				double MusterTime = 0;
				String OldSeq = "";
				String NewSeq = "";
				String ProtInfo = "";
				int CEoccurences = 0;
				double CExcorr = 0;
				int LCoccurences = 0;
				double LCxcorr = 0;
				double ConfidenceScore = 0;
				double CEmz = 0;
				double LCmz = 0;
				double Averagemz = 0;

				MusterID = result.getString("Muster_ID");
				OldSeq = result.getString("Muster_Old_Sequence");
				MusterTime = result.getDouble("Muster_CE_t");
				MusterMass = result.getDouble("Muster_Exp_Mass");

				NewSeq = result.getString("Sequence");
				ProtInfo = result.getString("Protein_Info");
				CEmz = result.getDouble("Average_m_z_Da");
				CEoccurences = result.getInt("Occurrences");
				CExcorr = result.getDouble("Average_Xcorr");
				double CEvectorfrequency = CEoccurences * 1 / maxCEOcc;
				double CEvectoroccurence = CExcorr * 1 / maxCEXcorr;

				// Check LCMSMSSELECTED
				String queryLCMSMSSELECTEDALSO = "SELECT * FROM LCMSMSSELECTED WHERE Muster_ID ='"
						+ MusterID + "'";
				int rsSize2 = 0;
				Connection connection2 = getConn();
				Statement s2 = connection2.createStatement();
				try {
					ResultSet result2 = s2
							.executeQuery(queryLCMSMSSELECTEDALSO);
					rsSize2 = getResultSetSize(result2);

					if (rsSize2 == 0) {
						// NOT in LCMSMSSELECTED; Check LCMSMSCONFLICT
						String queryLCMSMSCONFLICTALSO = "SELECT * FROM LCMSMSCONFLICT WHERE Muster_ID ='"
								+ MusterID + "'";
						int rsSize3 = 0;
						Connection connection3 = getConn();
						Statement s3 = connection3.createStatement();
						try {
							ResultSet result3 = s3
									.executeQuery(queryLCMSMSCONFLICTALSO);
							ResultSet result4 = s3
									.executeQuery(queryLCMSMSCONFLICTALSO);
							rsSize3 = getResultSetSize(result3);

							if (rsSize3 == 0) {
								System.out.println("CE+ LC- HC");
								// MusterID only in CESELECTED : CE+ LC- HC
								ConfidenceScore = Math.sqrt(CEvectorfrequency
										* CEvectorfrequency + CEvectoroccurence
										* CEvectoroccurence + 0.5 * 0.5);
								Averagemz = (CEmz + LCmz) / 2;
								updateHighConfidenceDB(MusterID, MusterMass,
										MusterTime, OldSeq, NewSeq, ProtInfo,
										Averagemz, CEoccurences, CExcorr, 0, 0,
										ConfidenceScore);
							} else {	
								// MusterID both CESELECTED and LCCONFLICT;
								// check if contains same sequence
								boolean samesequence = isSameSequence(NewSeq,
										result3);
								;
								if (samesequence) {
									System.out.println("CE+ LC+ / many CE- LC+ conflict");
									// CE+ LC+ / many CE- LC+ conflict
									while (result4.next()) {
										String LCSeq = result4
												.getString("Sequence");
										String LCProtInfo = result4
												.getString("Protein_Info");
										LCmz = result4
												.getDouble("Average_m_z_Da");
										LCoccurences = result4
												.getInt("Occurrences");
										LCxcorr = result4
												.getDouble("Average_Xcorr");
										double LCvectorfrequency = LCoccurences
												* 1 / maxLCOcc;
										double LCvectoroccurence = LCxcorr * 1
												/ maxLCXcorr;
										if (LCSeq.equals(NewSeq)) {
											ConfidenceScore = Math.sqrt(CEvectorfrequency
													* CEvectorfrequency + CEvectoroccurence
													* CEvectoroccurence + LCvectorfrequency
													* LCvectorfrequency + LCvectoroccurence
													* LCvectoroccurence + 1 * 1);
											Averagemz = (CEmz + LCmz) / 2;
											updateConflictHighConfidenceDB(MusterID, MusterMass,
													MusterTime, OldSeq, NewSeq, ProtInfo,
													Averagemz, CEoccurences, CExcorr,
													LCoccurences, LCxcorr, ConfidenceScore);
										} else {
											ConfidenceScore = Math
													.sqrt(LCvectorfrequency
															* LCvectorfrequency
															+ LCvectoroccurence
															* LCvectoroccurence + 0
															* 0);
											Averagemz = (0 + LCmz) / 2;
											updateConflictHighConfidenceDB(
													MusterID, MusterMass,
													MusterTime, OldSeq, LCSeq,
													LCProtInfo, Averagemz, 0, 0,
													LCoccurences, LCxcorr,
													ConfidenceScore);
										}

									}

								} else {
									System.out.println("CE+ LC- / many CE- LC+ conflict");
									// CE+ LC- / many CE- LC+ conflict
									ConfidenceScore = Math
											.sqrt(CEvectorfrequency
													* CEvectorfrequency
													+ CEvectoroccurence
													* CEvectoroccurence + 0.5
													* 0.5);
									Averagemz = (CEmz + 0) / 2;
									updateConflictHighConfidenceDB(MusterID,
											MusterMass, MusterTime, OldSeq,
											NewSeq, ProtInfo, Averagemz,
											CEoccurences, CExcorr, 0, 0,
											ConfidenceScore);
									while (result4.next()) {
										String LCSeq = result4
												.getString("Sequence");
										String LCProtInfo = result4
												.getString("Protein_Info");
										LCmz = result4
												.getDouble("Average_m_z_Da");
										LCoccurences = result4
												.getInt("Occurrences");
										LCxcorr = result4
												.getDouble("Average_Xcorr");
										double LCvectorfrequency = LCoccurences
												* 1 / maxLCOcc;
										double LCvectoroccurence = LCxcorr * 1
												/ maxLCXcorr;
										ConfidenceScore = Math
												.sqrt(LCvectorfrequency
														* LCvectorfrequency
														+ LCvectoroccurence
														* LCvectoroccurence + 0
														* 0);
										Averagemz = (0 + LCmz) / 2;
										updateConflictHighConfidenceDB(
												MusterID, MusterMass,
												MusterTime, OldSeq, LCSeq,
												LCProtInfo, Averagemz, 0, 0,
												LCoccurences, LCxcorr,
												ConfidenceScore);
									}

								}

							}
							result3.close();
							result4.close();
							s3.close();
							connection3.close();
						} catch (Throwable ignore) {
							System.err.println("Mysql Statement Error: "
									+ queryLCMSMSCONFLICTALSO);
							ignore.printStackTrace();
						}

					} else {
						
						// MusterID both CESELECTED and LCSELECTED
						String LCSeq = result2.getString("Sequence");
						String LCProtInfo = result2.getString("Protein_Info");
						LCmz = result2.getDouble("Average_m_z_Da");
						LCoccurences = result2.getInt("Occurrences");
						LCxcorr = result2.getDouble("Average_Xcorr");

						if (LCSeq.equals(NewSeq)) {
							System.out.println("CE+ LC+ HC");
							// same sequence : CE+ LC+ HC
							double LCvectorfrequency = LCoccurences * 1
									/ maxLCOcc;
							double LCvectoroccurence = LCxcorr * 1 / maxLCXcorr;
							ConfidenceScore = Math.sqrt(CEvectorfrequency
									* CEvectorfrequency + CEvectoroccurence
									* CEvectoroccurence + LCvectorfrequency
									* LCvectorfrequency + LCvectoroccurence
									* LCvectoroccurence + 1 * 1);
							Averagemz = (CEmz + LCmz) / 2;
							updateHighConfidenceDB(MusterID, MusterMass,
									MusterTime, OldSeq, NewSeq, ProtInfo,
									Averagemz, CEoccurences, CExcorr,
									LCoccurences, LCxcorr, ConfidenceScore);
						} else {
							System.out.println("CE+ LC- / CE- LC+ Conflict");
							// conflict sequences (one only in CE and one only
							// in LC): CE+ LC- / CE- LC+ Conflict
							double LCvectorfrequency = LCoccurences * 1
									/ maxLCOcc;
							double LCvectoroccurence = LCxcorr * 1 / maxLCXcorr;
							// one only in CE
							ConfidenceScore = Math.sqrt(CEvectorfrequency
									* CEvectorfrequency + CEvectoroccurence
									* CEvectoroccurence + 0.5 * 0.5);
							Averagemz = (CEmz + 0) / 2;
							updateConflictHighConfidenceDB(MusterID,
									MusterMass, MusterTime, OldSeq, NewSeq,
									ProtInfo, Averagemz, CEoccurences, CExcorr,
									0, 0, ConfidenceScore);
							// one only in LC
							ConfidenceScore = Math.sqrt(LCvectorfrequency
									* LCvectorfrequency + LCvectoroccurence
									* LCvectoroccurence + 0 * 0);
							Averagemz = (0 + LCmz) / 2;
							updateConflictHighConfidenceDB(MusterID,
									MusterMass, MusterTime, OldSeq, LCSeq,
									LCProtInfo, Averagemz, 0, 0, LCoccurences,
									LCxcorr, ConfidenceScore);
						}
					}
					result2.close();
					s2.close();
					connection2.close();
				} catch (Throwable ignore) {
					System.err.println("Mysql Statement Error: "
							+ queryCEMSMSSELECTED);
					ignore.printStackTrace();
				}

			}

			result.close();
			s.close();
			connection.close();
		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + queryCEMSMSSELECTED);
			ignore.printStackTrace();
		}

	}

	private boolean isSameSequence(String NewSeq, ResultSet result3)
			throws SQLException {
		while (result3.next()) {
			String LCseq = result3.getString("Sequence");
			if (LCseq.equals(NewSeq)) {
				return true;
			}
		}
		return false;
	}

	private List<Integer> countOccurence(String query, int occurence,
			List<Integer> listocc) throws SQLException {
		Connection connectionCount = getConn();
		Statement sCount = connectionCount.createStatement();
		try {
			ResultSet resultCount = sCount.executeQuery(query);
			while (resultCount.next()) {
				occurence = resultCount.getInt("Occurrences");
				listocc.add(occurence);
			}
			resultCount.close();
			sCount.close();
			connectionCount.close();
		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + query);
			ignore.printStackTrace();
		}
		return listocc;
	}

	private List<Double> countXCorr(String query, double xcorr,
			List<Double> listxcorr) throws SQLException {
		Connection connectionCount = getConn();
		Statement sCount = connectionCount.createStatement();
		try {
			ResultSet resultCount = sCount.executeQuery(query);
			while (resultCount.next()) {
				xcorr = resultCount.getDouble("Average_Xcorr");
				listxcorr.add(xcorr);
			}
			resultCount.close();
			sCount.close();
			connectionCount.close();
		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + query);
			ignore.printStackTrace();
		}
		return listxcorr;
	}

	private void updateHighConfidenceDB(String MusterID, double MusterMass,
			double MusterTime, String OldSeq, String NewSeq, String ProtInfo,
			double Averagemz, int CEoccurences, double CExcorr,
			int LCoccurences, double LCxcorr, double ConfidenceScore)
			throws SQLException {

		int rsSize3 = 0;
		String selectedPeptide = "SELECT * FROM highconfidencepeptide WHERE Muster_ID = '"
				+ MusterID + "'";
		Connection connection3 = getConn();
		Statement s3 = connection3.createStatement();
		try {
			ResultSet result3 = s3.executeQuery(selectedPeptide);
			rsSize3 = getResultSetSize(result3);
			if (rsSize3 > 0) {
				updatePeptideS(MusterID, MusterMass, MusterTime, OldSeq,
						NewSeq, ProtInfo, Averagemz, CEoccurences, CExcorr,
						LCoccurences, LCxcorr, ConfidenceScore);
			} else {
				insertPeptideS(MusterID, MusterMass, MusterTime, OldSeq,
						NewSeq, ProtInfo, Averagemz, CEoccurences, CExcorr,
						LCoccurences, LCxcorr, ConfidenceScore);
			}
			result3.close();
			s3.close();
			connection3.close();
		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + selectedPeptide);
			ignore.printStackTrace();
		}
	}

	private void updateConflictHighConfidenceDB(String MusterID,
			double MusterMass, double MusterTime, String OldSeq, String NewSeq,
			String ProtInfo, double Averagemz, int CEoccurences,
			double CExcorr, int LCoccurences, double LCxcorr,
			double ConfidenceScore) throws SQLException {

		int rsSize3 = 0;
		String selectedPeptide = "SELECT * FROM conflict_highconfidencepeptide WHERE Muster_ID = '"
				+ MusterID
				+ "' AND CONVERT (Sequence using latin1) COLLATE Latin1_General_CS ='"
				+ NewSeq + "'";
		Connection connection3 = getConn();
		Statement s3 = connection3.createStatement();
		try {
			ResultSet result3 = s3.executeQuery(selectedPeptide);
			rsSize3 = getResultSetSize(result3);
			if (rsSize3 > 0) {
				updatePeptideC(MusterID, MusterMass, MusterTime, OldSeq,
						NewSeq, ProtInfo, Averagemz, CEoccurences, CExcorr,
						LCoccurences, LCxcorr, ConfidenceScore);
			} else {
				insertPeptideC(MusterID, MusterMass, MusterTime, OldSeq,
						NewSeq, ProtInfo, Averagemz, CEoccurences, CExcorr,
						LCoccurences, LCxcorr, ConfidenceScore);
			}
			result3.close();
			s3.close();
			connection3.close();
		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + selectedPeptide);
			ignore.printStackTrace();
		}
	}

	private void updatePeptideS(String MusterID, double MusterMass,
			double MusterTime, String OldSeq, String NewSeq, String ProtInfo,
			double Averagemz, int CEoccurences, double CExcorr,
			int LCoccurences, double LCxcorr, double ConfidenceScore)
			throws SQLException {
		String updatePeptide = "UPDATE LCMSMSDatabase.highconfidencepeptide SET CE_Occurrences = "
				+ CEoccurences
				+ " , CE_Xcorr = "
				+ CExcorr
				+ " , LC_Occurrences = "
				+ LCoccurences
				+ " , LC_Xcorr = "
				+ LCxcorr
				+ " , Average_mz = "
				+ Averagemz
				+ " , Confidence_Score = "
				+ ConfidenceScore
				+ " WHERE Muster_ID ='" + MusterID + "'";
		Connection connection4 = getConn();
		Statement s4 = connection4.createStatement();
		try {
			int result4 = s4.executeUpdate(updatePeptide);
			s4.close();
			connection4.close();
		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + updatePeptide);
			ignore.printStackTrace();
		}
	}

	private void insertPeptideS(String MusterID, double MusterMass,
			double MusterTime, String OldSeq, String NewSeq, String ProtInfo,
			double Averagemz, int CEoccurences, double CExcorr,
			int LCoccurences, double LCxcorr, double ConfidenceScore)
			throws SQLException {
		String insertpeptide = "INSERT INTO highconfidencepeptide VALUES('"
				+ MusterID + "', " + MusterMass + ", " + MusterTime + ", '"
				+ OldSeq + "', '" + NewSeq + "', '" + ProtInfo + "', "
				+ Averagemz + "," + CEoccurences + "," + CExcorr + ", "
				+ LCoccurences + ", " + LCxcorr + ", " + ConfidenceScore + ")";
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

	private void updatePeptideC(String MusterID, double MusterMass,
			double MusterTime, String OldSeq, String NewSeq, String ProtInfo,
			double Averagemz, int CEoccurences, double CExcorr,
			int LCoccurences, double LCxcorr, double ConfidenceScore)
			throws SQLException {
		String updatePeptide = "UPDATE LCMSMSDatabase.conflict_highconfidencepeptide SET CE_Occurrences = "
				+ CEoccurences
				+ " , CE_Xcorr = "
				+ CExcorr
				+ " , LC_Occurrences = "
				+ LCoccurences
				+ " , LC_Xcorr = "
				+ LCxcorr
				+ " , Average_mz = "
				+ Averagemz
				+ " , Confidence_Score = "
				+ ConfidenceScore
				+ " WHERE Muster_ID ='"
				+ MusterID
				+ "' AND CONVERT (Sequence using latin1) COLLATE Latin1_General_CS ='"
				+ NewSeq + "'";
		Connection connection4 = getConn();
		Statement s4 = connection4.createStatement();
		try {
			int result4 = s4.executeUpdate(updatePeptide);
			s4.close();
			connection4.close();
		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + updatePeptide);
			ignore.printStackTrace();
		}
	}

	private void insertPeptideC(String MusterID, double MusterMass,
			double MusterTime, String OldSeq, String NewSeq, String ProtInfo,
			double Averagemz, int CEoccurences, double CExcorr,
			int LCoccurences, double LCxcorr, double ConfidenceScore)
			throws SQLException {
		String insertpeptide = "INSERT INTO conflict_highconfidencepeptide VALUES('"
				+ MusterID
				+ "', "
				+ MusterMass
				+ ", "
				+ MusterTime
				+ ", '"
				+ OldSeq
				+ "', '"
				+ NewSeq
				+ "', '"
				+ ProtInfo
				+ "', "
				+ Averagemz
				+ ","
				+ CEoccurences
				+ ","
				+ CExcorr
				+ ", "
				+ LCoccurences + ", " + LCxcorr + ", " + ConfidenceScore + ")";
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

	private boolean findConflictHigh(Iterator iterator2) {
		while (iterator2.hasNext()) {
			String values2 = iterator2.next().toString();
			String splitHashMap2[] = values2.split("\\], \\[");
			String conf = splitHashMap2[0];
			conf = conf.replaceAll("\\[", "");
			if (conf.equals("Conflict (High)")) {
				return true;
			}
		}
		return false;
	}

	private boolean findHigh(Iterator iterator2) {
		while (iterator2.hasNext()) {
			String values2 = iterator2.next().toString();
			String splitHashMap2[] = values2.split("\\], \\[");
			String conf = splitHashMap2[0];
			conf = conf.replaceAll("\\[", "");
			if (conf.equals("High")) {
				return true;
			}
		}
		return false;
	}

	public static void main(String[] args) throws IOException, SQLException {
		// TODO code application logic here
		Main_Identifypeptides_mixed Main_Identifypeptides_mixed = new Main_Identifypeptides_mixed();
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

}
