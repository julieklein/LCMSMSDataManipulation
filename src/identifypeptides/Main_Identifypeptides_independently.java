package identifypeptides;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tools.ant.types.CommandlineJava.SysProperties;

import applyconfidence.Output;

public class Main_Identifypeptides_independently {

	public Main_Identifypeptides_independently() throws SQLException {

		int rsSize = 0;
		String queryMuster = "SELECT * FROM MusterList40";
		Connection connection = getConn();
		Statement s = connection.createStatement();
		try {
			ResultSet result = s.executeQuery(queryMuster);
			rsSize = getResultSetSize(result);

			while (result.next()) {
				String musterId = Integer.toString(result.getInt("idMuster"));
				double musterMass = result.getDouble("ExperimentalMass_Da");
				double musterCE = result.getDouble("CE_t_min");
				String musterOldSequence = result.getString("Old_Sequence");

				double ppm = 0;
				if (musterMass <= 800) {
					ppm = 50;
				} else if (musterMass >= 1500) {
					ppm = 75;
				} else {
					ppm = 0.0018 * musterMass + 48.592;
				}
				double minmass = musterMass - musterMass * ppm / 1000000;
				double maxmass = musterMass + musterMass * ppm / 1000000;

				double delta = 0;
				if (musterCE <= 19) {
					delta = 1;
				} else if (musterCE >= 50) {
					delta = 2.5;
				} else {
					delta = 0.0323 * musterCE + 0.3871;
				}
				double minrt = musterCE - delta;
				double maxrt = musterCE + delta;

				int rsSize2 = 0;

				String queryPeptide = "SELECT * FROM CEMSMSCOMPLETEDATA WHERE (ConfTotal > 1.8 OR ConfTotal = 1.8) AND Calibrated_CE_t_min NOT LIKE 0  AND Rank = 1.0"
						// +
						// " AND NOT Modifications LIKE '%N1(Deamidated)%' AND NOT Modifications LIKE '%Q1(Deamidated)%'"
						+ " AND ((TheoriticalMass_Da > "
						+ minmass
						+ " OR TheoriticalMass_Da ="
						+ minmass
						+ ") AND (TheoriticalMass_Da < "
						+ maxmass
						+ " OR TheoriticalMass_Da ="
						+ maxmass
						+ ")) "
						+ "AND ((Calibrated_CE_t_min >"
						+ minrt
						+ " OR Calibrated_CE_t_min ="
						+ minrt
						+ ") AND (Calibrated_CE_t_min <"
						+ maxrt
						+ " OR Calibrated_CE_t_min = " + maxrt + "))";
				Connection connection2 = getConn();
				Statement s2 = connection2.createStatement();
				try {
					ResultSet result2 = s2.executeQuery(queryPeptide);
					rsSize2 = getResultSetSize(result2);
					if (rsSize2 == 0) {
						// TODO PAS DE SEQUENCE POUR CE PEPTIDEID
					} else {
						System.out.println(musterId + " found");
						Map<String, List<Set<String>>> peptideselectionHigh = new LinkedHashMap<String, List<Set<String>>>();
						while (result2.next()) {
							double rank = result2.getDouble("Rank");
							String sequence = result2.getString("Sequence");
							String accession = result2
									.getString("Protein_Accession");
							String symbol = result2.getString("Protein_Symbol");
							String name = result2.getString("Protein_Name");
							String modifications = result2
									.getString("Modifications");
							double theomass = result2
									.getDouble("TheoriticalMass_Da");
							double expmass = result2
									.getDouble("ExperimentalMass_H_Da");
							double mz = result2.getDouble("m_z_Da");
							int basic = result2.getInt("Nb_Basic_Aa");
							double rt = result2
									.getDouble("Calibrated_CE_t_min");
							String spectrum = result2
									.getString("Spectrum_File");
							double scan = result2.getDouble("First_Scan");
							double Xcorr = result2.getDouble("Xcorr");

							int nbP = 0;
							int nbN = 0;
							int nbQ = 0;
							int nbM = 0;
							String splitModif[] = sequence.split("");
							for (String string : splitModif) {
								if (string.contains("p")) {
									nbP++;
								}
							}
							for (String string : splitModif) {
								if (string.contains("q")) {
									nbQ++;
								}
							}
							for (String string : splitModif) {
								if (string.contains("n")) {
									nbN++;
								}
							}
							for (String string : splitModif) {
								if (string.contains("m")) {
									nbM++;
								}
							}

							int deamid = nbN + nbQ;
							int ox = nbM + nbP;
							
							String sequenceUP = sequence.toUpperCase();
							String key = musterId + ", " + sequenceUP + ", "
									+ Integer.toString(ox) + ", "
									+ Integer.toString(deamid);
							if (!peptideselectionHigh.containsKey(key)) {
								List value = new ArrayList<Set<String>>();
								for (int j = 0; j < 17; j++) {
									value.add(new HashSet<String>());
								}
								peptideselectionHigh.put(key, value);
							}

							peptideselectionHigh.get(key).get(0).add(musterId);
							peptideselectionHigh.get(key).get(1)
									.add(Double.toString(musterMass));
							peptideselectionHigh.get(key).get(2)
									.add(Double.toString(musterCE));
							peptideselectionHigh.get(key).get(3)
									.add(musterOldSequence);
							peptideselectionHigh.get(key).get(4).add(sequence);
							peptideselectionHigh.get(key).get(5).add(accession);
							peptideselectionHigh.get(key).get(6).add(symbol);
							peptideselectionHigh.get(key).get(7).add(name);
							peptideselectionHigh.get(key).get(8)
									.add(modifications + ";");
							peptideselectionHigh.get(key).get(9)
									.add(Double.toString(theomass));
							peptideselectionHigh.get(key).get(10)
									.add(Double.toString(expmass));
							peptideselectionHigh.get(key).get(11)
									.add(Double.toString(mz));
							peptideselectionHigh.get(key).get(12)
									.add(Integer.toString(basic));
							peptideselectionHigh.get(key).get(13)
									.add(Double.toString(rt));
							peptideselectionHigh.get(key).get(14).add(spectrum);
							peptideselectionHigh
									.get(key)
									.get(15)
									.add(spectrum + Double.toString(scan) + ";");
							peptideselectionHigh.get(key).get(16)
									.add(Double.toString(Xcorr));
						}

						Map<String, List<Set<String>>> nbconflict = new LinkedHashMap<String, List<Set<String>>>();
						Iterator iterator2 = peptideselectionHigh.values()
								.iterator();
						while (iterator2.hasNext()) {
							String values = iterator2.next().toString();
							String splitHashMap[] = values.split("\\], \\[");
							String musterID = splitHashMap[0];
							musterID = musterID.replaceAll("\\[", "");
							String key = musterID;
							int i = 0;
							if (!nbconflict.containsKey(key)) {
								List value = new ArrayList<Set<String>>();
								for (int j = 0; j < 1; j++) {
									value.add(new HashSet<String>());
									i++;
								}
								nbconflict.put(key, value);
							}
							nbconflict.get(key).get(0).add(Integer.toString(i));
						}

						Iterator iterator3 = nbconflict.values().iterator();
						String values2 = iterator3.next().toString();
						String splitHashMap2[] = values2.split("\\], \\[");
						String sConflict = splitHashMap2[0];
						sConflict = sConflict.replaceAll("\\[", "");
						sConflict = sConflict.replaceAll("\\]", "");
						int j = 0;
						String splitconflict[] = sConflict.split(",");
						for (String string : splitconflict) {
							j++;
						}

						String confidence = j == 1 ? "High" : "High (Conflict)";

						Iterator iterator = peptideselectionHigh.values()
								.iterator();
						while (iterator.hasNext()) {
							String values = iterator.next().toString();
							String splitHashMap[] = values.split("\\], \\[");

							String allsequence = splitHashMap[4];
							allsequence = allsequence.replaceAll("\\[", "");
							String splitsequence[] = allsequence.split(",");
							String sequence = splitsequence[0];

							String accession = splitHashMap[5];
							String symbol = splitHashMap[6];
							String name = splitHashMap[7];
							String list = accession + "; " + symbol;

							String allmodif = splitHashMap[8];
							String modifications = "";

							if (allmodif.contains(";")) {
								String splitmodif[] = allmodif.split("; ");
								modifications = splitmodif[0];
							} else {
								modifications = allmodif;
							}

							double theomass = Double
									.parseDouble(splitHashMap[9]);

							String Allexpmass = splitHashMap[10];
							double Averageexpmass = 0;
							int nbMass = 0;
							String splitAllmass[] = Allexpmass.split(",");
							for (String string : splitAllmass) {
								Averageexpmass = Averageexpmass
										+ Double.parseDouble(string);
								nbMass++;
							}
							Averageexpmass = Averageexpmass / nbMass;

							String Allmz = splitHashMap[11];
							double Averagemz = 0;
							int nbmz = 0;
							String splitAllmz[] = Allmz.split(",");
							for (String string : splitAllmz) {
								Averagemz = Averagemz
										+ Double.parseDouble(string);
								nbmz++;
							}
							Averagemz = Averagemz / nbmz;

							int basic = Integer.parseInt(splitHashMap[12]);

							String AllRT = splitHashMap[13];
							double AverageRT = 0;
							int nbRT = 0;
							String splitAll[] = AllRT.split(",");
							for (String string : splitAll) {
								AverageRT = AverageRT
										+ Double.parseDouble(string);
								nbRT++;
							}
							AverageRT = AverageRT / nbRT;

							String sOccurence = splitHashMap[14];
							String splitOcc[] = sOccurence.split(",");
							int nbOccurence = splitOcc.length;

							String AllXcorr = splitHashMap[16];
							AllXcorr = AllXcorr.replaceAll("\\]", "");
							double AverageX = 0;
							int nbX = 0;
							String splitAllX[] = AllXcorr.split(",");
							for (String string : splitAllX) {
								AverageX = AverageX
										+ Double.parseDouble(string);
								nbX++;
							}
							AverageX = AverageX / nbX;

							if (confidence.equals("High")) {
								System.out.println("High");

								updateSelectedDB(musterId, musterMass,
										musterCE, musterOldSequence, sequence,
										list, theomass, basic, AverageRT,
										nbOccurence, confidence, AverageX,
										Averageexpmass, Averagemz);
							} else {
								System.out.println("High (Conflict)");

								updateConflictDB(musterId, musterMass,
										musterCE, musterOldSequence, sequence,
										list, theomass, basic, AverageRT,
										nbOccurence, confidence, AverageX,
										Averageexpmass, Averagemz);
							}
						}
					}
					result2.close();
					s2.close();
					connection2.close();
				} catch (Throwable ignore) {
					System.err
							.println("Mysql Statement Error: " + queryPeptide);
					ignore.printStackTrace();
				}
			}
			result.close();
			s.close();
			connection.close();
		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + queryMuster);
			ignore.printStackTrace();
		}

	}

	private void updateSelectedDB(String musterID, double musterMass,
			double musterCE, String musterOldSequence, String allsequence,
			String list, double theomass, int basic, double AverageRT,
			int nbOccurence, String confidence, double AverageX,
			double Averageexpmass, double Averagemz) throws SQLException {

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date today = new Date();
		String todaydate = dateFormat.format(today);

		int rsSize3 = 0;
		String selectedPeptide = "SELECT * FROM CEMSMSSELECTED WHERE Muster_ID = '"
				+ musterID + "'";
		Connection connection3 = getConn();
		Statement s3 = connection3.createStatement();
		try {
			ResultSet result3 = s3.executeQuery(selectedPeptide);
			rsSize3 = getResultSetSize(result3);
			if (rsSize3 > 0) {
				while (result3.next()){
					int Occurrences = result3.getInt("Occurrences");
					if(nbOccurence != Occurrences) {
				String Status = "updated_" + todaydate;
				System.out.println(Status);
				updatePeptideS(musterID, musterMass, musterCE,
						musterOldSequence, allsequence, list, theomass, basic,
						AverageRT, nbOccurence, confidence, AverageX,
						Averageexpmass, Averagemz, Status);
					}
				}
			} else {
				String Status = "new_" + todaydate;
				System.out.println(Status);
				insertPeptideS(musterID, musterMass, musterCE,
						musterOldSequence, allsequence, list, theomass, basic,
						AverageRT, nbOccurence, confidence, AverageX,
						Averageexpmass, Averagemz, Status);
			}
			result3.close();
			s3.close();
			connection3.close();
		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + selectedPeptide);
			ignore.printStackTrace();
		}
	}

	private void updateConflictDB(String musterID, double musterMass,
			double musterCE, String musterOldSequence, String allsequence,
			String list, double theomass, int basic, double AverageRT,
			int nbOccurence, String confidence, double AverageX,
			double Averageexpmass, double Averagemz) throws SQLException {

		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date today = new Date();
		String todaydate = dateFormat.format(today);

		int rsSize2 = 0;
		String checkifinselected = "SELECT * FROM CEMSMSSELECTED WHERE Muster_ID = '"
				+ musterID + "'";
		Connection connection2 = getConn();
		Statement s2 = connection2.createStatement();
		try {
			ResultSet result2 = s2.executeQuery(checkifinselected);
			rsSize2 = getResultSetSize(result2);
			if (rsSize2 > 0) {
				String updatePeptide = "UPDATE LCMSMSDatabase.CEMSMSSELECTED SET Status ='deleted' WHERE Muster_ID ='"
						+ musterID + "'";
				Connection connection4 = getConn();
				Statement s4 = connection4.createStatement();
				try {
					int result4 = s4.executeUpdate(updatePeptide);
					s4.close();
					connection4.close();
				} catch (Throwable ignore) {
					System.err.println("Mysql Statement Error: "
							+ updatePeptide);
					ignore.printStackTrace();
				}
			}
			result2.close();
			s2.close();
			connection2.close();
		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + checkifinselected);
			ignore.printStackTrace();
		}

		int rsSize3 = 0;
		String selectedPeptide = "SELECT * FROM CEMSMSCONFLICT WHERE Muster_ID = '"
				+ musterID + "' AND Sequence ='" + allsequence + "'";
		System.out.println(selectedPeptide);
		Connection connection3 = getConn();
		Statement s3 = connection3.createStatement();
		try {
			ResultSet result3 = s3.executeQuery(selectedPeptide);
			rsSize3 = getResultSetSize(result3);
			System.out.println(rsSize3);
			if (rsSize3 > 0) {
				while (result3.next()) {
					String sequence = result3.getString("Sequence");
					int Occurrences = result3.getInt("Occurrences");
					int nbP = 0;
					int nbN = 0;
					int nbQ = 0;
					int nbM = 0;
					String splitModif[] = sequence.split("");
					for (String string : splitModif) {
						if (string.contains("p")) {
							nbP++;
						}
					}
					for (String string : splitModif) {
						if (string.contains("q")) {
							nbQ++;
						}
					}
					for (String string : splitModif) {
						if (string.contains("n")) {
							nbN++;
						}
					}
					for (String string : splitModif) {
						if (string.contains("m")) {
							nbM++;
						}
					}
					int deamid = nbQ + nbN;
					int ox = nbM + nbP;

					int nbP2 = 0;
					int nbN2 = 0;
					int nbQ2 = 0;
					int nbM2 = 0;
					String splitModif2[] = allsequence.split("");
					for (String string : splitModif2) {
						if (string.contains("p")) {
							nbP2++;
						}
					}
					for (String string : splitModif2) {
						if (string.contains("q")) {
							nbQ2++;
						}
					}
					for (String string : splitModif2) {
						if (string.contains("n")) {
							nbN2++;
						}
					}
					for (String string : splitModif2) {
						if (string.contains("m")) {
							nbM2++;
						}
					}
					int deamid2 = nbQ2 + nbN2;
					int ox2 = nbP2 + nbM2;
					if (deamid == deamid2 && ox == ox2) {
						if (nbOccurence != Occurrences) {
						String Status = "updated_" + todaydate;
						System.out.println(Status);
						updatePeptideC(musterID, musterMass, musterCE,
								musterOldSequence, allsequence, list, theomass,
								basic, AverageRT, nbOccurence, confidence,
								AverageX, Averageexpmass, Averagemz, Status);
						}
					} else {
						String Status = "new_" + todaydate;
						System.out.println(Status);
						insertPeptideC(musterID, musterMass, musterCE,
								musterOldSequence, allsequence, list, theomass,
								basic, AverageRT, nbOccurence, confidence,
								AverageX, Averageexpmass, Averagemz, Status);
					}
				}
			} else {
				String Status = "new_" + todaydate;
				System.out.println(Status);
				insertPeptideC(musterID, musterMass, musterCE,
						musterOldSequence, allsequence, list, theomass, basic,
						AverageRT, nbOccurence, confidence, AverageX,
						Averageexpmass, Averagemz, Status);
			}
			result3.close();
			s3.close();
			connection3.close();
		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + selectedPeptide);
			ignore.printStackTrace();
		}
	}

	private void updatePeptideS(String musterID, double musterMass,
			double musterCE, String musterOldSequence, String allsequence,
			String list, double theomass, int basic, double AverageRT,
			int nbOccurence, String confidence, double AverageX,
			double Averageexpmass, double Averagemz, String Status)
			throws SQLException {
		String updatePeptide = "UPDATE LCMSMSDatabase.CEMSMSSELECTED SET Occurrences ="
				+ nbOccurence
				+ ", Confidence ='"
				+ confidence
				+ "', Sequence ='"
				+ allsequence
				+ "', Protein_Info ='"
				+ list
				+ "', TheoriticalMass_Da = "
				+ theomass
				+ ", Average_ExperimentalMass_H_Da = "
				+ Averageexpmass
				+ ", Average_m_z_Da = "
				+ Averagemz
				+ ", Average_Xcorr = "
				+ AverageX
				+ ", Average_Calibrated_CE_t_min = "
				+ AverageRT
				+ " , Muster_ID = '"
				+ musterID
				+ "', Muster_Exp_Mass ="
				+ musterMass
				+ ", Muster_CE_t="
				+ musterCE
				+ ", Muster_Old_Sequence ='"
				+ musterOldSequence
				+ "', Status ='"
				+ Status
				+ "' WHERE Muster_ID ='"
				+ musterID
				+ "'";
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

	private void insertPeptideS(String musterID, double musterMass,
			double musterCE, String musterOldSequence, String allsequence,
			String list, double theomass, int basic, double AverageRT,
			int nbOccurence, String confidence, double AverageX,
			double Averageexpmass, double Averagemz, String Status)
			throws SQLException {
		String insertpeptide = "INSERT INTO CEMSMSSELECTED VALUES('" + musterID
				+ "', " + musterMass + ", " + musterCE + ", '"
				+ musterOldSequence + "', '" + allsequence + "', '" + list
				+ "', " + theomass + "," + Averageexpmass + "," + Averagemz
				+ ", " + AverageX + ", " + basic + ", " + AverageRT + " , "
				+ nbOccurence + ", '" + confidence + "', 'Yes', '" + Status
				+ "')";
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

	private void updatePeptideC(String musterID, double musterMass,
			double musterCE, String musterOldSequence, String allsequence,
			String list, double theomass, int basic, double AverageRT,
			int nbOccurence, String confidence, double AverageX,
			double Averageexpmass, double Averagemz, String Status)
			throws SQLException {
		String updatePeptide = "UPDATE LCMSMSDatabase.CEMSMSCONFLICT SET Occurrences = "
				+ nbOccurence
				+ ", Confidence ='"
				+ confidence
				+ "', Sequence ='"
				+ allsequence
				+ "', Protein_Info ='"
				+ list
				+ "', TheoriticalMass_Da = "
				+ theomass
				+ ", Average_ExperimentalMass_H_Da = "
				+ Averageexpmass
				+ ", Average_m_z_Da = "
				+ Averagemz
				+ ", Average_Xcorr = "
				+ AverageX
				+ ", Average_Calibrated_CE_t_min = "
				+ AverageRT
				+ " , Muster_ID = '"
				+ musterID
				+ "', Muster_Exp_Mass ="
				+ musterMass
				+ ", Muster_CE_t="
				+ musterCE
				+ ", Muster_Old_Sequence ='"
				+ musterOldSequence
				+ "' , Status = '"
				+ Status
				+ "' WHERE Muster_ID ='"
				+ musterID
				+ "' AND Sequence ='"
				+ allsequence + "'";
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

	private void insertPeptideC(String musterID, double musterMass,
			double musterCE, String musterOldSequence, String allsequence,
			String list, double theomass, int basic, double AverageRT,
			int nbOccurence, String confidence, double AverageX,
			double Averageexpmass, double Averagemz, String Status)
			throws SQLException {
		String insertpeptide = "INSERT INTO CEMSMSCONFLICT VALUES('" + musterID
				+ "', " + musterMass + ", " + musterCE + ", '"
				+ musterOldSequence + "', '" + allsequence + "', '" + list
				+ "', " + theomass + "," + Averageexpmass + "," + Averagemz
				+ ", " + AverageX + ", " + basic + ", " + AverageRT + " , "
				+ nbOccurence + ", '" + confidence + "', '', '" + Status + "')";
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
			if (conf.equals("High (Conflict)")) {
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
		Main_Identifypeptides_independently Main_Identifypeptides_independently = new Main_Identifypeptides_independently();
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
