package identifypeptides;

import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.tools.ant.types.CommandlineJava.SysProperties;

import applyconfidence_3.Output;

public class CopyOfMain_Identifypeptides {
	public CopyOfMain_Identifypeptides() throws SQLException {

		int rsSize = 0;
		String queryMuster = "SELECT * FROM MusterList40";
		Connection connection = getConn();
		Statement s = connection.createStatement();
		try {
			ResultSet result = s.executeQuery(queryMuster);
			rsSize = getResultSetSize(result);
			
			while (result.next()) {
				Map<String, List<Set<String>>> peptideselectionHigh = new LinkedHashMap<String, List<Set<String>>>();
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
					delta = 0.0484 * musterCE + 0.0806;
				}
				double minrt = musterCE - delta;
				double maxrt = musterCE + delta;

				int rsSize2 = 0;
				String queryPeptide = "SELECT * FROM CEMSMSCOMPLETEDATA WHERE (ConfTotal > 1.8 OR ConfTotal = 1.8) AND Rank = 1.0"
						+ " AND ((TheoriticalMass_Da > "
						+ minmass
						+ "OR TheoriticalMass_Da ="
						+ minmass
						+ ") AND (TheoriticalMass_Da < "
						+ maxmass
						+ "OR TheoriticalMass_Da ="
						+ maxmass
						+ ")) "
						+ "AND ((Calibrated_CE_t_min >"
						+ minrt
						+ "OR Calibrated_CE_t_min ="
						+ minrt
						+ ") AND (Calibrated_CE_t_min <"
						+ maxrt
						+ "OR Calibrated_CE_t_min = " + maxrt + "))";
				Connection connection2 = getConn();
				Statement s2 = connection2.createStatement();
				try {
					ResultSet result2 = s2.executeQuery(queryPeptide);
					rsSize2 = getResultSetSize(result2);
					if (rsSize2 == 0) {
						// TODO PAS DE SEQUENCE POUR CE PEPTIDEID
					} else {
						System.out.println(musterId + " found");
						
						String key = musterId;
						if (!peptideselectionHigh.containsKey(key)) {
							List value = new ArrayList<Set<String>>();
							for (int j = 0; j < 13; j++) {
								value.add(new HashSet<String>());
							}
							peptideselectionHigh.put(key, value);
						}
						
						peptideselectionHigh.get(key).get(0).add(musterId);
						peptideselectionHigh.get(key).get(1).add(Double.toString(musterMass));
						peptideselectionHigh.get(key).get(2).add(Double.toString(musterCE));
						peptideselectionHigh.get(key).get(3).add(musterOldSequence);
						
						Map<String, List<Set<String>>> allepeptides = new LinkedHashMap<String, List<Set<String>>>();
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
							double rt = result2.getDouble("Calibrated_CE_t_min");
							String spectrum = result2.getString("Spectrum_File");
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
							String sequenceUP = sequence.toUpperCase();
							String key2 = sequenceUP + ", "
									+ Integer.toString(nbP) + ", "
									+ Integer.toString(nbQ) + ", "
									+ Integer.toString(nbN) + ", "
									+ Integer.toString(nbM);
							if (!allepeptides.containsKey(key2)) {
								List value2 = new ArrayList<Set<String>>();
								for (int j = 0; j < 13; j++) {
									value2.add(new HashSet<String>());
								}
								allepeptides.put(key2, value2);
							}
							
							allepeptides.get(key2).get(0).add(sequence);
							allepeptides.get(key2).get(1).add(accession);
							allepeptides.get(key2).get(2).add(symbol);
							allepeptides.get(key2).get(3).add(name);
							allepeptides.get(key2).get(4).add(modifications + ";");
							allepeptides.get(key2).get(5)
									.add(Double.toString(theomass));
							allepeptides.get(key2).get(6)
									.add(Double.toString(expmass));
							allepeptides.get(key2).get(7)
									.add(Double.toString(mz));
							allepeptides.get(key2).get(8)
									.add(Integer.toString(basic));
							allepeptides.get(key2).get(9)
									.add(Double.toString(rt));
							allepeptides.get(key2).get(10).add(spectrum);
							allepeptides
									.get(key2)
									.get(11)
									.add(spectrum + Double.toString(scan) + ";");
							allepeptides.get(key2).get(12)
									.add(Double.toString(Xcorr));
						}
						Iterator iterator = allepeptides.values().iterator();
						while (iterator.hasNext()) {
							String values = iterator.next().toString();
							String splitHashMap[] = values.split("\\], \\[");

							String allsequence = splitHashMap[0];
							allsequence = allsequence.replaceAll("\\[", "");
							String splitsequence[] = allsequence.split(",");
							String sequence = splitsequence[0];

							String accession = splitHashMap[1];
							String symbol = splitHashMap[2];
							String name = splitHashMap[3];

							String allmodif = splitHashMap[4];
							String modifications = "";
							
							if (allmodif.contains(";")) {
							String splitmodif[] = allmodif.split("; ");
							modifications = splitmodif[0];
							} else {
								modifications = allmodif;
							}
							
							double theomass = Double.parseDouble(splitHashMap[5]);
							
							String Allexpmass = splitHashMap[6];
							double Averageexpmass = 0;
							int nbMass = 0;
							String splitAllmass[] = Allexpmass.split(",");
							for (String string : splitAllmass) {
								Averageexpmass = Averageexpmass + Double.parseDouble(string);
								nbMass++;
							}
							Averageexpmass = Averageexpmass / nbMass;

							String Allmz = splitHashMap[7];
							double Averagemz = 0;
							int nbmz = 0;
							String splitAllmz[] = Allmz.split(",");
							for (String string : splitAllmz) {
								Averagemz = Averagemz + Double.parseDouble(string);
								nbmz++;
							}
							Averagemz = Averagemz / nbmz;

							int basic = Integer.parseInt(splitHashMap[8]);

							String AllRT = splitHashMap[9];
							double AverageRT = 0;
							int nbRT = 0;
							String splitAll[] = AllRT.split(",");
							for (String string : splitAll) {
								AverageRT = AverageRT + Double.parseDouble(string);
								nbRT++;
							}
							AverageRT = AverageRT / nbRT;

							String sOccurence = splitHashMap[10];
							String splitOcc[] = sOccurence.split(",");
							int nbOccurence = splitOcc.length;


							String AllXcorr = splitHashMap[12];
							AllXcorr = AllXcorr.replaceAll("\\]", "");
							double AverageX = 0;
							int nbX = 0;
							String splitAllX[] = AllXcorr.split(",");
							for (String string : splitAllX) {
								AverageX = AverageX + Double.parseDouble(string);
								nbX++;
							}
							AverageX = AverageX / nbX;

							
							peptideselectionHigh.get(key).get(4).add(sequence);
							peptideselectionHigh.get(key).get(5).add(sequence + " (" + accession + "; " + symbol + ")");
							peptideselectionHigh.get(key).get(6)
									.add(Double.toString(theomass));
							peptideselectionHigh.get(key).get(7)
									.add(Double.toString(Averageexpmass));
							peptideselectionHigh.get(key).get(8)
									.add(Double.toString(Averagemz));
							peptideselectionHigh.get(key).get(9)
									.add(Integer.toString(basic));
							peptideselectionHigh.get(key).get(10)
									.add(Double.toString(AverageRT));
							peptideselectionHigh.get(key).get(11).add(Integer.toString(nbOccurence));
							peptideselectionHigh.get(key).get(12)
									.add(Double.toString(AverageX));
						}
						
						Iterator iterator2 = peptideselectionHigh.values().iterator();
						String confidence = "";
						String values = iterator2.next().toString();
						String splitHashMap[] = values.split("\\], \\[");
						
						String musterID = splitHashMap[0];
						musterID = musterID.replaceAll("\\[", "");
		
						double musterMass2 = Double.parseDouble(splitHashMap[1]);
						double musterCE2 = Double.parseDouble(splitHashMap[2]);
						String musterOldSequence2 = splitHashMap[3];
						
						String allsequence = splitHashMap[4];
						String splitsequence[] = allsequence.split(",");
						int nbSequence  = splitsequence.length;
						if (nbSequence == 1) {
							System.out.println("High");
							confidence = "High";
							String list = splitHashMap[5];
							list = list.replace(allsequence + "; ", "");
							double theomass = Double.parseDouble(splitHashMap[6]);
							double Averageexpmass = Double.parseDouble(splitHashMap[7]);
							double Averagemz = Double.parseDouble(splitHashMap[8]);
							int basic = Integer.parseInt(splitHashMap[9]);
							double AverageRT = Double.parseDouble(splitHashMap[10]);
							int nbOccurence = Integer.parseInt(splitHashMap[11]);
							String sAverageX = splitHashMap[12];
							sAverageX = sAverageX.replaceAll("\\]", "");
							double AverageX = Double.parseDouble(sAverageX);
							
							updateSelectedDB(musterID, musterMass2, musterCE2, musterOldSequence2, allsequence, list,
									theomass, basic, AverageRT,
									nbOccurence, confidence,
									AverageX, Averageexpmass,
									Averagemz);
						} else {
							System.out.println("Conflict (High)");
							confidence = "Conflict (High)";
							String list = splitHashMap[5];
							String theomass = splitHashMap[6];
							String Averageexpmass = splitHashMap[7];
							String Averagemz = splitHashMap[8];
							String basic = splitHashMap[9];
							String AverageRT = splitHashMap[10];
							String nbOccurence = splitHashMap[11];
							String AverageX = splitHashMap[12];
							AverageX = AverageX.replaceAll("\\]", "");
							
							updateConflictDB(musterID, musterMass2, musterCE2, musterOldSequence2, allsequence, list,
									theomass, basic, AverageRT,
									nbOccurence, confidence,
									AverageX, Averageexpmass,
									Averagemz);
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
			
//			Iterator iterator = peptideselectionHigh.values().iterator();
//			while (iterator.hasNext()) {
//				String confidence = "";
//				String values = iterator.next().toString();
//				String splitHashMap[] = values.split("\\], \\[");
//				
//				String musterID = splitHashMap[0];
//				musterID = musterID.replaceAll("\\[", "");
//				
//				double musterMass = Double.parseDouble(splitHashMap[1]);
//				double musterCE = Double.parseDouble(splitHashMap[2]);
//				String musterOldSequence = splitHashMap[3];
//				
//				String allsequence = splitHashMap[4];
//				String splitsequence[] = allsequence.split(",");
//				int nbSequence  = splitsequence.length;
//				if (nbSequence == 1) {
//					System.out.println("High");
//					confidence = "High";
//					String list = splitHashMap[5];
//					list = list.replace(allsequence + "; ", "");
//					double theomass = Double.parseDouble(splitHashMap[6]);
//					double Averageexpmass = Double.parseDouble(splitHashMap[7]);
//					double Averagemz = Double.parseDouble(splitHashMap[8]);
//					int basic = Integer.parseInt(splitHashMap[9]);
//					double AverageRT = Double.parseDouble(splitHashMap[10]);
//					int nbOccurence = Integer.parseInt(splitHashMap[11]);
//					double AverageX = Double.parseDouble(splitHashMap[12]);
//					updateSelectedDB(musterID, musterMass, musterCE, musterOldSequence, allsequence, list,
//							theomass, basic, AverageRT,
//							nbOccurence, confidence,
//							AverageX, Averageexpmass,
//							Averagemz);
//				} else {
//					System.out.println("Conflict (High)");
//					confidence = "Conflict (High)";
//					String list = splitHashMap[5];
//					String theomass = splitHashMap[6];
//					String Averageexpmass = splitHashMap[7];
//					String Averagemz = splitHashMap[8];
//					String basic = splitHashMap[9];
//					String AverageRT = splitHashMap[10];
//					String nbOccurence = splitHashMap[11];
//					String AverageX = splitHashMap[12];
//					updateConflictDB(musterID, musterMass, musterCE, musterOldSequence, allsequence, list,
//							theomass, basic, AverageRT,
//							nbOccurence, confidence,
//							AverageX, Averageexpmass,
//							Averagemz);
//				}
//				
//				
//			}
			
			result.close();
			s.close();
			connection.close();
		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + queryMuster);
			ignore.printStackTrace();
		}

//		int rsSize = 0;
//		String queryPeptide = "SELECT * FROM CEMSMSCOMPLETEDATA WHERE ConfTotal > 1";
//		Connection connection = getConn();
//		Statement s = connection.createStatement();
//		Map<String, List<Set<String>>> peptideselectionHigh = new HashMap<String, List<Set<String>>>();
//		// High, Conflict (High), Pending
//		try {
//			ResultSet result = s.executeQuery(queryPeptide);
//			rsSize = getResultSetSize(result);
//
//			while (result.next()) {
//				double rank = result.getDouble("Rank");
//				String sequence = result.getString("Sequence");
//				String accession = result.getString("Protein_Accession");
//				String symbol = result.getString("Protein_Symbol");
//				String name = result.getString("Protein_Name");
//				String modifications = result.getString("Modifications");
//				double theomass = result.getDouble("TheoriticalMass_Da");
//				double expmass = result.getDouble("ExperimentalMass_H_Da");
//				double mz = result.getDouble("m_z_Da");
//				int basic = result.getInt("Nb_Basic_Aa");
//				double rt = result.getDouble("Calibrated_CE_t_min");
//				String spectrum = result.getString("Spectrum_File");
//				double scan = result.getDouble("First_Scan");
//				double Xcorr = result.getDouble("Xcorr");
//
//				if (Xcorr >= 1.8 && rank == 1.0) {
//					String SequenceUpp = sequence.toUpperCase();
//					String key = SequenceUpp + Double.toString(theomass);
//					if (!peptideselectionHigh.containsKey(key)) {
//						List value = new ArrayList<Set<String>>();
//						for (int j = 0; j < 13; j++) {
//							value.add(new HashSet<String>());
//						}
//						peptideselectionHigh.put(key, value);
//					}
//					// if (!modifications.contains("N1(Deamidated)") ||
//					// !modifications.contains("Q1(Deamidated)")) {
//					peptideselectionHigh.get(key).get(0).add(sequence + ";");
//					peptideselectionHigh.get(key).get(1).add(accession);
//					peptideselectionHigh.get(key).get(2).add(symbol);
//					peptideselectionHigh.get(key).get(3).add(name);
//					peptideselectionHigh.get(key).get(4)
//							.add(modifications + ";");
//					peptideselectionHigh.get(key).get(5)
//							.add(Double.toString(theomass));
//					peptideselectionHigh.get(key).get(6)
//							.add(Double.toString(expmass));
//					peptideselectionHigh.get(key).get(7)
//							.add(Double.toString(mz));
//					peptideselectionHigh.get(key).get(8)
//							.add(Integer.toString(basic));
//					peptideselectionHigh.get(key).get(9)
//							.add(Double.toString(rt));
//					peptideselectionHigh.get(key).get(10).add(spectrum);
//					peptideselectionHigh.get(key).get(11)
//							.add(spectrum + Double.toString(scan) + ";");
//					peptideselectionHigh.get(key).get(12)
//							.add(Double.toString(Xcorr));
//					// }
//				}
//			}
//			result.close();
//			s.close();
//			connection.close();
//		} catch (Throwable ignore) {
//			System.err.println("Mysql Statement Error: " + queryPeptide);
//			ignore.printStackTrace();
//		}
//
//		Iterator iterator = peptideselectionHigh.values().iterator();
//		while (iterator.hasNext()) {
//			String values = iterator.next().toString();
//			String splitHashMap[] = values.split("\\], \\[");
//
//			String allsequence = splitHashMap[0];
//			allsequence = allsequence.replaceAll("\\[", "");
//			String splitsequence[] = allsequence.split(";");
//			String sequence = splitsequence[0];
//
//			String accession = splitHashMap[1];
//			String symbol = splitHashMap[2];
//			String name = splitHashMap[3];
//
//			String modifications = "";
//			if (splitHashMap[4].contains(";") && splitHashMap[4].contains("(")) {
//				String allmodifications = splitHashMap[4];
//				String splitmodifications[] = allmodifications.split(";");
//				modifications = splitmodifications[0];
//			} else {
//				modifications = splitHashMap[4];
//				modifications = modifications.replaceAll(";", "");
//			}
//			int nbP = 0;
//			int nbN = 0;
//			int nbQ = 0;
//			int nbM = 0;
//			String splitModif[] = modifications.split(",");
//			for (String string : splitModif) {
//				if (string.contains("P")) {
//					nbP++;
//				}
//			}
//			for (String string : splitModif) {
//				if (string.contains("Q")) {
//					nbQ++;
//				}
//			}
//			for (String string : splitModif) {
//				if (string.contains("N")) {
//					nbN++;
//				}
//			}
//			for (String string : splitModif) {
//				if (string.contains("M")) {
//					nbM++;
//				}
//			}
//			double theomass = Double.parseDouble(splitHashMap[5]);
//			String Allexpmass = splitHashMap[6];
//			double Averageexpmass = 0;
//			int nbMass = 0;
//			String splitAllmass[] = Allexpmass.split(",");
//			for (String string : splitAllmass) {
//				Averageexpmass = Averageexpmass + Double.parseDouble(string);
//				nbMass++;
//			}
//			Averageexpmass = Averageexpmass / nbMass;
//
//			String Allmz = splitHashMap[7];
//			double Averagemz = 0;
//			int nbmz = 0;
//			String splitAllmz[] = Allmz.split(",");
//			for (String string : splitAllmz) {
//				Averagemz = Averagemz + Double.parseDouble(string);
//				nbmz++;
//			}
//			Averagemz = Averagemz / nbmz;
//
//			int basic = Integer.parseInt(splitHashMap[8]);
//
//			String AllRT = splitHashMap[9];
//			double AverageRT = 0;
//			int nbRT = 0;
//			String splitAll[] = AllRT.split(",");
//			for (String string : splitAll) {
//				AverageRT = AverageRT + Double.parseDouble(string);
//				nbRT++;
//			}
//			AverageRT = AverageRT / nbRT;
//
//			double delta = 0;
//			if (AverageRT <= 19) {
//				delta = 1;
//			} else if (AverageRT >= 50) {
//				delta = 2.5;
//			} else {
//				delta = 0.0484 * AverageRT + 0.0806;
//			}
//			double minrt = AverageRT - delta;
//			double maxrt = AverageRT + delta;
//
//			String sOccurence = splitHashMap[10];
//			String splitOcc[] = sOccurence.split(",");
//			int nbOccurence = splitOcc.length;
//
//			double minmass = theomass - theomass * 10 / 1000000;
//			double maxmass = theomass + theomass * 10 / 1000000;
//
//			String Xcorr = splitHashMap[12];
//			Xcorr = Xcorr.replaceAll("\\]", "");
//
//			int rsSize2 = 0;
//			String queryMass = "SELECT * FROM CEMSMSCOMPLETEDATA WHERE ((TheoriticalMass_Da > "
//					+ minmass
//					+ "OR TheoriticalMass_Da ="
//					+ minmass
//					+ ") AND (TheoriticalMass_Da < "
//					+ maxmass
//					+ "OR TheoriticalMass_Da ="
//					+ maxmass
//					+ ")) "
//					+ "AND ((Calibrated_CE_t_min >"
//					+ minrt
//					+ "OR Calibrated_CE_t_min ="
//					+ minrt
//					+ ") AND (Calibrated_CE_t_min <"
//					+ maxrt
//					+ "OR Calibrated_CE_t_min = "
//					+ maxrt
//					+ ")) AND (ConfTotal > 1.8) AND Rank = 1.0";
//			// +" AND NOT (CONVERT (Sequence using latin1) COLLATE latin1_general_cs = '"
//			// + sequence + "')";
//			Connection connection2 = getConn();
//			Statement s2 = connection2.createStatement();
//			String confidence = null;
//			String conflictsequence = "";
//			String conflictsequencelist = "";
//			try {
//				ResultSet result2 = s2.executeQuery(queryMass);
//				rsSize2 = getResultSetSize(result2);
//				if (rsSize2 > 0) {
//					Map<String, List<Set<String>>> conflictpeptidesHigh = new HashMap<String, List<Set<String>>>();
//					while (result2.next()) {
//						conflictsequence = result2.getString("Sequence");
//						String conflictmodifications = result2
//								.getString("Modifications");
//						if (conflictsequence.equalsIgnoreCase(sequence)) {
//							int nbconflictP = 0;
//							int nbconflictN = 0;
//							int nbconflictQ = 0;
//							int nbconflictM = 0;
//							String splitOutmodif[] = conflictmodifications
//									.split(",");
//							for (String string : splitOutmodif) {
//								if (string.contains("P")) {
//									nbconflictP++;
//								}
//							}
//							for (String string : splitOutmodif) {
//								if (string.contains("N")) {
//									nbconflictN++;
//								}
//							}
//							for (String string : splitOutmodif) {
//								if (string.contains("Q")) {
//									nbconflictQ++;
//								}
//							}
//							for (String string : splitOutmodif) {
//								if (string.contains("M")) {
//									nbconflictM++;
//								}
//							}
//							if ((nbP == nbconflictP) && (nbQ == nbconflictQ)
//									&& (nbN == nbconflictN)
//									&& (nbM == nbconflictM)) {
//								confidence = "High";
//							} else {
//								confidence = "Conflict (High)";
//							}
//						} else {
//							confidence = "Conflict (High)";
//						}
//						String key = confidence;
//						if (!conflictpeptidesHigh.containsKey(key)) {
//							List value = new ArrayList<Set<String>>();
//							for (int j = 0; j < 2; j++) {
//								value.add(new HashSet<String>());
//							}
//							conflictpeptidesHigh.put(key, value);
//						}
//						conflictpeptidesHigh.get(key).get(0).add(confidence);
//						conflictpeptidesHigh.get(key).get(1)
//								.add(conflictsequence);
//					}
//					Iterator iterator2 = conflictpeptidesHigh.values()
//							.iterator();
//					boolean conflictHigh = false;
//					boolean high = false;
//					conflictHigh = findConflictHigh(iterator2);
//					high = findHigh(iterator2);
//
//					if (conflictHigh) {
//						Iterator iterator3 = conflictpeptidesHigh.values()
//								.iterator();
//						while (iterator3.hasNext()) {
//							String values3 = iterator3.next().toString();
//							String splitHashMap3[] = values3.split("\\], \\[");
//							String conf = splitHashMap3[0];
//							conf = conf.replaceAll("\\[", "");
//							if (conf.equals("Conflict (High)")) {
//								conflictsequencelist = splitHashMap3[1];
//								conflictsequencelist = conflictsequencelist
//										.replaceAll("\\]", "");
//
//								updateSelectedDB(sequence, accession, symbol,
//										name, modifications, theomass, basic,
//										AllRT, AverageRT, nbOccurence, conf,
//										conflictsequencelist, Xcorr,
//										Averageexpmass, Averagemz);
//
//							}
//						}
//					} else if (high) {
//						updateSelectedDB(sequence, accession, symbol, name,
//								modifications, theomass, basic, AllRT,
//								AverageRT, nbOccurence, confidence,
//								conflictsequencelist, Xcorr, Averageexpmass,
//								Averagemz);
//					} else {
//						updateSelectedDB(sequence, accession, symbol, name,
//								modifications, theomass, basic, AllRT,
//								AverageRT, nbOccurence, confidence,
//								conflictsequencelist, Xcorr, Averageexpmass,
//								Averagemz);
//					}
//
//				} else {
//					confidence = "High";
//					updateSelectedDB(sequence, accession, symbol, name,
//							modifications, theomass, basic, AllRT, AverageRT,
//							nbOccurence, confidence, conflictsequencelist,
//							Xcorr, Averageexpmass, Averagemz);
//				}
//				result2.close();
//				s2.close();
//				connection2.close();
//			} catch (Throwable ignore) {
//				System.err.println("Mysql Statement Error: " + queryMass);
//				ignore.printStackTrace();
//			}
//
//		}

	}

	private void updateSelectedDB(String musterID, double musterMass, double musterCE, String musterOldSequence, String allsequence, String list,
			double theomass,
			int basic,  double AverageRT, int nbOccurence,
			String confidence, double AverageX,
			double Averageexpmass, double Averagemz) throws SQLException {

		int rsSize3 = 0;
		String selectedPeptide = "SELECT * FROM CEMSMSSELECTED WHERE Muster_ID = '"
				+ musterID + "'";
		Connection connection3 = getConn();
		Statement s3 = connection3.createStatement();
		try {
			ResultSet result3 = s3.executeQuery(selectedPeptide);
			rsSize3 = getResultSetSize(result3);
			if (rsSize3 > 0) {
				updatePeptideS(musterID, musterMass, musterCE, musterOldSequence, allsequence, list,
						theomass, basic, AverageRT,
						nbOccurence, confidence,
						AverageX, Averageexpmass,
						Averagemz);
			} else {
				insertPeptideS(musterID, musterMass, musterCE, musterOldSequence, allsequence, list,
						theomass, basic, AverageRT,
						nbOccurence, confidence,
						AverageX, Averageexpmass,
						Averagemz);
			}
			result3.close();
			s3.close();
			connection3.close();
		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + selectedPeptide);
			ignore.printStackTrace();
		}
	}
	
	private void updateConflictDB(String musterID, double musterMass, double musterCE, String musterOldSequence, String allsequence, String list,
			String theomass,
			String basic,  String AverageRT, String nbOccurence,
			String confidence, String AverageX,
			String Averageexpmass, String Averagemz) throws SQLException {

		int rsSize3 = 0;
		String selectedPeptide = "SELECT * FROM CEMSMSSELECTED WHERE Muster_ID = '"
				+ musterID + "'";
		Connection connection3 = getConn();
		Statement s3 = connection3.createStatement();
		try {
			ResultSet result3 = s3.executeQuery(selectedPeptide);
			rsSize3 = getResultSetSize(result3);
			if (rsSize3 > 0) {
				updatePeptideC(musterID, musterMass, musterCE, musterOldSequence, allsequence, list,
						theomass, basic, AverageRT,
						nbOccurence, confidence,
						AverageX, Averageexpmass,
						Averagemz);
			} else {
				insertPeptideC(musterID, musterMass, musterCE, musterOldSequence, allsequence, list,
						theomass, basic, AverageRT,
						nbOccurence, confidence,
						AverageX, Averageexpmass,
						Averagemz);
			}
			result3.close();
			s3.close();
			connection3.close();
		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + selectedPeptide);
			ignore.printStackTrace();
		}
	}

	private void updatePeptideS(String musterID, double musterMass, double musterCE, String musterOldSequence, String allsequence, String list,
			double theomass,
			int basic,  double AverageRT, int nbOccurence,
			String confidence, double AverageX,
			double Averageexpmass, double Averagemz) throws SQLException {
		String updatePeptide = "UPDATE MSMSDatabase.CEMSMSSELECTED SET Occurences ="
				+ nbOccurence
				+ ", Confidence ='"
				+ confidence
				+ "', Sequence ='"
				+ allsequence
				+ "', Protein_Info ='"
				+ list
				+ "', Theoritical_Mass_Da = "
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
				+ "' WHERE CONVERT MusterID ='"
				+ musterID + "'";
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

	private void insertPeptideS(String musterID, double musterMass, double musterCE, String musterOldSequence, String allsequence, String list,
			double theomass,
			int basic,  double AverageRT, int nbOccurence,
			String confidence, double AverageX,
			double Averageexpmass, double Averagemz) throws SQLException {
		String insertpeptide = "INSERT INTO CEMSMSSELECTED VALUES('" + musterID
				+ "', " + musterMass + ", " + musterCE + ", '"
				+ musterOldSequence + "', '" + allsequence + "', '" + list
				+ "', " + theomass + "," + Averageexpmass + "," + Averagemz + ", "
				+ AverageX + ", " + basic + ", " + AverageRT
				+ " , " + nbOccurence + ", '" + confidence
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

	private void updatePeptideC(String musterID, double musterMass, double musterCE, String musterOldSequence, String allsequence, String list,
			String theomass,
			String basic,  String AverageRT, String nbOccurence,
			String confidence, String AverageX,
			String Averageexpmass, String Averagemz) throws SQLException {
		String updatePeptide = "UPDATE MSMSDatabase.CEMSMSCONFLICT SET Occurences ='"
				+ nbOccurence
				+ "', Confidence ='"
				+ confidence
				+ "', Sequence ='"
				+ allsequence
				+ "', Protein_Info ='"
				+ list
				+ "', Theoritical_Mass_Da = '"
				+ theomass
				+ "', Average_ExperimentalMass_H_Da = '"
				+ Averageexpmass
				+ "', Average_m_z_Da = '"
				+ Averagemz
				+ "', Average_Xcorr = '"
				+ AverageX
				+ "', Average_Calibrated_CE_t_min =' "
				+ AverageRT
				+ "' , Muster_ID = '"
				+ musterID
				+ "', Muster_Exp_Mass ="
				+ musterMass
				+ ", Muster_CE_t="
				+ musterCE
				+ ", Muster_Old_Sequence ='"
				+ musterOldSequence
				+ "' WHERE CONVERT MusterID ='"
				+ musterID + "'";
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

	private void insertPeptideC(String musterID, double musterMass, double musterCE, String musterOldSequence, String allsequence, String list,
			String theomass,
			String basic,  String AverageRT, String nbOccurence,
			String confidence, String AverageX,
			String Averageexpmass, String Averagemz) throws SQLException {
		String insertpeptide = "INSERT INTO CEMSMSCONFLICT VALUES('" + musterID
				+ "', " + musterMass + ", " + musterCE + ", '"
				+ musterOldSequence + "', '" + allsequence + "', '" + list
				+ "', '" + theomass + "','" + Averageexpmass + "','" + Averagemz + "', '"
				+ AverageX + "', '" + basic + "', '" + AverageRT
				+ "' , '" + nbOccurence + "', '" + confidence
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
		CopyOfMain_Identifypeptides Main_Identifypeptides = new CopyOfMain_Identifypeptides();
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
