package convertfromomsatodb;

import java.io.BufferedReader;
import java.io.File;
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

import applyconfidence_3.Main_ApplyConfidence;
import applyconfidence_3.Output;

public class Main_fromOmsatoDB {
	public Main_fromOmsatoDB() throws IOException, SQLException {
		File f = new File(
				"//Users/julieklein/Documents/MosaiquesDatabase/LCMSMSdata/textfiles/confidence/fait/extrapolated");
		File[] files = f.listFiles();
		for (File file : files) {
			String filepath = "/" + file.getPath();
			String woextrapol = filepath
					.replace(
							"//Users/julieklein/Documents/MosaiquesDatabase/LCMSMSdata/textfiles/confidence/fait/extrapolated/",
							"//Users/julieklein/Documents/MosaiquesDatabase/LCMSMSdata/textfiles/confidence/fait/");
			woextrapol = woextrapol.replace("_ConfOMSA-20130311-1258.txt",
					"_Confidence.txt");
			String withextrapol = woextrapol.replace("_Confidence.txt",
					"_Extrapo_Conf.txt");

			System.out.println(filepath);
			System.out.println(woextrapol);
			System.out.println(withextrapol);
			
			File withextrafile = new File(withextrapol);
			if (!withextrafile.exists()) {

			if (!filepath.contains("DS_Store") && !filepath.contains("Extrapo") && !filepath.contains("_los")) {
				PrintStream csvWriter = null;
				LinkedList<Output> outputList = new LinkedList<Output>();
				BufferedReader bReader = createBufferedreader(woextrapol);
				String line;
				bReader.readLine();
				while ((line = bReader.readLine()) != null) {
					String splitarray2[] = line.split("\t");
					double extrapolRT = 0;
					Output output = new Output();
					String filename2 = splitarray2[22];
					String sequence2 = splitarray2[5];
					double Xcorr2 = Double.parseDouble(splitarray2[10]);
					double Expmass2 = Double.parseDouble(splitarray2[12]);
					String symbol2 = splitarray2[7];
					String accession2 = splitarray2[6];
					double mz2 = Double.parseDouble(splitarray2[11]);
					double rt2 = Double.parseDouble(splitarray2[16]);
					double theomass2 = Double.parseDouble(splitarray2[13]);

					output.MS_CalibratedRT = extrapolRT;
					output.Conf_Cystein = Integer.parseInt(splitarray2[1]);
					output.Conf_Oxidation = Integer.parseInt(splitarray2[2]);
					output.Conf_DeltaMass = Integer.parseInt(splitarray2[3]);
					if (output.Conf_Cystein == 1 && output.Conf_DeltaMass == 1
							&& output.Conf_Oxidation == 1) {
						output.Conf_Total = Xcorr2;
					}
					output.PD_Rank = Double.parseDouble(splitarray2[4]);
					output.Pep_Sequence = sequence2;
					output.Precursor_ID = accession2;
					output.Precursor_Symbol = symbol2;
					output.Precursor_Name = splitarray2[8];
					output.Pep_Modifications = splitarray2[9];
					output.PD_XCorr = Xcorr2;
					output.MS_MZ = mz2;
					output.MS_MH = Expmass2;
					output.MS_CalculatedMass = theomass2;
					output.MS_DeltaM = Double.parseDouble(splitarray2[14]);
					output.Pep_NbBasicAa = Integer.parseInt(splitarray2[15]);
					output.MS_RT = rt2;
					output.MS_FirstScan = Double.parseDouble(splitarray2[17]);
					output.MS_LastScan = Double.parseDouble(splitarray2[18]);
					output.MS_MSOrder = splitarray2[19];
					output.MS_IonMatched = Integer.parseInt(splitarray2[20]);
					output.MS_IonTotal = Integer.parseInt(splitarray2[21]);
					output.PD_SpectrumFile = filename2;
					output.PD_Conf = splitarray2[23];
					output.PD_NumberProt = splitarray2[24];
					output.PD_NumberProtGroup = splitarray2[25];
					output.MS_ActivationType = splitarray2[26];
					output.PD_Proba = splitarray2[27];
					output.PD_Score = Double.parseDouble(splitarray2[28]);
					output.MS_Area = Double.parseDouble(splitarray2[29]);
					output.MS_Intensity = Double.parseDouble(splitarray2[30]);
					output.MS_Charge = Double.parseDouble(splitarray2[31]);

					BufferedReader bReader2 = createBufferedreader(filepath);
					String line2;
					bReader2.readLine();
					while ((line2 = bReader2.readLine()) != null) {
						String splitarray[] = line2.split("\t");
						String filename = splitarray[8];
						if (filename.equals(filename2)) {
							String sequence = splitarray[9];
							if (sequence.equals(sequence2)) {
								double Xcorr = Double
										.parseDouble(splitarray[10]);
//								
								
								if (Xcorr == Xcorr2) {
									double Expmass = Double
											.parseDouble(splitarray[11]) + 1.007276;
//									
									
									if (Expmass == Expmass2) {
										String symbol = splitarray[12];
										if (symbol.equals(symbol2)) {
											String accession = splitarray[13];
											if (accession.equals(accession2)) {
												double mz = Double
														.parseDouble(splitarray[14]);
//											
												if (mz == mz2) {
													double rt = Double
															.parseDouble(splitarray[15]);
//													
													if (rt == rt2) {
														double theomass = Double
																.parseDouble(splitarray[19]);
//														
														if (theomass == theomass2) {
															extrapolRT = Double
																	.parseDouble(splitarray[2]);
															output.MS_CalibratedRT = extrapolRT;
//
//															int rsSize = 0;
//															String queryUpdate = "UPDATE LCMSMSCOMPLETEDATA SET LCMSMSCOMPLETEDATA.Calibrated_CE_t_min = "
//																	+ extrapolRT 
//																	+ ", LCMSMSCOMPLETEDATA.ConfTotal = "
//																	+ output.Conf_Total
//																	+" WHERE CONVERT (Sequence using latin1) COLLATE latin1_general_cs = '"
//																	+ sequence2 
//																	+ "' AND Protein_Accession = '"
//																	+ accession2
//																	+ "' AND Protein_Symbol ='"
//																	+ symbol2
//																	+"' AND Xcorr >= "
//																	+ Xcorr3min + " AND Xcorr <=" + Xcorr3max
//																	+ " AND m_z_Da >= "
//																	+ mz3min + " AND m_z_Da <=" + mz3max
//																	+ " AND ExperimentalMass_H_Da >= "
//																	+ Expmass3min + " AND ExperimentalMass_H_Da <=" + Expmass3max
//																	+ " AND TheoriticalMass_Da >= "
//																	+ theomass3min + " AND TheoriticalMass_Da <=" + theomass3max
//																	+ " AND CE_t_min >= "
//																	+ rt3min + " AND CE_t_min <=" + rt3max
//																	+ " AND Spectrum_File = '"
//																	+ filename2
//																	+ "'";
//															System.out
//																	.println(queryUpdate);
//															Connection connection = getConn();
//															Statement s = connection
//																	.createStatement();
//															try {
//																int result = s
//																		.executeUpdate(queryUpdate);
//																s.close();
//																connection
//																		.close();
//															} catch (Throwable ignore) {
//																System.err
//																		.println("Mysql Statement Error: "
//																				+ queryUpdate);
//																ignore.printStackTrace();
//															}
														}
													}
												}
											}
										}
									}
								}
							}
						}
					}
					outputList.add(output);
				}
				// Ecrire le fichier
				try {
					System.out.println("-----------------");
					csvWriter = new PrintStream(withextrapol);
					populateHeaders(csvWriter);
					for (Output output : outputList) {
						populateData(csvWriter, output);
					}

				} catch (FileNotFoundException ex) {
					Logger.getLogger(Main_ApplyConfidence.class.getName()).log(
							Level.SEVERE, null, ex);
				} finally {
					csvWriter.close();
				}
			}
		}

		}

	}

	public static void main(String[] args) throws IOException, SQLException {
		// TODO code application logic here
		Main_fromOmsatoDB Main_fromOmsatoDB = new Main_fromOmsatoDB();
	}

	private BufferedReader createBufferedreader(String datafilename)
			throws FileNotFoundException {
		BufferedReader bReader = new BufferedReader(
				new FileReader(datafilename));
		return bReader;

	}

	private void populateHeaders(PrintStream csvWriter) {
		csvWriter.print("ConfTotal");
		csvWriter.print("\t");
		csvWriter.print("ConfCystein/X");
		csvWriter.print("\t");
		csvWriter.print("ConfOxidation");
		csvWriter.print("\t");
		// csvWriter.print("ConfDeamination");
		// csvWriter.print("\t");
		csvWriter.print("ConfDeltaMass");
		csvWriter.print("\t");
		csvWriter.print("Rank");
		csvWriter.print("\t");
		csvWriter.print("Sequence");
		csvWriter.print("\t");
		csvWriter.print("Protein Accession");
		csvWriter.print("\t");
		csvWriter.print("Protein Symbol");
		csvWriter.print("\t");
		csvWriter.print("Protein Name");
		csvWriter.print("\t");
		csvWriter.print("Modifications");
		csvWriter.print("\t");
		csvWriter.print("Xcorr");
		csvWriter.print("\t");
		csvWriter.print("m/z [Da]");
		csvWriter.print("\t");
		csvWriter.print("MH+ [Da]");
		csvWriter.print("\t");
		csvWriter.print("Calculated M [Da]");
		csvWriter.print("\t");
		csvWriter.print("?M [ppm]");
		csvWriter.print("\t");
		csvWriter.print("Nb basic aa");
		csvWriter.print("\t");
		csvWriter.print("RT [min]");
		csvWriter.print("\t");
		csvWriter.print("CalibratedRT [min]");
		csvWriter.print("\t");
		csvWriter.print("First Scan");
		csvWriter.print("\t");
		csvWriter.print("Last Scan");
		csvWriter.print("\t");
		csvWriter.print("MS Order");
		csvWriter.print("\t");
		csvWriter.print("Ions Matched");
		csvWriter.print("\t");
		csvWriter.print("Ions Total");
		csvWriter.print("\t");
		csvWriter.print("Spectrum File");
		csvWriter.print("\t");
		csvWriter.print("Confidence Icon");
		csvWriter.print("\t");
		csvWriter.print("# Proteins");
		csvWriter.print("\t");
		csvWriter.print("# Protein Groups");
		csvWriter.print("\t");
		csvWriter.print("Activation Type");
		csvWriter.print("\t");
		csvWriter.print("Probability");
		csvWriter.print("\t");
		csvWriter.print("? Score");
		csvWriter.print("\t");
		csvWriter.print("Area");
		csvWriter.print("\t");
		csvWriter.print("Intensity");
		csvWriter.print("\t");
		csvWriter.print("Charge");
		csvWriter.print("\n");
	}

	private void populateData(PrintStream csvWriter, Output output) {
		csvWriter.print(output.Conf_Total);
		csvWriter.print("\t");
		csvWriter.print(output.Conf_Cystein);
		csvWriter.print("\t");
		csvWriter.print(output.Conf_Oxidation);
		csvWriter.print("\t");
		// csvWriter.print(output.Conf_Deamination);
		// csvWriter.print("\t");
		csvWriter.print(output.Conf_DeltaMass);
		csvWriter.print("\t");
		csvWriter.print(output.PD_Rank);
		csvWriter.print("\t");
		csvWriter.print(output.Pep_Sequence);
		csvWriter.print("\t");
		csvWriter.print(output.Precursor_ID);
		csvWriter.print("\t");
		csvWriter.print(output.Precursor_Symbol);
		csvWriter.print("\t");
		csvWriter.print(output.Precursor_Name);
		csvWriter.print("\t");
		csvWriter.print(output.Pep_Modifications);
		csvWriter.print("\t");
		csvWriter.print(output.PD_XCorr);
		csvWriter.print("\t");
		csvWriter.print(output.MS_MZ);
		csvWriter.print("\t");
		csvWriter.print(output.MS_MH);
		csvWriter.print("\t");
		csvWriter.print(output.MS_CalculatedMass);
		csvWriter.print("\t");
		csvWriter.print(output.MS_DeltaM);
		csvWriter.print("\t");
		csvWriter.print(output.Pep_NbBasicAa);
		csvWriter.print("\t");
		csvWriter.print(output.MS_RT);
		csvWriter.print("\t");
		csvWriter.print(output.MS_CalibratedRT);
		csvWriter.print("\t");
		csvWriter.print(output.MS_FirstScan);
		csvWriter.print("\t");
		csvWriter.print(output.MS_LastScan);
		csvWriter.print("\t");
		csvWriter.print(output.MS_MSOrder);
		csvWriter.print("\t");
		csvWriter.print(output.MS_IonMatched);
		csvWriter.print("\t");
		csvWriter.print(output.MS_IonTotal);
		csvWriter.print("\t");
		csvWriter.print(output.PD_SpectrumFile);
		csvWriter.print("\t");
		csvWriter.print(output.PD_Conf);
		csvWriter.print("\t");
		csvWriter.print(output.PD_NumberProt);
		csvWriter.print("\t");
		csvWriter.print(output.PD_NumberProtGroup);
		csvWriter.print("\t");
		csvWriter.print(output.MS_ActivationType);
		csvWriter.print("\t");
		csvWriter.print(output.PD_Proba);
		csvWriter.print("\t");
		csvWriter.print(output.PD_Score);
		csvWriter.print("\t");
		csvWriter.print(output.MS_Area);
		csvWriter.print("\t");
		csvWriter.print(output.MS_Intensity);
		csvWriter.print("\t");
		csvWriter.print(output.MS_Charge);
		csvWriter.print("\n");
	}

	private Connection getConn() {
		Connection conn = null;

		try {
			String host = "jdbc:mysql://localhost:3306/";
			String dbName = "MSMSDatabase";
			String usermame = "root";
			String pwd = "kschoicesql";
			// String host = "jdbc:mysql://srvW2008R2.samba:3306/";
			// String dbName = "LCMSMSDatabase";
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

}
