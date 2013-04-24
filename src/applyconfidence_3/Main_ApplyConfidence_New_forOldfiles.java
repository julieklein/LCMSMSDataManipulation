package applyconfidence_3;

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
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.w3c.dom.Document;
import org.w3c.dom.NodeList;

public class Main_ApplyConfidence_New_forOldfiles {
	private String[][] aaMasses = new String[2][24];

	public Main_ApplyConfidence_New_forOldfiles() throws IOException, SQLException {
		boolean dirFlag = false;

		aaMasses[0][0] = "A";
		aaMasses[0][1] = "R";
		aaMasses[0][2] = "N";
		aaMasses[0][3] = "D";
		aaMasses[0][4] = "C";
		aaMasses[0][5] = "E";
		aaMasses[0][6] = "Q";
		aaMasses[0][7] = "G";
		aaMasses[0][8] = "H";
		aaMasses[0][9] = "I";
		aaMasses[0][10] = "L";
		aaMasses[0][11] = "K";
		aaMasses[0][12] = "M";
		aaMasses[0][13] = "F";
		aaMasses[0][14] = "P";
		aaMasses[0][15] = "S";
		aaMasses[0][16] = "T";
		aaMasses[0][17] = "W";
		aaMasses[0][18] = "Y";
		aaMasses[0][19] = "V";
		aaMasses[0][20] = "p";
		aaMasses[0][21] = "m";
		aaMasses[0][22] = "n";
		aaMasses[0][23] = "q";

		aaMasses[1][0] = "71.03711";
		aaMasses[1][1] = "156.10111";
		aaMasses[1][2] = "114.04293";
		aaMasses[1][3] = "115.02694";
		aaMasses[1][4] = "103.00919";
		aaMasses[1][5] = "129.04259";
		aaMasses[1][6] = "128.05858";
		aaMasses[1][7] = "57.02146";
		aaMasses[1][8] = "137.05891";
		aaMasses[1][9] = "113.08406";
		aaMasses[1][10] = "113.08406";
		aaMasses[1][11] = "128.09496";
		aaMasses[1][12] = "131.04049";
		aaMasses[1][13] = "147.06841";
		aaMasses[1][14] = "97.05276";
		aaMasses[1][15] = "87.03203";
		aaMasses[1][16] = "101.04768";
		aaMasses[1][17] = "186.07931";
		aaMasses[1][18] = "163.06333";
		aaMasses[1][19] = "99.06841";
		aaMasses[1][20] = "113.04767";
		aaMasses[1][21] = "147.03540";
		aaMasses[1][22] = "115.02695";
		aaMasses[1][23] = "129.0426";

		// create File object
		File stockDir = new File(
				"//Users/julieklein/Documents/MosaiquesDatabase/LCMSMSdata/textfiles/confidence/fait2/new");

		try {
			dirFlag = stockDir.mkdir();
		} catch (SecurityException Se) {
			System.out.println("Error while creating directory in Java:" + Se);
		}

		if (dirFlag)
			System.out.println("Directory created successfully");
		else
			System.out.println("Directory was not created successfully");
		//
		File f = new File(
				"//Users/julieklein/Documents/MosaiquesDatabase/LCMSMSdata/textfiles/confidence/fait4");
		File[] files = f.listFiles();
		for (File file : files) {
			String filepath = "/" + file.getPath();
			if (!filepath.contains("DS_Store") && !filepath.contains("OMSA")
					&& !filepath.contains("extrapolated") && !filepath.contains("saved") && !filepath.contains("new")) {

				String inputfilename = filepath;
				inputfilename = inputfilename.replaceAll("_Confidence.txt", "");
				inputfilename = inputfilename
						.replace(
								"//Users/julieklein/Documents/MosaiquesDatabase/LCMSMSdata/textfiles/confidence/fait4/",
								"");
				System.out.println(inputfilename);
				String outputfilename = "//Users/julieklein/Documents/MosaiquesDatabase/LCMSMSdata/textfiles/confidence/fait4/new/"
						+ inputfilename + "_Extrapo_Conf.txt";
				System.out.println(outputfilename);

				String LCorCE = "";
				if (filepath.contains("LCMSMS")) {
					LCorCE = "LC";
					System.out.println(LCorCE);
				} else if (filepath.contains("CEMSMS")) {
					LCorCE = "CE";
					System.out.println(LCorCE);
				}

				String inputfilenameomsa = "//Users/julieklein/Documents/MosaiquesDatabase/LCMSMSdata/textfiles/confidence/fait4/extrapolated/"
						+ inputfilename + "_ConfOMSA-20130416-1013.txt";

				System.out.println(inputfilenameomsa);

				PrintStream csvWriter = null;
				LinkedList<Output> outputList = new LinkedList<Output>();
				BufferedReader bReader = createBufferedreader(filepath);
				String line;
				bReader.readLine();
				int i = 0;
				while ((line = bReader.readLine()) != null) {
					System.out.println(i);
					String splitarray[] = line.split("\t");
					if (!line.isEmpty()) {
						Output output = new Output();
						String multipleIDList = splitarray[6];
						multipleIDList = multipleIDList.replaceAll("\"", "");
						output.Precursor_Symbol = splitarray[7];
						output.Precursor_ID = multipleIDList;
						output.Precursor_Name = splitarray[8];
						
						
//						int rsSize = 0;
//						String queryProtease = "SELECT * FROM IDMAPPING WHERE MultipleIDs= '"
//								+ multipleIDList + "'";
//						Connection connection = getConn();
//						Statement s = connection.createStatement();
//						try {
//							ResultSet result = s.executeQuery(queryProtease);
//							rsSize = getResultSetSize(result);
//							if (rsSize != 0) {
//								while (result.next()) {
//									Output output = new Output();
//									String precursorsymbol = result
//											.getString("PrecursorSymbol");
//									String precursorname = result
//											.getString("PrecursorName");
//									String precursorid = result
//											.getString("ChosenID");
//									output.Precursor_Symbol = precursorsymbol;
//									output.Precursor_ID = precursorid;
//									output.Precursor_Name = precursorname;
									populateOutput(splitarray, output, LCorCE,
											inputfilenameomsa, multipleIDList);
									outputList.add(output);
//								}
//							} else {
//								String precursorIDList[] = splitarray[2]
//										.split(";");
//								Map<String, List<Set<String>>> processedLinesMultipleIDs = new HashMap<String, List<Set<String>>>();
//								for (String searchUni : precursorIDList) {
//									searchUni = searchUni.replaceAll("\"", "");
//									System.out.println(searchUni);
//									String substratename = "-";
//									String substratesymbol = "-";
//									String dataset = "";
//
//									// Retrieve PROT NAME AND SYMBOL IN UNIPROT
//									Document xml = checkSubUniprot(searchUni);
//
//									XPathUniprotPep XPather3 = new XPathUniprotPep();
//									String xpathQuery3 = "/uniprot/entry[@type='dataset']/text()";
//									NodeList getNodeListbyXPath3 = XPather3
//											.getNodeListByXPath(xpathQuery3,
//													xml);
//									if (getNodeListbyXPath3.getLength() > 0) {
//										XPathNodeUniprot XPathNoder3 = new XPathNodeUniprot();
//										String xpathQueryNode3 = "/uniprot/entry[@type='dataset']/text()";
//										Loop l1 = new Loop();
//										for (int j1 = 0; j1 < getNodeListbyXPath3
//												.getLength(); j1++) {
//											NodeList getNodeListByXPathNoder3 = XPathNoder3
//													.getNodeListByXPath(
//															xpathQueryNode3,
//															getNodeListbyXPath3
//																	.item(j1));
//											LinkedList<String> stringfromNodelist3 = l1
//													.getStringfromNodelist(getNodeListByXPathNoder3);
//											dataset = stringfromNodelist3
//													.getFirst();
//										}
//									}
//
//									XPathUniprotPep XPather4 = new XPathUniprotPep();
//									String xpathQuery4 = "/uniprot/entry/protein/recommendedName/fullName/text()";
//									NodeList getNodeListbyXPath4 = XPather4
//											.getNodeListByXPath(xpathQuery4,
//													xml);
//
//									if (getNodeListbyXPath4.getLength() > 0) {
//										XPathNodeUniprot XPathNoder4 = new XPathNodeUniprot();
//										String xpathQueryNode4 = "/uniprot/entry/protein/recommendedName/fullName/text()";
//										Loop l1 = new Loop();
//										for (int j1 = 0; j1 < getNodeListbyXPath4
//												.getLength(); j1++) {
//											NodeList getNodeListByXPathNoder4 = XPathNoder4
//													.getNodeListByXPath(
//															xpathQueryNode4,
//															getNodeListbyXPath4
//																	.item(j1));
//											LinkedList<String> stringfromNodelist4 = l1
//													.getStringfromNodelist(getNodeListByXPathNoder4);
//											substratename = !(stringfromNodelist4
//													.isEmpty()) ? stringfromNodelist4
//													.getFirst() : "---";
//										}
//									} else {
//										XPathUniprotPep XPather40 = new XPathUniprotPep();
//										String xpathQuery40 = "/uniprot/entry/protein/submittedName/fullName/text()";
//										NodeList getNodeListbyXPath40 = XPather40
//												.getNodeListByXPath(
//														xpathQuery40, xml);
//
//										if (getNodeListbyXPath40.getLength() > 0) {
//											XPathNodeUniprot XPathNoder40 = new XPathNodeUniprot();
//											String xpathQueryNode40 = "/uniprot/entry/protein/submittedName/fullName/text()";
//											Loop l1 = new Loop();
//											for (int j1 = 0; j1 < getNodeListbyXPath40
//													.getLength(); j1++) {
//												NodeList getNodeListByXPathNoder40 = XPathNoder40
//														.getNodeListByXPath(
//																xpathQueryNode40,
//																getNodeListbyXPath40
//																		.item(j1));
//												LinkedList<String> stringfromNodelist40 = l1
//														.getStringfromNodelist(getNodeListByXPathNoder40);
//												substratename = !(stringfromNodelist40
//														.isEmpty()) ? stringfromNodelist40
//														.getFirst() : "---";
//											}
//										}
//									}
//									substratename = substratename.replaceAll(
//											"'", "");
//
//									XPathUniprotPep XPather5 = new XPathUniprotPep();
//									String xpathQuery5 = "/uniprot/entry/gene/name[@type='primary']/text()";
//									NodeList getNodeListbyXPath5 = XPather5
//											.getNodeListByXPath(xpathQuery5,
//													xml);
//
//									if (getNodeListbyXPath5.getLength() > 0) {
//										XPathNodeUniprot XPathNoder5 = new XPathNodeUniprot();
//										String xpathQueryNode5 = "/uniprot/entry/gene/name[@type='primary']/text()";
//										Loop l1 = new Loop();
//										for (int j1 = 0; j1 < getNodeListbyXPath5
//												.getLength(); j1++) {
//											NodeList getNodeListByXPathNoder5 = XPathNoder5
//													.getNodeListByXPath(
//															xpathQueryNode5,
//															getNodeListbyXPath5
//																	.item(j1));
//											LinkedList<String> stringfromNodelist5 = l1
//													.getStringfromNodelist(getNodeListByXPathNoder5);
//											substratesymbol = !(stringfromNodelist5
//													.isEmpty()) ? stringfromNodelist5
//													.getFirst() : substratename;
//
//										}
//									} else {
//										substratesymbol = substratename;
//									}
//
//									String key = substratesymbol;
//									if (!processedLinesMultipleIDs
//											.containsKey(key)) {
//										List value = new ArrayList<Set<String>>();
//										for (int j = 0; j < 2; j++) {
//											value.add(new HashSet<String>());
//										}
//										processedLinesMultipleIDs.put(key,
//												value);
//									}
//									processedLinesMultipleIDs.get(key).get(0)
//											.add(substratesymbol);
//									processedLinesMultipleIDs
//											.get(key)
//											.get(1)
//											.add(substratesymbol + dataset
//													+ ";" + searchUni + ";"
//													+ substratename);
//								}
//
//								Iterator iterator = processedLinesMultipleIDs
//										.values().iterator();
//								while (iterator.hasNext()) {
//									String values = iterator.next().toString();
//									String splitHashMap[] = values
//											.split("\\], \\[");
//									String precursorsymbol = splitHashMap[0];
//									precursorsymbol = precursorsymbol
//											.replaceAll("\\[", "");
//									String multiple = splitHashMap[1];
//									multiple = multiple.replaceAll("\\]", "");
//									if (multiple.contains("Swiss-Prot")) {
//										String splitmultiple[] = multiple
//												.split("substratesymbol");
//										for (String string : splitmultiple) {
//											if (string.contains("Swiss-Prot")) {
//												String splitsplitmultiple[] = string
//														.split(";");
//												Output output = new Output();
//												output.Precursor_Symbol = precursorsymbol;
//												output.Precursor_ID = splitsplitmultiple[1];
//												output.Precursor_Name = splitsplitmultiple[2];
//												populateOutput(splitarray,
//														output, LCorCE,
//														inputfilenameomsa,
//														multipleIDList);
//												outputList.add(output);
//
//												String queryaddMultipleIDs = "INSERT INTO IDMAPPING VALUES('"
//														+ multipleIDList
//														+ "', '"
//														+ splitsplitmultiple[1]
//														+ "', '"
//														+ splitsplitmultiple[2]
//														+ "','"
//														+ precursorsymbol
//														+ "')";
//												Connection connection2 = getConn();
//												Statement s2 = connection2
//														.createStatement();
//												try {
//													int result2 = s2
//															.executeUpdate(queryaddMultipleIDs);
//													s2.close();
//													connection2.close();
//												} catch (Throwable ignore) {
//													System.err
//															.println("Mysql Statement Error: "
//																	+ queryaddMultipleIDs);
//													ignore.printStackTrace();
//												}
//
//											}
//										}
//									} else {
//										String splitmultiple[] = multiple
//												.split("substratesymbol");
//										for (String string : splitmultiple) {
//											String splitsplitmultiple[] = string
//													.split(";");
//											Output output = new Output();
//											output.Precursor_Symbol = precursorsymbol;
//											output.Precursor_ID = splitsplitmultiple[1];
//											output.Precursor_Name = splitsplitmultiple[2];
//											populateOutput(splitarray, output,
//													LCorCE, inputfilenameomsa,
//													multipleIDList);
//											outputList.add(output);
//											String queryaddMultipleIDs = "INSERT INTO IDMAPPING VALUES('"
//													+ multipleIDList
//													+ "', '"
//													+ splitsplitmultiple[1]
//													+ "', '"
//													+ splitsplitmultiple[2]
//													+ "','"
//													+ precursorsymbol
//													+ "')";
//											Connection connection2 = getConn();
//											Statement s2 = connection2
//													.createStatement();
//											try {
//												int result2 = s2
//														.executeUpdate(queryaddMultipleIDs);
//												s2.close();
//												connection2.close();
//											} catch (Throwable ignore) {
//												System.err
//														.println("Mysql Statement Error: "
//																+ queryaddMultipleIDs);
//												ignore.printStackTrace();
//											}
//										}
//									}
//								}
//							}
//							result.close();
//							s.close();
//							connection.close();
//						} catch (Throwable ignore) {
//							System.err.println("Mysql Statement Error: "
//									+ queryProtease);
//							ignore.printStackTrace();
//						}
						i++;
					}
				}

				try {
					System.out.println("-----------------");
					csvWriter = new PrintStream(outputfilename);
					populateHeaders(csvWriter);
					for (Output output : outputList) {
						populateData(csvWriter, output);

					}

				} catch (FileNotFoundException ex) {
					Logger.getLogger(Main_ApplyConfidence_New_forOldfiles.class.getName())
							.log(Level.SEVERE, null, ex);
				} finally {
					csvWriter.close();
				}
				file.delete();
			}
			
		}

	}

	private void populateOutput(String[] splitarray, Output output,
			String LCorCE, String inputfilenameomsa, String multipleIDList)
			throws SQLException, IOException {
		output.PD_Conf = splitarray[23];
		output.Pep_Sequence = splitarray[5];
		output.PD_NumberProt = splitarray[24];
		output.PD_NumberProtGroup = splitarray[25];
		output.MS_ActivationType = splitarray[26];
		output.Pep_Modifications = splitarray[9];
		output.PD_Proba = splitarray[27];
		String sXcorr = splitarray[10].replaceAll(",", ".");
		output.PD_XCorr = Double.parseDouble(sXcorr);
		String sScore = splitarray[28].replaceAll(",", ".");
		output.PD_Score = !splitarray[28].equals("") ? Double
				.parseDouble(sScore) : 0;
		String sRank = splitarray[4].replaceAll(",", ".");
		output.PD_Rank = Double.parseDouble(sRank);
		String sArea = splitarray[29].replaceAll(",", ".");
		output.MS_Area = !splitarray[29].equals("") ? Double.parseDouble(sArea)
				: 0;
		String sIntensity = splitarray[30].replaceAll(",", ".");
		output.MS_Intensity = !splitarray[30].equals("") ? Double
				.parseDouble(sIntensity) : 0;
		String sCharge = splitarray[31].replaceAll(",", ".");
		output.MS_Charge = Double.parseDouble(sCharge);
		String smz = splitarray[11].replaceAll(",", ".");
		output.MS_MZ = Double.parseDouble(smz);
		String smh = splitarray[12].replaceAll(",", ".");
		output.MS_MH = Double.parseDouble(smh);
		String sdelta = splitarray[14].replaceAll(",", ".");
		output.MS_DeltaM = Double.parseDouble(sdelta);
		String sRT = splitarray[16].replaceAll(",", ".");
		output.MS_RT = Double.parseDouble(sRT);

		String sfirst = splitarray[17].replaceAll(",", ".");
		output.MS_FirstScan = Double.parseDouble(sfirst);
		String slast = splitarray[18].replaceAll(",", ".");
		output.MS_LastScan = Double.parseDouble(slast);
		output.MS_MSOrder = splitarray[19];
//		String Ion = splitarray[21];
//		String splitIon[] = Ion.split("/");
		output.MS_IonMatched = Integer.parseInt(splitarray[20]);
		output.MS_IonTotal = Integer.parseInt(splitarray[21]);
		output.PD_SpectrumFile = splitarray[22];

		//
		double calculatedmass = 0;
		int nbBasicAa = 0;
		String splitsequence[] = output.Pep_Sequence.split("");
		for (String string : splitsequence) {
			nbBasicAa = (string.equalsIgnoreCase("R")
					|| string.equalsIgnoreCase("H") || string
					.equalsIgnoreCase("K")) ? nbBasicAa + 1 : nbBasicAa;
			for (int j = 0; j < 24; j++) {
				calculatedmass = string.equals(aaMasses[0][j]) ? calculatedmass
						+ Double.parseDouble(aaMasses[1][j]) : calculatedmass;
			}
		}
		calculatedmass = calculatedmass + 18.0105647;
		output.Pep_NbBasicAa = nbBasicAa;
		output.MS_CalculatedMass = calculatedmass;

		int confOxidation = 1;
		if (output.Pep_Modifications.contains("P")) {
			confOxidation = (output.Precursor_Name.contains("Collagen") || output.Precursor_Name
					.contains("Elastin")) ? 1 : 0;
		}
		output.Conf_Oxidation = confOxidation;

		// Matcher retrieveNDeamination = getPatternmatcher("(N[^(]+)",
		// output.Pep_Modifications);
		// Matcher retrieveQDeamination = getPatternmatcher("(Q[^(]+)",
		// output.Pep_Modifications);
		// boolean confidenceAmination = true;
		// confidenceAmination = getConfNDeamination(confidenceAmination,
		// retrieveNDeamination);
		// confidenceAmination = getConfQDeamination(confidenceAmination,
		// retrieveQDeamination);
		// int confDeamination = confidenceAmination ? 1 : 0;
		// output.Conf_Deamination = confDeamination;

		int confCystein = (!splitarray[5].contains("C") && !splitarray[5]
				.contains("X")) ? 1 : 0;
		output.Conf_Cystein = confCystein;

		double Xcorr = Double.parseDouble(splitarray[10].replaceAll(",", "."));

		double Deltamass = Double.parseDouble(splitarray[14].replaceAll(",",
				"."));
		int confDeltaMass = (Deltamass > -5 && Deltamass < 5) ? 1 : 0;
		output.Conf_DeltaMass = confDeltaMass;

		if (confCystein == 0) {
			output.Conf_Total = 0;
		} else if (confOxidation == 0) {
			output.Conf_Total = 0;
		} else if (output.PD_Conf.equals("Low")
				|| output.PD_Conf.equals("Medium")) {
			output.Conf_Total = 0;
		} else if (confDeltaMass == 0) {
			output.Conf_Total = 0;
		} else {
			output.Conf_Total = Xcorr;
		}

		if (LCorCE.equals("CE")) {
			String scalibrated = splitarray[23].replaceAll(",", ".");
			output.MS_CalibratedRT = Double.parseDouble(scalibrated);
		} else {
			double calibrated = 0;
			output.MS_CalibratedRT = calibrated;
			BufferedReader bReader2 = createBufferedreader(inputfilenameomsa);
			String line2;
			bReader2.readLine();
			if (!output.Pep_Modifications.contains("N1(Deamidated)")
					&& !output.Pep_Modifications.contains("Q1(Deamidated)")) {
				while ((line2 = bReader2.readLine()) != null) {
					String splitarray2[] = line2.split("\t");
					String filename = splitarray2[8];
					if (filename.equals(output.PD_SpectrumFile)) {
						String sequence = splitarray2[9];
						if (sequence.equals(output.Pep_Sequence)) {
							double Xcorr2 = Double.parseDouble(splitarray2[10]);

							if (Xcorr2 == output.PD_XCorr) {
								double Expmass = Double
										.parseDouble(splitarray2[11]) + 1.007276;

								if (Expmass == output.MS_MH) {
									String accession = splitarray2[13];
									if (accession.equals(multipleIDList)) {
										double mz = Double
												.parseDouble(splitarray2[14]);
										if (mz == output.MS_MZ) {
											double rt = Double
													.parseDouble(splitarray2[15]);
											if (rt == output.MS_RT) {
												double theomass = Double
														.parseDouble(splitarray2[19]);
												if (theomass == output.MS_CalculatedMass) {
													if (!splitarray2[5]
															.isEmpty()) {
														double charge = Double
																.parseDouble(splitarray2[5]);
														if (output.Pep_NbBasicAa + 1 <= 5) {
															if (charge == output.Pep_NbBasicAa + 1) {
																calibrated = Double
																		.parseDouble(splitarray2[2]);
																output.MS_CalibratedRT = calibrated;
															}
														} else if (output.Pep_NbBasicAa + 1 > 5){
															if (charge == 5) {
																calibrated = Double
																		.parseDouble(splitarray2[2]);
																output.MS_CalibratedRT = calibrated;
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
					}
				}
			}

		}
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
		Date today = new Date();
		String todaydate = dateFormat.format(today);
		String Status = "new_" + todaydate;

		String queryadddata = "";
		if (output.Conf_Cystein != 0 && output.Conf_Oxidation != 0) {

			queryadddata = "INSERT IGNORE INTO LCMSMSCOMPLETEDATA_good VALUES("
					+ output.Conf_Total + ", " + output.Conf_Cystein + ", "
					+ output.Conf_Oxidation + ", " + output.Conf_DeltaMass
					+ ", " + output.PD_Rank + ", '" + output.Pep_Sequence
					+ "', '" + output.Precursor_ID + "', '"
					+ output.Precursor_Symbol + "', '" + output.Precursor_Name
					+ "', '" + output.Pep_Modifications + "', "
					+ output.PD_XCorr + ", " + output.MS_MZ + ", "
					+ output.MS_MH + ", " + output.MS_CalculatedMass + ", "
					+ output.MS_DeltaM + ", " + output.Pep_NbBasicAa + ", "
					+ output.MS_RT + ", " + output.MS_CalibratedRT + ", "
					+ output.MS_FirstScan + ", " + output.MS_LastScan + ", '"
					+ output.MS_MSOrder + "', " + output.MS_IonMatched + ", "
					+ output.MS_IonTotal + ", '" + output.PD_SpectrumFile
					+ "', '" + output.PD_Conf + "', '" + output.PD_NumberProt
					+ "', '" + output.PD_NumberProtGroup + "', '"
					+ output.MS_ActivationType + "', '" + output.PD_Proba
					+ "', '" + output.PD_Score + "', " + output.MS_Area + ", "
					+ output.MS_Intensity + ", " + output.MS_Charge + " ,'"
					+ Status + "')";
		} else {
			queryadddata = "INSERT IGNORE INTO LCMSMSCOMPLETEDATA_wrong VALUES("
					+ output.Conf_Total
					+ ", "
					+ output.Conf_Cystein
					+ ", "
					+ output.Conf_Oxidation
					+ ", "
					+ output.Conf_DeltaMass
					+ ", "
					+ output.PD_Rank
					+ ", '"
					+ output.Pep_Sequence
					+ "', '"
					+ output.Precursor_ID
					+ "', '"
					+ output.Precursor_Symbol
					+ "', '"
					+ output.Precursor_Name
					+ "', '"
					+ output.Pep_Modifications
					+ "', "
					+ output.PD_XCorr
					+ ", "
					+ output.MS_MZ
					+ ", "
					+ output.MS_MH
					+ ", "
					+ output.MS_CalculatedMass
					+ ", "
					+ output.MS_DeltaM
					+ ", "
					+ output.Pep_NbBasicAa
					+ ", "
					+ output.MS_RT
					+ ", "
					+ output.MS_CalibratedRT
					+ ", "
					+ output.MS_FirstScan
					+ ", "
					+ output.MS_LastScan
					+ ", '"
					+ output.MS_MSOrder
					+ "', "
					+ output.MS_IonMatched
					+ ", "
					+ output.MS_IonTotal
					+ ", '"
					+ output.PD_SpectrumFile
					+ "', '"
					+ output.PD_Conf
					+ "', '"
					+ output.PD_NumberProt
					+ "', '"
					+ output.PD_NumberProtGroup
					+ "', '"
					+ output.MS_ActivationType
					+ "', '"
					+ output.PD_Proba
					+ "', '"
					+ output.PD_Score
					+ "', "
					+ output.MS_Area
					+ ", "
					+ output.MS_Intensity
					+ ", "
					+ output.MS_Charge
					+ " ,'"
					+ Status + "')";

		}

		Connection connection2 = getConn();
		Statement s2 = connection2.createStatement();
		try {
			int result2 = s2.executeUpdate(queryadddata);
			s2.close();
			connection2.close();
		} catch (Throwable ignore) {
			System.err.println("Mysql Statement Error: " + queryadddata);
			ignore.printStackTrace();
		}

	}

	public static void main(String[] args) throws IOException, SQLException {
		// TODO code application logic here
		Main_ApplyConfidence_New_forOldfiles Main_ApplyConfidence = new Main_ApplyConfidence_New_forOldfiles();
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

	private Document checkSubUniprot(String substrateuni) {
		String UniprotURL = "http://www.uniprot.org/uniprot/" + substrateuni
				+ ".xml";
		System.out.println(UniprotURL + "bbbbb");
		ParseUniprotPep parser = new ParseUniprotPep();
		Document xml = null;
		String xmlstring = parser.getXMLasstring(UniprotURL);
		xml = parser.getXML(UniprotURL);
		xml.getXmlVersion();
		return xml;

	}

	private Connection getConn() {
		Connection conn = null;

		try {
			// String host = "jdbc:mysql://localhost:3306/";
			// String dbName = "LCMSMSDatabase";
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

	private Matcher getPatternmatcher(String expression, String string) {
		Pattern p = Pattern.compile(expression, Pattern.DOTALL
				| Pattern.UNIX_LINES | Pattern.MULTILINE);
		Matcher matcher = p.matcher(string);
		return matcher;
	}

	private boolean getConfNDeamination(boolean confidence, Matcher N) {

		while (N.find()) {
			if (!N.group(0).equals("N1")) {
				confidence = false;
				return confidence;
			}
		}
		return confidence;
	}

	private boolean getConfQDeamination(boolean confidence, Matcher Q) {

		while (Q.find()) {
			if (!Q.group(0).equals("Q1")) {
				confidence = false;
				return confidence;
			}
		}
		return confidence;
	}
}
