package convertfrompdtoomsa_2;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.logging.Level;
import java.util.logging.Logger;

public class Main_ConvertfromPdtoOmsa_New {
	
	private String[][] aaMasses = new String [2][24];
	public Main_ConvertfromPdtoOmsa_New() throws IOException {
		
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

//		File f = new File(
//				"//Users/julieklein/Documents/MosaiquesDatabase/LCMSMSdata/textfiles/");
//		File[] files = f.listFiles();
//		for (File file : files) {
//			String filepath = "/" + file.getPath();
			String filepath = "//Users/julieklein/Documents/MosaiquesDatabase/LCMSMSdata/textfiles/10985-LTX-Galle-Urin-26355-2718-B_part2.txt";
			String filename = filepath;
			String Omsafilename = filename
					.replaceAll(".txt", "_OMSA.csv");

			if (!filepath.contains("DS_Store")) {
				System.out.println(Omsafilename);
				PrintStream csvWriter = null;
				LinkedList<Output> outputList = new LinkedList<Output>();
				BufferedReader bReader = createBufferedreader(filepath);
				String line;
				bReader.readLine();
				while ((line = bReader.readLine()) != null) {
					String splitarray[] = line.split("\t");
					Output output = new Output();
					
				
					if (Double.parseDouble(splitarray[10]) == 1.0
							&& !splitarray[1].contains("C")
							&& !splitarray[1].contains("X")) {
						output.Precursor_ID = splitarray[2];
						output.Pep_Sequence = splitarray[1];
						output.PD_XCorr = Double.parseDouble(splitarray[8]);
						output.MS_MZ = Double.parseDouble(splitarray[14]);
						output.MS_MH = Double.parseDouble(splitarray[15]) - 1.007276;
						
						double calculatedmass = 0;
						String splitsequence[] = output.Pep_Sequence.split("");
						for (String string : splitsequence) {
							for (int j = 0; j<24; j++ ) {
								calculatedmass = string.equals(aaMasses[0][j]) ? calculatedmass + Double.parseDouble(aaMasses[1][j]) : calculatedmass;
							}
						}
						calculatedmass = calculatedmass + 18.0105647;
						output.MS_CalculatedMass = calculatedmass;
						
						output.MS_RT = Double.parseDouble(splitarray[17]);
						output.MS_FirstScan = Double
								.parseDouble(splitarray[18]);
					
						String spectrum = splitarray[22];
						if (spectrum.contains(",")) {
							output.PD_SpectrumFile = "\"" + spectrum + "\"";
						} else {
							output.PD_SpectrumFile = spectrum;
						}
						output.PD_SpectrumFile = splitarray[22];
						
						output.MS_Charge = Double.parseDouble(splitarray[13]);
						outputList.add(output);
					}
				}

				try {
					System.out.println("-----------------");
					csvWriter = new PrintStream(Omsafilename);
					populateHeaders(csvWriter);
					for (Output output : outputList) {
						populateData(csvWriter, output);
					}

				} catch (FileNotFoundException ex) {
					Logger.getLogger(Main_ConvertfromPdtoOmsa_New.class.getName()).log(
							Level.SEVERE, null, ex);
				} finally {
					csvWriter.close();
				}
			}
//		}

	}

	public static void main(String[] args) throws IOException, SQLException {
		// TODO code application logic here
		Main_ConvertfromPdtoOmsa_New Main_ConvertfromPdtoOmsa = new Main_ConvertfromPdtoOmsa_New();
	}

	private BufferedReader createBufferedreader(String datafilename)
			throws FileNotFoundException {
		BufferedReader bReader = new BufferedReader(
				new FileReader(datafilename));
		return bReader;

	}

	private void populateHeaders(PrintStream csvWriter) {
		csvWriter.print("Spectrum number");
		csvWriter.print(",");
		csvWriter.print("Filename/id");
		csvWriter.print(",");
		csvWriter.print("Peptide");
		csvWriter.print(",");
		csvWriter.print("E-value");
		csvWriter.print(",");
		csvWriter.print("Mass");
		csvWriter.print(",");
		csvWriter.print("gi");
		csvWriter.print(",");
		csvWriter.print("Accession");
		csvWriter.print(",");
		csvWriter.print("Start");
		csvWriter.print(",");
		csvWriter.print("Stop");
		csvWriter.print(",");
		csvWriter.print("Defline");
		csvWriter.print(",");
		csvWriter.print("Mods");
		csvWriter.print(",");
		csvWriter.print("Charge");
		csvWriter.print(",");
		csvWriter.print("Theo Mass");
		csvWriter.print(",");
		csvWriter.print("P-value");
		csvWriter.print(",");
		csvWriter.print("NIST score");
		csvWriter.print("\n");
	}

	private void populateData(PrintStream csvWriter, Output output) {
		csvWriter.print(output.MS_FirstScan);
		csvWriter.print(",");
		csvWriter.print(output.PD_SpectrumFile);
		csvWriter.print(",");
		csvWriter.print(output.Pep_Sequence);
		csvWriter.print(",");
		csvWriter.print(output.PD_XCorr);
		csvWriter.print(",");
		csvWriter.print(output.MS_MH);
		csvWriter.print(",");
		csvWriter.print("-");
		csvWriter.print(",");
		csvWriter.print(output.Precursor_ID);
		csvWriter.print(",");
		csvWriter.print(output.MS_MZ);
		csvWriter.print(",");
		csvWriter.print(output.MS_RT);
		csvWriter.print(",");
		csvWriter.print("-");
		csvWriter.print(",");
		csvWriter.print("-");
		csvWriter.print(",");
		csvWriter.print(output.MS_Charge);
		csvWriter.print(",");
		csvWriter.print(output.MS_CalculatedMass);
		csvWriter.print(",");
		csvWriter.print("-");
		csvWriter.print(",");
		csvWriter.print("-");
		csvWriter.print("\n");
	}

}
