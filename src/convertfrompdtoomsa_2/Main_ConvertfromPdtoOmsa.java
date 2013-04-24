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

public class Main_ConvertfromPdtoOmsa {
	public Main_ConvertfromPdtoOmsa() throws IOException {

		File f = new File(
				"//Users/julieklein/Documents/MosaiquesDatabase/LCMSMSdata/textfiles/confidence/fait4");
		File[] files = f.listFiles();
		for (File file : files) {
			String filepath = "/" + file.getPath();
			String inputfilename = filepath;
			inputfilename = inputfilename
					.replaceAll("Confidence.txt", "ConfOMSA.csv");

			if (!filepath.contains("DS_Store")) {
				System.out.println(inputfilename);
				PrintStream csvWriter = null;
				LinkedList<Output> outputList = new LinkedList<Output>();
				BufferedReader bReader = createBufferedreader(filepath);
				String line;
				bReader.readLine();
				while ((line = bReader.readLine()) != null) {
					String splitarray[] = line.split("\t");
					Output output = new Output();
					output.Conf_Total = Double.parseDouble(splitarray[0]);
					output.Conf_Cystein = Integer.parseInt(splitarray[1]);
					output.Conf_Oxidation = Integer.parseInt(splitarray[2]);
					output.Conf_DeltaMass = Integer.parseInt(splitarray[3]);
					output.PD_Rank = Double.parseDouble(splitarray[4]);
					output.Pep_Sequence = splitarray[5];

					if (Double.parseDouble(splitarray[4]) == 1.0
							&& !splitarray[5].contains("C")
							&& !splitarray[5].contains("X")) {
						output.Precursor_ID = splitarray[6];

						String symbol = splitarray[7];
						if (symbol.contains(",")) {
							output.Precursor_Symbol = "\"" + symbol + "\"";
						} else {
							output.Precursor_Symbol = symbol;
						}
						output.Precursor_Symbol = splitarray[7];
						output.Precursor_Name = splitarray[8];
						output.Pep_Modifications = splitarray[9];
						output.PD_XCorr = Double.parseDouble(splitarray[10]);
						output.MS_MZ = Double.parseDouble(splitarray[11]);
						output.MS_MH = Double.parseDouble(splitarray[12]) - 1.007276;
						output.MS_CalculatedMass = Double
								.parseDouble(splitarray[13]);
						output.MS_DeltaM = Double.parseDouble(splitarray[14]);
						output.Pep_NbBasicAa = Integer.parseInt(splitarray[15]);
						output.MS_RT = Double.parseDouble(splitarray[16]);
						output.MS_FirstScan = Double
								.parseDouble(splitarray[17]);
						output.MS_LastScan = Double.parseDouble(splitarray[18]);
						output.MS_MSOrder = splitarray[19];
						output.MS_IonMatched = Integer.parseInt(splitarray[20]);
						output.MS_IonTotal = Integer.parseInt(splitarray[21]);

						String spectrum = splitarray[22];
						if (spectrum.contains(",")) {
							output.PD_SpectrumFile = "\"" + spectrum + "\"";
						} else {
							output.PD_SpectrumFile = spectrum;
						}
						output.PD_SpectrumFile = splitarray[22];
						output.PD_Conf = splitarray[23];
						output.PD_NumberProt = splitarray[24];
						output.PD_NumberProtGroup = splitarray[25];
						output.MS_ActivationType = splitarray[26];
						output.PD_Proba = splitarray[27];
						output.PD_Score = Double.parseDouble(splitarray[28]);
						output.MS_Area = Double.parseDouble(splitarray[29]);
						output.MS_Intensity = Double
								.parseDouble(splitarray[30]);
						output.MS_Charge = Double.parseDouble(splitarray[31]);
						outputList.add(output);
					}
				}

				try {
					System.out.println("-----------------");
					csvWriter = new PrintStream(inputfilename);
					populateHeaders(csvWriter);
					for (Output output : outputList) {
						populateData(csvWriter, output);
					}

				} catch (FileNotFoundException ex) {
					Logger.getLogger(Main_ConvertfromPdtoOmsa.class.getName()).log(
							Level.SEVERE, null, ex);
				} finally {
					csvWriter.close();
				}
			}
		}

	}

	public static void main(String[] args) throws IOException, SQLException {
		// TODO code application logic here
		Main_ConvertfromPdtoOmsa Main_ConvertfromPdtoOmsa = new Main_ConvertfromPdtoOmsa();
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
		csvWriter.print(output.Precursor_Symbol);
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
