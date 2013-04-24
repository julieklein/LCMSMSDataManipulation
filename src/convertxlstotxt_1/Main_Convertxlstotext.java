package convertxlstotxt_1;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;

public class Main_Convertxlstotext {

	private static List<List<HSSFCell>> cellGrid;

	public Main_Convertxlstotext() throws IOException {
		
		boolean dirFlag = false;
		// create File object
		File stockDir = new File("//Users/julieklein/Documents/MosaiquesDatabase/LCMSMSdata/RAT");

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
		File f = new File("//Users/julieklein/Documents/MosaiquesDatabase/LCMSMSdata/RAT");
		File[] files = f.listFiles();
		for (File xls : files) {
//			String filepath = "//Users/julieklein/Documents/MosaiquesDatabase/LCMSMSdata/10985-LTX-Galle-Urin-26355-2718-B_part2.xls";
			String filepath = "/" + xls.getPath();
			String filename = filepath;
			filename = filename.replaceAll(".xls", "");
//			filename = filename.replace("LCMSMSdata", "LCMSMSdata/RAT");
			System.out.println(filename);
			if (!filepath.contains("DS_Store") && !filepath.contains("textfiles")) {
				try {
					cellGrid = new ArrayList<List<HSSFCell>>();
					FileInputStream myInput = new FileInputStream(filepath);
					POIFSFileSystem myFileSystem = new POIFSFileSystem(myInput);
					HSSFWorkbook myWorkBook = new HSSFWorkbook(myFileSystem);
					HSSFSheet mySheet = myWorkBook.getSheetAt(0);
					Iterator<?> rowIter = mySheet.rowIterator();

					while (rowIter.hasNext()) {
						HSSFRow myRow = (HSSFRow) rowIter.next();
						Iterator<?> cellIter = myRow.cellIterator();
						List<HSSFCell> cellRowList = new ArrayList<HSSFCell>();
						while (cellIter.hasNext()) {
							HSSFCell myCell = (HSSFCell) cellIter.next();
							cellRowList.add(myCell);
						}
						cellGrid.add(cellRowList);
					}
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}

				File file = new File(filename + ".txt");
				PrintStream stream = new PrintStream(file);
				for (int i = 0; i < cellGrid.size(); i++) {
					List<HSSFCell> cellRowList = cellGrid.get(i);
					for (int j = 0; j < cellRowList.size(); j++) {
						HSSFCell myCell = (HSSFCell) cellRowList.get(j);
						String stringCellValue = myCell.toString();
						stream.print(stringCellValue + "\t");
					}
					stream.println("");
				}
			}
		}

	}

	public static void main(String[] args) throws IOException {
		// TODO code application logic here
		Main_Convertxlstotext Main_Convertxlstotext = new Main_Convertxlstotext();
	}

}
