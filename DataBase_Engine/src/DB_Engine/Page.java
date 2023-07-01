package DB_Engine;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.io.File;
import java.io.FileWriter;


import DBAppException.DBAppException;

import java.io.FileWriter;

public class Page  implements Serializable{

	String csvFileName;
	ArrayList<String[]> sortingTuples;
	final static int maxTuples = 3;
	private int  numberOfTuples = 0;
	private   int pagenumnber = 0;
	private static  int pagesCreated = 0;

	public  int getPagenumnber() {
		return pagenumnber;
	}

	public  void setPagenumnber(int pagenumnber) {
		this.pagenumnber = pagenumnber;
	}

	

	public Page() {
		this.pagenumnber=pagesCreated;
		this.sortingTuples= new ArrayList<>()
				;
		

		String csvFilePath = "./src/fileSystem/Page"
				+ pagenumnber + ".csv";
		pagesCreated++;
		try {
			FileWriter writer = new FileWriter(csvFilePath);
			System.out.println("hello");
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	
	public void writeIntoPage(String row,String path ) throws DBAppException {



			try (BufferedWriter writer = new BufferedWriter(new FileWriter(path, true))) {
				// Write header
//				Iterator<String> iterator = row.iterator();
				/*
				 * while (iterator.hasNext()) { String row = iterator.next(); writer.newLine();
				 * writer.write(row); writer.newLine();
				 * 
				 * 
				 * System.out.println("Data has been written to the CSV file."); }
				 */
				
				

	            // Append the row to the CSV file
	            writer.write(row);
	            writer.newLine(); // Add a new line after the row
                this.numberOfTuples++;
	            writer.close();
	         
				System.out.println("Data has been written to the CSV file.");

			}
			 catch (IOException e) {
				e.printStackTrace();
				throw new DBAppException("Error when writing tuple into Page");
			}
		}
	
//	public static void clearCSVFile(String filePath) {
//	    try {
//	        File file = new File(filePath);
//	        file.createNewFile();
//	        
//	        FileWriter writer = new FileWriter(file);
//	        writer.close();
//	        
//	        System.out.println("CSV file cleared successfully.");
//	    } catch (IOException e) {
//	        e.printStackTrace();
//	    }
//	}
	
	public static void clearCSVFile(String filePath) {
		 try {
	            // Open the CSV file in write mode and truncate its content
	            FileWriter writer = new FileWriter(filePath, false);
	            writer.write(""); // Truncate the file
	            writer.close();

	            System.out.println("All records deleted successfully from the CSV file.");
	        } catch (IOException e) {
	            e.printStackTrace();
	        }
	}
//	
	
//	public static void Savep (Page p)
//	{
//		String filePath = "./src/fileSystem/page"
//				+p.pagenumnber  + ".ser";
//		try {
//			FileOutputStream fileOut = new FileOutputStream(filePath);
//			ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);
//
//			objectOut.writeObject(p); // Serialize and write the object to the file
//
//			objectOut.close();
//			fileOut.close();
//
//			System.out.println("Table deserialized and loaded successfully.");
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//	}
//	public static Page loadP (String Pagenummber) 
//	{
//		String filePath = "./Users/marinaelwy/eclipse-workspace2/DataBase2_Engine_Project/src/fileSystem/page"
//				+Pagenummber+ ".ser";
//		Page p =null;
//		try {
//			FileInputStream fileIn = new FileInputStream(filePath);
//			ObjectInputStream objectIn = new ObjectInputStream(fileIn);
//
//			p = (Page) objectIn.readObject(); // Deserialize and read the object from the file
//
//			objectIn.close();
//			fileIn.close();
//
//			System.out.println("Object loaded successfully.");
//
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		return p;
//	}
	
	
	
	public static void Save_Page(Page p)
	{
		String filePath = "./src/fileSystem/Page"
				+p.pagenumnber  + ".ser";
		try {
			FileOutputStream fileOut = new FileOutputStream(filePath);
			ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);

			objectOut.writeObject(p); // Serialize and write the object to the file

			objectOut.close();
			fileOut.close();

			System.out.println("Table deserialized and loaded successfully.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	public static Page load_Page (String Pagenumber) 
	{
		String filePath = ".src/fileSystem/Page"
				+Pagenumber+ ".ser";
		Page p =null;
		try {
			FileInputStream fileIn = new FileInputStream(filePath);
			ObjectInputStream objectIn = new ObjectInputStream(fileIn);

			p = (Page) objectIn.readObject(); // Deserialize and read the object from the file

			objectIn.close();
			fileIn.close();

			System.out.println("Object loaded successfully.");

		} catch (Exception e) {
			e.printStackTrace();
		}
		return p;
	}
	

//	public void loadIntoCSVFile(String pageName) throws IOException {
//		BufferedReader br = new BufferedReader(new FileReader("/DataBase2_Engine_Project/src/fileSystem/page"+pageName+".csv"));
//		String line = br.readLine();
//		int i = 0;
//		while (line!=null) {
//			String[] content = line.split(",");
//
//			line = br.readLine();
//			
//	}
//		br.close();
//		
//	}
	public static ArrayList<String[]> ReadfromCsvFile(String pageNumber) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(
				"./src/fileSystem/Page"
						+ pageNumber + ".csv"));
		  String line;
		ArrayList<String[]> Page = new ArrayList<>();
		 while ((line = br.readLine()) != null) {
			 String[] data = line.split(",");
		      
		        Page.add(data);	 
		 }
			return Page;
		 }
//
//	public ArrayList<String> getTuples() {
//		return tuples;
//	}
//
//	public void setTuples(ArrayList<String> tuples) {
//		this.tuples = tuples;
//	}

	public String getCsvFileName() {
		return csvFileName;
	}

	public void setCsvFileName(String csvFileName) {
		this.csvFileName = csvFileName;
	}

	public  int getNumberOfTuples() {
		return numberOfTuples;
	}

	public  void setNumberOfTuples(int numberOfTuples) {
		this.numberOfTuples = numberOfTuples;
	}

	public static int getMaxtuples() {
		return maxTuples;
	}

	public ArrayList<String[]> getSortingTuples() {
		return sortingTuples;
	}

	public void setSortingTuples(ArrayList<String[]> sortingTuples) {
		this.sortingTuples = sortingTuples;
	}

	public static int getPagesCreated() {
		return pagesCreated;
	}

	public static void setPagesCreated(int pagesCreated) {
		Page.pagesCreated = pagesCreated;
	}

	

}