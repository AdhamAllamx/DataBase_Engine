
package DB_Engine;

import DBAppException.DBAppException;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.Set;

public class Meta_Data_File {

	private static ArrayList<String> rows_MetaData;
	private static String metaDataName;

	public Meta_Data_File() {
		this.rows_MetaData = new ArrayList<>();
		this.metaDataName ="metaData";
	}

//	
//	
//public static void writeMetaData(Table T) {
//	String csvFile = "path/to/your/csv/file.csv";
//
//    try (CSVWriter writer = new CSVWriter(new FileWriter(csvFile))) {
//    	
//    	Enumeration<String> keys = T.htblColNameType.keys();
//    	while (keys.hasMoreElements()) {
//    	    String key = keys.nextElement();
//    	    String value = T.htblColNameType.get(key);
//    	    String[] row1 = { T.strTableName,key,value,T.strClusteringKeyColumn.equals(key)+"",};
//            writer.writeNext(row1);
//
//    	   
//     	}
//
//
//        String[] header = { "TableName", "ColumnName", "ColumnType", "ClusteringKey", "IndexName", "IndexType", "min", "max", "ForeignKey", "ForeignTableName", "ForeignColumnName", "Computed" };
//        writer.writeNext(header);
//
//        String[] row1 = { "John Doe", "25", "New York" };
//        writer.writeNext(row1);
//
//        String[] row2 = { "Jane Smith", "30", "London" };
//        writer.writeNext(row2);
//
//        System.out.println("CSV file created successfully.");
//    } catch (IOException e) {
//        e.printStackTrace();
//    }
//
//	
//}

	public String getMetaDataName() {
		return metaDataName;
	}

	public void setMetaDataName(String metaDataName) {
		this.metaDataName = metaDataName;
	}

	public static ArrayList<String> getRows_MetaData() {
		return rows_MetaData;
	}

	public static void writeMetaData(ArrayList<String> rows_MetaData) throws DBAppException {

//	 List<String> rows_MetaData = List.of("Row 1", "Row 2", "Row 3"); // Example data

//     
//     try (FileOutputStream output = new FileOutputStream("./src/Disk/metaData.csv", true)) {
//         Iterator<String> iterator = rows_MetaData.iterator();
//         while (iterator.hasNext()) {
//             String row = iterator.next();
//             byte[] data = (row + "\n").getBytes(); // Add newline character after each row
//
//             output.write(data);
//         }
//     } catch (IOException e) {
//         e.printStackTrace();
//         throw new DBAppException("Error when writing metadata");}
//     }

		try (BufferedWriter writer = new BufferedWriter(new FileWriter("./src/fileSystem/metaData.csv"))) {
			// Write header
			Iterator<String> iterator = rows_MetaData.iterator();
			/*
			 * while (iterator.hasNext()) { String row = iterator.next(); writer.newLine();
			 * writer.write(row); writer.newLine();
			 * 
			 * 
			 * System.out.println("Data has been written to the CSV file."); }
			 */
			for (String row : rows_MetaData) {
				writer.write(row);
//                writer.newLine();
			}
			System.out.println("Data has been written to the CSV file.");
		} catch (IOException e) {
			e.printStackTrace();
			throw new DBAppException("Error when writing metadata");
		}
	}

	public ArrayList<String[]> LoadMetaData() {
		ArrayList<String[]> metadata = new ArrayList<>();
		try (BufferedReader br = new BufferedReader(new FileReader("./src/fileSystem/metaData.csv"))) {
			String line;

			while ((line = br.readLine()) != null) {
				metadata.add(line.split(","));// Split the line into an array of values using the comma as the separator

			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		return metadata;

	}

	public static void setRows_MetaData(ArrayList<String> rows_MetaData) {
		Meta_Data_File.rows_MetaData = rows_MetaData;

	}

}
