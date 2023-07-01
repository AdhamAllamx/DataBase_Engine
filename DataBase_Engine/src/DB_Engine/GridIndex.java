package DB_Engine;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

public class GridIndex  {
	
	
	private String colName1 ;
	private String tableName ;
	private String colName2;
	private Object [][] index ; 
	
	
	public GridIndex (String colName1 , String colName2 ,String tableName, int max1 , int max2) {
		this.colName1 = colName1;
		this.tableName= tableName;
		this.colName2 = colName2;
		this.index = new Object [max1][max2];
		
		
	}
	

	public static void save_GridIndex_into_Disk(GridIndex grid) {

		String filePath = "./src/fileSystem/GridIndex_"
				+ grid.tableName + ".ser";
		try {
			FileOutputStream fileOut = new FileOutputStream(filePath);
			ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);

			objectOut.writeObject(grid); // Serialize and write the object to the file

			objectOut.close();
			fileOut.close();

			System.out.println("Table deserialized and loaded successfully.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public static GridIndex load_Table(String tableName) {
		String filePath = "./src/fileSystem/GridIndex_"
				+ tableName + ".ser";
		GridIndex GridIndex = null;
		try {
			FileInputStream fileIn = new FileInputStream(filePath);
			ObjectInputStream objectIn = new ObjectInputStream(fileIn);

			GridIndex = (GridIndex) objectIn.readObject(); // Deserialize and read the object from the file

			objectIn.close();
			fileIn.close();

			System.out.println("Object loaded successfully.");

		} catch (Exception e) {
			e.printStackTrace();
		}
		return GridIndex;
	}


	public String getColName1() {
		return colName1;
	}


	public void setColName1(String colName1) {
		this.colName1 = colName1;
	}


	public String getColName2() {
		return colName2;
	}


	public void setColName2(String colName2) {
		this.colName2 = colName2;
	}


	public Object[][] getIndex() {
		return index;
	}


	public void setIndex(Object[][] index) {
		this.index = index;
	}

	
	
	
}
