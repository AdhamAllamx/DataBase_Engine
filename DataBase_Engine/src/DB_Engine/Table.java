package DB_Engine;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Set;

public class Table implements Serializable {

	LinkedList<Page> Pages;
	ArrayList <String> PagesNo;
	
	String[] filePath;
	String strTableName;
	Hashtable<String, String> htblColNameType;
	Hashtable<String, String> htblColNameMin;
	Hashtable<String, String> htblColNameMax;
	Hashtable<String, String> htblForeignKeys;
	String[] computedCols;
	String strClusteringKeyColumn;
	String indexName;
	String indexType;
	ArrayList<String> headers ; 
	

	public Table(String strTableName, String strClusteringKeyColumn, Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameMin, Hashtable<String, String> htblColNameMax,
			Hashtable<String, String> htblForeignKeys, String[] computedCols) {
		
		Pages = new LinkedList<Page>();
		PagesNo=new ArrayList<String>();
		headers= new ArrayList<String>();
		Page p = new Page();
		String page_num =""+p.getPagenumnber();
		PagesNo.add(page_num);
		Pages.add(p);
		Set<String> set_type = htblColNameType.keySet();
		Iterator<String> itr = set_type.iterator();
		int i = 0;

		while(itr.hasNext()){
			String key =  itr.next();
			headers.add(key);
			i++;
		}

		for(String s : headers) {
			System.out.print(s+",");
			
		}
		System.out.println();
		
		
		

		this.filePath = filePath;
		this.strClusteringKeyColumn = strClusteringKeyColumn;
		this.strTableName = strTableName;
		this.htblColNameType = htblColNameType;
		this.htblColNameMin = htblColNameMin;
		this.htblColNameMax = htblColNameMax;
		this.htblForeignKeys = htblForeignKeys;
		this.computedCols = computedCols;
		this.indexName = null;
		this.indexType = null;
	}

	public static void save_Table(Table table) {

		String filePath = "./src/fileSystem/Table_"
				+ table.strTableName + ".ser";
		try {
			FileOutputStream fileOut = new FileOutputStream(filePath);
			ObjectOutputStream objectOut = new ObjectOutputStream(fileOut);

			objectOut.writeObject(table); // Serialize and write the object to the file

			objectOut.close();
			fileOut.close();

			System.out.println("Table deserialized and loaded successfully.");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	public static Table load_Table(String tableName) {
		String filePath = "./src/fileSystem/Table_"
				+ tableName + ".ser";
		Table table = null;
		try {
			FileInputStream fileIn = new FileInputStream(filePath);
			ObjectInputStream objectIn = new ObjectInputStream(fileIn);

			table = (Table) objectIn.readObject(); // Deserialize and read the object from the file

			objectIn.close();
			fileIn.close();

			System.out.println("Object loaded successfully.");

		} catch (Exception e) {
			e.printStackTrace();
		}
		return table;
	}

	public ArrayList<String> getPagesNo() {
		return PagesNo;
	}

	public void setPagesNo(ArrayList<String> pagesNo) {
		PagesNo = pagesNo;
	}

	@Override
	public String toString() {
		// TODO Auto-generated method stub
		return  strTableName+", ";
	}
}