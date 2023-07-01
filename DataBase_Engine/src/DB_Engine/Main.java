package DB_Engine;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.Date;
import java.util.Hashtable;
import java.util.ArrayList;

import DBAppException.DBAppException;

public class Main {

	public static void main(String[] args)
			throws DBAppException, ClassNotFoundException, InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, IOException {
		DBApp dbApp = new DBApp();
		dbApp.init();
		Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("ProductID", "java.lang.Integer");
		htblColNameType.put("ProductName", "java.lang.String");
		htblColNameType.put("ProductPrice", "java.lang.Double");

		Hashtable<String, String> htblColNameMin = new Hashtable<String, String>();
		htblColNameMin.put("ProductID", "0");
		htblColNameMin.put("ProductName", "A");
		htblColNameMin.put("ProductPrice", "0");

		Hashtable<String, String> htblColNameMax = new Hashtable<String, String>();
		htblColNameMax.put("ProductID", "1000");
		htblColNameMax.put("ProductName", "ZZZZZZZZZZZ");
		htblColNameMax.put("ProductPrice", "100000");

		Hashtable<String, String> htblForeignKeys = new Hashtable<String, String>();
//		htblForeignKeys.put("ProductID", 1000);
//		htblForeignKeys.put("ProductName", "ZZZZZZZ");
//		htblForeignKeys.put("ProductPrice", 100000);
		String[] computed = {};

		dbApp.createTable("Product", "ProductID", htblColNameType, htblColNameMin, htblColNameMax, htblForeignKeys,
				computed);

		htblColNameType.clear();
		htblColNameType = new Hashtable<String, String>();
		htblColNameType.put("SaleID", "java.lang.Integer");
		htblColNameType.put("SaleDate", "java.util.Date");
		htblColNameType.put("ProductID", "java.lang.Integer");
		htblColNameType.put("Quantity", "java.lang.Integer");
		htblColNameType.put("TotalAmount", "java.lang.Double");

		htblColNameMin.clear();
		htblColNameMin = new Hashtable<String, String>();
		htblColNameMin.put("SaleID", "0");
		htblColNameMin.put("SaleDate", "01.01.2000");
		htblColNameMin.put("ProductID", "0");
		htblColNameMin.put("Quantity", "1");
		htblColNameMin.put("TotalAmount", "0");

		htblColNameMax.clear();
		htblColNameMax = new Hashtable<String, String>();
		htblColNameMax.put("SaleID", "1000");
		htblColNameMax.put("SaleDate", "31.12.2030");
		htblColNameMax.put("ProductID", "1000");
		htblColNameMax.put("Quantity", "10000");
		htblColNameMax.put("TotalAmount", "1000000000");
		String[] computed_1 = { "TotalAmount" };
		System.out.println("Test:"+htblColNameMax.get("Quantity"));
		

		htblForeignKeys.put("ProductID", "Product.ProductID");

		dbApp.createTable("Sale", "SaleID", htblColNameType, htblColNameMin, htblColNameMax, htblForeignKeys,
				computed_1);
		;

//		for (String[] array : dbApp.getMetaData().LoadMetaData()) {
//			System.out.println(Arrays.toString(array));
//		}
//		
		
		// Object o=new Object();
		// o=3;
		// System.out.println((o instanceof java.lang.Integer));

		// System.out.println(dbApp.getMetaData().LoadMetaData().get(0).toString());
		
		
		
		
		Hashtable<String,Object> htblColNameValue = new Hashtable<String,Object>();
		

		htblColNameValue.clear();
		htblColNameValue.put("ProductID", new Integer(2));
		htblColNameValue.put("ProductName", new String("Mobile Phone"));
		htblColNameValue.put("ProductPrice", new Double(299));


		
		htblColNameValue.clear();
		htblColNameValue.put("ProductID", new Integer(3));
		htblColNameValue.put("ProductName", new String("TV"));
		htblColNameValue.put("ProductPrice", new Double(499));
		dbApp.insertIntoTable("Product", htblColNameValue);
		
		htblColNameValue.clear();
		htblColNameValue.put("ProductID", new Integer(1));
		htblColNameValue.put("ProductName", new String("PC"));
		htblColNameValue.put("ProductPrice", new Double(999));
		dbApp.insertIntoTable("Product", htblColNameValue);
		
		
		
		
		htblColNameValue.clear();
		htblColNameValue.put("ProductID", new Integer(4));
		htblColNameValue.put("ProductName", new String("MP3"));
		htblColNameValue.put("ProductPrice", new Double(999));

		dbApp.insertIntoTable("Product", htblColNameValue);

		htblColNameValue.clear( );
		htblColNameValue.put("SaleID", new Integer( 2)); 
		htblColNameValue.put("SaleDate", new Date(2012,12,8));
		htblColNameValue.put("ProductID", new Integer( 3 ) );
		htblColNameValue.put("Quantity", new Integer( 500) );

		// Mobile Phone // add the date and the quantity too
		dbApp.insertIntoTable( "Sale" , htblColNameValue );

		
		
		htblColNameValue.clear( );
		htblColNameValue.put("SaleID", new Integer( 1 )); 
		htblColNameValue.put("SaleDate", new Date(2010,2,1));
		
		htblColNameValue.put("Quantity", new Integer( 288) );
		htblColNameValue.put("ProductID", new Integer(4) );

		// Mobile Phone // add the date and the quantity too

		dbApp.insertIntoTable( "Sale" , htblColNameValue );
//		
//		htblColNameValue.clear();
////		htblColNameValue.put("roductPrice", new String("hehe"));
////		htblColNameValue.put("ProductPrice", new Double(2));
//		htblColNameValue.put("ProductPrice",new Double(999));
//		htblColNameValue.put("ProductName", new String("MP3"));
		
//		dbApp.updateTable("Sale", "2", htblColNameValue);
//		dbApp.deleteFromTable("Product", htblColNameValue);
		String [] indexcol = {"Quantity","TotalAmount"};


		
		for (String[] array : Page.ReadfromCsvFile("1")) {
			System.out.println(Arrays.toString(array));
		}
		for (Object item: dbApp.MEM)
		{
			System.out.println(item);
		}
		



	}

}
