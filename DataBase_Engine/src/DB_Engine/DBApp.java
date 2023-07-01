package DB_Engine;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.LinkedHashMap;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import DBAppException.DBAppException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class DBApp {
	ArrayList<Object> MEM;

	private Meta_Data_File metaData;

	public Meta_Data_File getMetaData() {
		return metaData;
	}

	public void setMetaData(Meta_Data_File metaData) {
		this.metaData = metaData;
	}

	public DBApp() {
		MEM = new ArrayList<>();

	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType, Hashtable<String, String> htblColNameMin,
			Hashtable<String, String> htblColNameMax, Hashtable<String, String> htblForeignKeys, String[] computedCols)
			throws DBAppException {

		Table Table = new Table(strTableName, strClusteringKeyColumn, htblColNameType, htblColNameMin, htblColNameMax,
				htblForeignKeys, computedCols);
		Set<String> set_type = htblColNameType.keySet();
		Iterator<String> itr_type = set_type.iterator();
		

		while (itr_type.hasNext()) {
			String key = itr_type.next();
			String rec;
			boolean foreignkey = false;
			boolean computed;
			String[] parts = new String[2];
			if (Table.htblForeignKeys.get(key) != null) {
				parts = htblForeignKeys.get(key).split("\\.");
				foreignkey = true;
			}
			if (Table.computedCols.length != 0 && key.equals(computedCols[0])) {
				computed = true;
			} else {
				computed = false;
			}

			if (key.equals(Table.strClusteringKeyColumn)) {
				rec = Table.strTableName + "," + Table.strClusteringKeyColumn + ","
						+ Table.htblColNameType.get(key).toString() + "," + "True" + "," + Table.indexName + ","
						+ Table.indexType + "," + Table.htblColNameMin.get(key).toString() + ","
						+ Table.htblColNameMax.get(key).toString() + "," + foreignkey + "," + parts[0] + "," + parts[1]
						+ "," + computed + "\n";
				Meta_Data_File.getRows_MetaData().add(rec);
			} else {
				rec = Table.strTableName + "," + key.toString() + "," + Table.htblColNameType.get(key).toString() + ","
						+ "False" + "," + Table.indexName + "," + Table.indexType + ","
						+ Table.htblColNameMin.get(key).toString() + "," + Table.htblColNameMax.get(key).toString()
						+ "," + foreignkey + "," + parts[0] + "," + parts[1] + "," + computed + "\n";
				Meta_Data_File.getRows_MetaData().add(rec);
			}
		}
		metaData.writeMetaData(Meta_Data_File.getRows_MetaData());
		Table.save_Table(Table);
		MEM.add(Table);
	}

	public void init() {
		metaData = new Meta_Data_File();

		// this does whatever initialization you would like //
	}

	public void clearCSVFile(String filePath) {
		try {
			File file = new File(filePath);
			file.createNewFile();

			FileWriter writer = new FileWriter(file);
			writer.close();

			System.out.println("CSV file cleared successfully.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	// or leave it empty if there is no code you want to //
	// execute at application startup
	// following method creates one table only
	// strClusteringKeyColumn is the name of the column that will be the primary
	// key and the clustering column as well. The data type of that column will
	// be passed in htblColNameType
	// htblColNameValue will have the column name as key and the data
	// type as value
	// htblColNameMin and htblColNameMax for passing minimum and maximum values
	// for data in the column. Key is the name of the column
	// htblForeignKeys for specifying which column is a foreign key
	// the key is the ColumnName to set as foreign key, value is a string specifying
	// the referenced key in format “TableName.ColumnName”
	// computedCols is an array of Strings that has the column names that their
	// value is computed

	// following method creates a grid index
	// If two column names are passed, create a grid index.
	// If only one or more than 2 column names are passed, throw an Exception.
	public void createIndex(String strTableName, String[] strarrColName) throws DBAppException {
		
		if(strarrColName[0].equals(strarrColName[1])) {
			throw new DBAppException("Column Name Already inserted for Grid Index !");
			
		}
		else if(strarrColName.length==1) {
			throw new DBAppException("Please enter two Column Name for Grid Index !");

		}
		else if(strarrColName.length>2) {
			throw new DBAppException("Please enter only two Column Name for Grid Index !");

		}
		else {
			int max_column1 = 0 ;
			int max_column2 = 0 ;
			int min_column1= 0 ;
			int min_column2 = 0;
			
			
			ArrayList<String[]> meta = metaData.LoadMetaData();
			String[][] meta_2d = new String[meta.size()][];
		    for (int i = 0; i < meta.size(); i++) {
		            String[] curr = meta.get(i);
		            meta_2d[i] = new String[curr.length];
		            System.arraycopy(curr, 0, meta_2d[i], 0, curr.length);
		     } 
		    int index_col1_row = 0;
		    int index_col2_row= 0;
		     String [] updated_col1 = new String [meta_2d[1].length];
		     String [] updated_col2 = new String [meta_2d[1].length];
		     

			boolean flag1  = false ;
			boolean flag2 = false ;
			
		    for(int i = 0 ; i < meta_2d.length ;i++) {
		    	for(int j = 0 ; j< meta_2d[i].length;j++) {
		    		   if(strTableName.equals(meta_2d[i][0])) {
		    			     if(flag1 ==true && flag2==true) {
		    			    	 break;
		    			     }
							if(strarrColName[0].equals(meta_2d[i][1])&&flag1==false) {
								
								max_column1 = Integer.parseInt(meta_2d[i][7]);
								index_col1_row = i;
								for(int k =0; k < meta_2d[i].length;k++) {
									updated_col1[k] = meta_2d[i][k];
								}
								flag1 = true;
								
								
							}
							if(strarrColName[1].equals(meta_2d[i][1])&& flag2 ==false) {
								max_column2 = Integer.parseInt(meta_2d[i][7]);
								index_col2_row = i;
								for(int k =0; k < meta_2d[i].length;k++) {
									updated_col2[k] = meta_2d[i][k];
								}
								flag2 = true;
								
							}				
							
						}
		    	}
		    	
		    }
		    
		    updated_col1[4]= strarrColName[0]+"."+strarrColName[1]+"."+"GridIndex";
		    updated_col1[5]= "Grid";
		    updated_col2[4]= strarrColName[0]+"."+strarrColName[1]+"."+"GridIndex";
		    updated_col2[5]= "Grid";

		    
		    for(int i =0 ;i<meta_2d.length;i++) {
		    	if(i==index_col1_row) {
		    		meta_2d[i] = updated_col1;
		    		for(int j = 0 ; j<meta_2d[i].length;j++) {
		    			System.out.print(meta_2d[i][j]+",,,,,,");
		    		}
		    	}
		    	if(i== index_col2_row) {
		    		meta_2d[i] = updated_col2;
		    		for(int j = 0 ; j<meta_2d[i].length;j++) {
		    			System.out.print(meta_2d[i][j]+",,,,,,");
		    		}
		    	}
		    }
		    ArrayList<String> meta_updated = new ArrayList<>();
		    for (String[] row : meta_2d) {
	            String rowcsv = String.join(",", row);
	            meta_updated.add(rowcsv);
	        }
		    
//		    for(int i = 0 ; i<meta_2d.length;i++) {
//		    	for(int j = 0 ;j<meta_2d[i].length;j++){
//		    		System.out.print(meta_2d[i][j]);
//		    		
//		    	}
//		    	System.out.println(f);
//		    }
	        
		    for(String row :meta_updated)
		    {
		    	System.out.println("row metadata updatedd " +row);
		    }
		    System.out.println(meta_updated.size());
		    
		    
		    clearCSVFile("./src/fileSystem/metaData.csv");
		    
		    
		    Meta_Data_File.writeMetaData(meta_updated);
		 
			

			
			
//			GridIndex grid_index = new GridIndex(strarrColName[0],strarrColName[1],strTableName,max_column1, max_column2);
			
			

		}
		

	}

	// following method inserts one row only.
	// htblColNameValue must include a value for the primary key
	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, ClassNotFoundException, InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, IOException {
		Set<String> set_type = htblColNameValue.keySet();
		Iterator<String> itr = set_type.iterator();

		// Table t = Table.loadT(strTableName);
		// get metadata file find the clustering key and sort on it and then check the
		// type of inserted object and then add it to the page in csv format
		ArrayList<String[]> meta = metaData.LoadMetaData();
		String row = "";
		boolean compflag = false;

		while (itr.hasNext()) {
			String key = itr.next();

			Object o = htblColNameValue.get(key);
			for (String[] data : meta) {
				String strColType = data[2];
				String strColMin = data[6];
				String strColMax = data[7];
				Object Max;
				Object Min;

//				System.out.println("data [7]: " + data[8]);

				if (strColType.equals("java.util.Date")) {
					String[] minDate = new String[3];
					minDate = strColMin.split("\\.");
					String[] maxDate = new String[3];
					maxDate = strColMax.split("\\.");

					int[] minDateInt = new int[3];
					int[] maxDateInt = new int[3];

					for (int i = 0; i < minDate.length; i++) {
						minDateInt[i] = Integer.parseInt(minDate[i]);
						maxDateInt[i] = Integer.parseInt(maxDate[i]);
					}
					Min = (Date) new Date(minDateInt[2], minDateInt[1], minDateInt[0]);
					System.out.println(Min);
					Max = (Date) new Date(maxDateInt[2], maxDateInt[1], maxDateInt[0]);
					System.out.println(Max);

				} else {
					Class<?> ColType = Class.forName(strColType);
					Constructor<?> constructor = ColType.getConstructor(String.class); // Assuming the constructor takes
																						// a
																						// String parameter
					Min = constructor.newInstance(strColMin);
					Max = constructor.newInstance(strColMax);
				}

				if (data[0].equals(strTableName)) {

//					String Type = htblColNameValue.get(key).getClass().getName().toString();
					Comparable compMin = null;
					Comparable compMax = null;
					Comparable compvalue = null;
					if (o instanceof Integer) {
						Integer value = (Integer) o;
						if (value instanceof Comparable) {
							compvalue = (Comparable) value;
						}

					}

					else if (o instanceof String) {
						String value = (String) o;
						if (value instanceof Comparable) {
							compvalue = (Comparable) value;
						}

					} else if (o instanceof Double) {
						Double value = (Double) o;
						if (value instanceof Comparable) {
							compvalue = (Comparable) value;
						}

					} else if (o instanceof Date) {
						Date value = (Date) o;
						if (value instanceof Comparable) {
							compvalue = (Comparable) value;
						}

					}

					if (Min instanceof Comparable && Max instanceof Comparable) {

						compMin = (Comparable) Min;
						compMax = (Comparable) Max;
					}
					String quantity = "1";
					if (data[11].equals("true")) {
						System.out.println("data[11] is true");
					}
					if (key.equals(data[1])) {
						System.out.println(" key.equals(data[1])");
					}
					if (compflag == false) {
						System.out.println("compflag =false");
					}
					if (data[8].equals("true") && data[1].equals(key)) {
						String forTableName = data[9];
						String forColName = data[1];
						Table t;
						boolean foundForKey = false;
						for (Object item : MEM) {
							if (item instanceof Table) {
								t = (Table) item;
								System.out.println("table found in mem");
								if (t.strTableName.equals(forTableName)) {
									System.out.println("productid table");

									for (int i = 0; i < t.Pages.size(); i++) {
										String number = t.Pages.get(i).getPagenumnber() + "";
										ArrayList<String[]> tuples_csv = Page.ReadfromCsvFile(number);
										// MEM.add(tuples_csv);

										for (String[] s : tuples_csv) {
											System.out.println("S[0]: " + s[0]);
											System.out.println("valueeeeeeeeeeeeeeeeeeeeeeeeeeeee:"
													+ htblColNameValue.get(key).toString());
											if (s[0].equals(htblColNameValue.get(key).toString())) {
												foundForKey = true;

											}

										}

									}
									if (foundForKey == false) {
										throw new DBAppException("Invalid Foreign Key !!");
									}

								}

							}
						}

					}
					if (data[11].equals("true") && compflag == false) {

						System.out.println("key here : " + key);

						quantity = htblColNameValue.get("Quantity").toString();
						System.out.println("quan: " + htblColNameValue.get("Quantity").toString());

						for (String[] meta2 : meta) {

							if (meta2[8].equals("true")) {

								int comp = 0;

								Set<String> set_type1 = htblColNameValue.keySet();
								Iterator<String> itr1 = set_type1.iterator();
								String clustering_Value = "";
								ArrayList<String[]> meta_data = metaData.LoadMetaData();
								String forTableName = "";
								String forColName = "";

								while (itr1.hasNext()) {
									String key1 = itr1.next();
									for (String[] row_meta : meta_data) {
										if (row_meta[0].equals(strTableName) && row_meta[8].equals("true")) {
											forTableName = row_meta[9];
											System.out.println("fortable name :" + forTableName);

											forColName = row_meta[10];
											System.out.println("fortable name :" + forColName);
											break;

										}
									}
									if (key1.equals(forColName)) {
										clustering_Value = (String) htblColNameValue.get(key1).toString();
										break;
									}

								}

								Table t;
								String price = "1";
								for (Object item : MEM) {
									if (item instanceof Table) {
										t = (Table) item;
										System.out.println("table found in mem");
										if (t.strTableName.equals(forTableName)) {
											System.out.println("productid table");

											for (int i = 0; i < t.Pages.size(); i++) {
												String number = "" + t.Pages.get(i).getPagenumnber();
												ArrayList<String[]> tuples_csv = Page.ReadfromCsvFile(number);
												// MEM.add(tuples_csv);
												for (String[] s : tuples_csv) {
													if (s[0].equals(clustering_Value)) {
														System.out.println("updated price :" + s[1]);

														price = s[1];

													}

												}

											}

										}

									}
								}
								double total_amount = Integer.parseInt(quantity) * Double.parseDouble(price);
								System.out.println("totalamoount  : " + total_amount);
								row += total_amount + ",";
								compflag = true;
								break;
							}
						}
					}

					if (key.equals(data[1])) {
						System.out.println("key in data in data [1] : " + key + "data [1] : +" + data[1]);
						if (data[2].equals(htblColNameValue.get(key).getClass().getName())) {

							if (compvalue.compareTo(compMin) >= 0 && compvalue.compareTo(compMax) <= 0) {
								if (compvalue instanceof Date) {
									Date d = (Date) compvalue;
									String date = "";

									int month = 0;
									if (d.getMonth() == 0) {
										month = 12;
										System.out.println(
												"Dateeeeeee " + d.getDate() + "." + month + "." + (d.getYear() - 1));
										date = "" + d.getDate() + "." + month + "." + (d.getYear() - 1);

									} else {
										System.out.println("Dateeeeeee " + d.getDate() + "." + (d.getMonth()) + "."
												+ (d.getYear()));
										date = "" + d.getDate() + "." + (d.getMonth()) + "." + (d.getYear());

									}
									row += date;

								} else {

									row += htblColNameValue.get(key).toString();
								}

								if (itr.hasNext()) {
									row += ",";
								}
							} else
								throw new DBAppException("Value Out Of Allowed Range");
						} else
							throw new DBAppException("Invalid Data Type");

					}

				}

			}

		}

		System.out.println(row);
		Table t = null;

		for (Object item : MEM) {
			if (item instanceof Table) {
				t = (Table) item;

				if (t.strTableName.equals(strTableName)) {
					ArrayList<String[]> tuples_csv = new ArrayList<>();
					ArrayList<String> tuples_page = new ArrayList<String>();

					tuples_csv = getTableRows(t);
					Page p = null;

					if (tuples_csv.size() == 0) {
						tuples_page.add(row);
						p = t.Pages.get(0);
						String filepath = "./src/fileSystem/Page" + (p.getPagenumnber()) + ".csv";
						p.writeIntoPage(row, filepath);
//						p.setNumberOfTuples(p.getNumberOfTuples() + 1);
						MEM.remove(tuples_csv);
					} else {
						String[] rowarr=row.split(",");
						for(String[] s:tuples_csv) {
							if(t.strTableName.equals("Product")) {
								if(s[0].equals(rowarr[0])) {
									throw new DBAppException("Primary key already exists");
								}
							}
							else if(t.strTableName.equals("Sale")) {
								if(s[2].equals(rowarr[2])) {
									throw new DBAppException("Primary key already exists");
								}
							}
						}
						
						
						tuples_csv.add(rowarr);
						if (strTableName.equals("Product")) {
							Collections.sort(tuples_csv, new SortingOnKey(0));

						} else {
							Collections.sort(tuples_csv, new SortingOnKey(2));

						}
						tuples_page.clear();

						for (String[] array : tuples_csv) {
							tuples_page.add(String.join(",", array));
						}
						int j = 0;

//						for (int i = 0; i < t.Pages.size(); i++) {
//							for (int k = 0; k < t.Pages.get(i).maxTuples; k++) {
//								String filepath = "./src/fileSystem/Page" + (t.Pages.get(i).getPagenumnber()) + ".csv";
//								if (j < tuples_page.size()) {
//									t.Pages.get(i).writeIntoPage(tuples_page.get(j), filepath);
//									j++;
//								}
//							}
//						}
						boolean dontEnter = false;

						for (int i = 0; i < t.Pages.size(); i++) {
							System.out.println("maxTuples :" + t.Pages.get(i).maxTuples);
							System.out.println("getNumberOfTuples :" + t.Pages.get(i).getNumberOfTuples());
							while (t.Pages.get(i).getNumberOfTuples() < t.Pages.get(i).maxTuples
									&& t.Pages.get(i).getNumberOfTuples() < tuples_page.size() && j<tuples_page.size()) {
								String filepath = "./src/fileSystem/Page" + (t.Pages.get(i).getPagenumnber()) + ".csv";
								t.Pages.get(i).writeIntoPage(tuples_page.get(j), filepath);
								j++;

							}

						}
						if (j < tuples_page.size()) {
							dontEnter = true;
						}

						if (dontEnter == true) {
							p = new Page();
							t.Pages.add(p);
							String filepath = "./src/fileSystem/Page" + (p.getPagenumnber()) + ".csv";
							p.writeIntoPage(tuples_page.get(j), filepath);
							System.out.println("create and insert one rec into a new page :)");
							dontEnter = false;

						}

//						Page p = t.Pages.get(i);

//						MEM.add(tuples_csv);
////							t.Pages.get(i).getTuples().add(row);
//						
//							
////						p.getSortingTuples().add(row.split(","));
//						Collections.sort(tuples_csv, new SortingOnKey(0));
////						tuples_page.clear();
//						for (String[] array : tuples_csv) {
//							tuples_page.add(String.join(",", array));
//						}

						// sort somehow on key
//						
//						String filepath = "./src/fileSystem/Page" + (p.getPagenumnber()) + ".csv";
//						//p.writeIntoPage(tuples_page, filepath ,t);
//						p.setNumberOfTuples(p.getNumberOfTuples() + 1);
//
//						MEM.remove(tuples_csv);

//						else System.out.println("add page yaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

					}

				}
			}
//				break;

		}
	}

	public ArrayList<String[]> getTableRows(Table t) throws IOException {

		ArrayList<String[]> row_table = new ArrayList<>();
		ArrayList<String[]> row_page = new ArrayList<>();

		for (int i = 0; i < t.Pages.size(); i++) {
			row_page = Page.ReadfromCsvFile("" + t.Pages.get(i).getPagenumnber());

			for (String[] array : row_page) {
				row_table.add(array);
			}
			Page.clearCSVFile("./src/fileSystem/Page" + (t.Pages.get(i).getPagenumnber()) + ".csv");
			t.Pages.get(i).setNumberOfTuples(0);
		}
		return row_table;
	}

	public ArrayList<String[]> getTableRowsWithoutDel(Table t) throws IOException {

		ArrayList<String[]> row_table = new ArrayList<>();
		ArrayList<String[]> row_page = new ArrayList<>();

		for (int i = 0; i < t.Pages.size(); i++) {
			row_page = Page.ReadfromCsvFile("" + t.Pages.get(i).getPagenumnber());

			for (String[] array : row_page) {
				row_table.add(array);
			}
//			Page.clearCSVFile("./src/fileSystem/Page" + (t.Pages.get(i).getPagenumnber()) + ".csv");
//			t.Pages.get(i).setNumberOfTuples(0);
		}
		return row_table;
	}

	// following method updates one row only
	// htblColNameValue holds the key and new value
	// htblColNameValue will not include clustering key as column name
	// strClusteringKeyValue is the value to look for to find the row to update.
	public void updateTable(String strTableName, String strClusteringKeyValue,
			Hashtable<String, Object> htblColNameValue)
			throws DBAppException, IOException, ClassNotFoundException, InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException {
		Hashtable<String, Object> updated_value = new Hashtable<String, Object>();
		for (Object item : MEM) {
			if (item instanceof Table) {
				Table table = (Table) item;

				if (table.strTableName.equals(strTableName)) {
					String clusteringKeyName = "";
					ArrayList<String[]> meta = metaData.LoadMetaData();
					for (int i = 0; i < meta.size(); i++) {
						String[] row_meta = meta.get(i);
						if (row_meta[0].equals(strTableName)) {
							
							if (row_meta[3].equals("True")) {
								clusteringKeyName = row_meta[1];
								break;
							}
						}
					}
					int index = 0;
					for (int j = 0; j < table.headers.size(); j++) {
						if (table.headers.get(j).equals(clusteringKeyName)) {
							index = j;
							break;
						}
					}

					ArrayList<String[]> table_rows = getTableRows(table);
					int index_row_in_table = 0;
					// MEM.add(table_rows);

					for (int j = 0; j < table_rows.size(); j++) {
						String[] row = table_rows.get(j);
						if (row[index].equals(strClusteringKeyValue)) {
							index_row_in_table = j;
							
						}
					}

					String[] desired_row = table_rows.get(index_row_in_table);
					Set<String> set_type = htblColNameValue.keySet();
					Iterator<String> itr = set_type.iterator();
					String data_type = "";

					for (int i = 0; i < desired_row.length; i++) {
						if (set_type.contains(table.headers.get(i))) {
							updated_value.put(table.headers.get(i), htblColNameValue.get(table.headers.get(i)));

						} else {
							ArrayList<String[]> meta_data = metaData.LoadMetaData();
							Object o = null;
							String value = "";

							for (String[] data : meta_data) {
								if (data[0].equals(strTableName)) {
									if (data[1].equals(table.headers.get(i))&& data[11].equals("false")) {

										data_type = data[2];
										value = desired_row[i];
										System.out.println("value of desired :" + value);
										System.out.println("data type :" + data_type);
										switch (data_type) {

										case "java.lang.Integer":
											o = (Integer) Integer.parseInt(value);
											break;
										// case"java.util.Date" : o = (Date) Date.parse(value);break;
										case "java.util.Date":
											String[] Date = new String[3];
											Date = value.split("\\.");
											int[] DateInt = new int[3];
											for (int j = 0; j < Date.length; j++) {
												DateInt[j] = Integer.parseInt(Date[j]);
											}
											o = (Date) new Date(DateInt[2], DateInt[1], DateInt[0]);
											break;

										case "java.lang.Double":
											o = (Double) Double.parseDouble(value);
											break;
										case "java.lang.String":
											o = (String) value;
											break;
										}
										updated_value.put(table.headers.get(i), o);
										break;

									}

								}
							}

//								updated_value.put(table.headers.get(i),o);
						}

					}

					table_rows.remove(index_row_in_table);

					for (Map.Entry<String, Object> entry : updated_value.entrySet()) {
						String key = entry.getKey();
						String value = entry.getValue().toString();
						System.out.println("Key: " + key + ", Value: " + value);
					}

					// htblColNameValue.put(clusteringKeyName, new
					// Integer(Integer.parseInt(strClusteringKeyValue)));
//					this.insertIntoTable(strTableName, updated_value);
					this.UpdateHelper(strTableName, updated_value,table_rows);

				}

			}
		}

	}
	// following method could be used to delete one or more rows.
	// htblColNameValue holds the key and value. This will be used in search
	// to identify which rows/tuples to delete.
	// htblColNameValue enteries are ANDED together
	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException, IOException {
		for (Object item : MEM) {
			if (item instanceof Table) {
				Table table = (Table) item;

				if (table.strTableName.equals(strTableName)) {
					ArrayList<Integer> indexOfColToDel=new ArrayList<Integer> ();
					ArrayList<Integer> rowToRemove=new ArrayList<Integer> ();
					Set<String> set_type = htblColNameValue.keySet();
					Iterator<String> itr = set_type.iterator();
					ArrayList<String[]> table_rows = getTableRows(table);
					
					
					
					
					
					int index=0;
					while (itr.hasNext()) {
						String key = itr.next();

					for (int j = 0; j < table.headers.size(); j++) {
						
					System.out.println("table.headers.get(j), key"+table.headers.get(j)+", "+key);
						if (table.headers.get(j).equals(key)) {
							index = j;
							indexOfColToDel.add(index);
							break;
						}
					}
					
					
					}
					for (Integer element : indexOfColToDel) {
			            System.out.println(element);
			        }
					System.out.println("-------------------------------------------------------------------");
					for(String s:table.headers) {
						
						System.out.print(s+",");
						
					}
					
					System.out.println("htblColNameValue.get(ProductPrice)  "+htblColNameValue.get("ProductPrice")+"-----------");
					int index_row_in_table = 0;
					for (int j = 0; j < table_rows.size(); j++) {
						String[] row = table_rows.get(j);
						Boolean allTrue=false;
						for(int i=0;i<indexOfColToDel.size() ;i++) {
							System.out.println("indexOfColToDel.get(i),i " + indexOfColToDel.get(i)+" , "+i );
							System.out.println("table.headers.get(indexOfColToDel.get(i))"+ table.headers.get(indexOfColToDel.get(i)));
							System.out.println("(row[indexOfColToDel.get(i)],i "+row[indexOfColToDel.get(i)]+ " htblColNameValue.get(table.headers.get(i) " + htblColNameValue.get(table.headers.get(indexOfColToDel.get(i))));
							if(row[indexOfColToDel.get(i)].equals(htblColNameValue.get(table.headers.get(indexOfColToDel.get(i))).toString())) {
								allTrue=true;
							}
							else {allTrue=false;
							break;}
							
						}
						if(allTrue==true) {
							index_row_in_table = j;
							rowToRemove.add(index_row_in_table);
						}
							
						
					}
					
					for(int remove :rowToRemove) {
						table_rows.remove(remove);
					}
					
					
					
					try {
						this.DeleteHelper(strTableName, table_rows);
					} catch (ClassNotFoundException | InstantiationException | IllegalAccessException
							| IllegalArgumentException | InvocationTargetException | NoSuchMethodException
							| SecurityException | DBAppException | IOException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}	
					
				}
				}
	
	}
	}
	
	
	
	

//	public Iterator selectFromTable(SQLTerm[] arrSQLTerms,
//
//			String[] strarrOperators) throws DBAppException {
//
//	}
	
	
	public void UpdateHelper(String strTableName, Hashtable<String, Object> htblColNameValue,ArrayList<String[]> tableTuples )
			throws DBAppException, ClassNotFoundException, InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, IOException {
		Set<String> set_type = htblColNameValue.keySet();
		Iterator<String> itr = set_type.iterator();

		// Table t = Table.loadT(strTableName);
		// get metadata file find the clustering key and sort on it and then check the
		// type of inserted object and then add it to the page in csv format
		ArrayList<String[]> meta = metaData.LoadMetaData();
		String row = "";
		boolean compflag = false;

		while (itr.hasNext()) {
			String key = itr.next();

			Object o = htblColNameValue.get(key);
			for (String[] data : meta) {
				String strColType = data[2];
				String strColMin = data[6];
				String strColMax = data[7];
				Object Max;
				Object Min;

//				System.out.println("data [7]: " + data[8]);

				if (strColType.equals("java.util.Date")) {
					String[] minDate = new String[3];
					minDate = strColMin.split("\\.");
					String[] maxDate = new String[3];
					maxDate = strColMax.split("\\.");

					int[] minDateInt = new int[3];
					int[] maxDateInt = new int[3];

					for (int i = 0; i < minDate.length; i++) {
						minDateInt[i] = Integer.parseInt(minDate[i]);
						maxDateInt[i] = Integer.parseInt(maxDate[i]);
					}
					Min = (Date) new Date(minDateInt[2], minDateInt[1], minDateInt[0]);
					System.out.println(Min);
					Max = (Date) new Date(maxDateInt[2], maxDateInt[1], maxDateInt[0]);
					System.out.println(Max);

				} else {
					Class<?> ColType = Class.forName(strColType);
					Constructor<?> constructor = ColType.getConstructor(String.class); // Assuming the constructor takes
																						// a
																						// String parameter
					Min = constructor.newInstance(strColMin);
					Max = constructor.newInstance(strColMax);
				}

				if (data[0].equals(strTableName)) {

//					String Type = htblColNameValue.get(key).getClass().getName().toString();
					Comparable compMin = null;
					Comparable compMax = null;
					Comparable compvalue = null;
					if (o instanceof Integer) {
						Integer value = (Integer) o;
						if (value instanceof Comparable) {
							compvalue = (Comparable) value;
						}

					}

					else if (o instanceof String) {
						String value = (String) o;
						if (value instanceof Comparable) {
							compvalue = (Comparable) value;
						}

					} else if (o instanceof Double) {
						Double value = (Double) o;
						if (value instanceof Comparable) {
							compvalue = (Comparable) value;
						}

					} else if (o instanceof Date) {
						Date value = (Date) o;
						if (value instanceof Comparable) {
							compvalue = (Comparable) value;
						}

					}

					if (Min instanceof Comparable && Max instanceof Comparable) {

						compMin = (Comparable) Min;
						compMax = (Comparable) Max;
					}
					String quantity = "1";
					if (data[11].equals("true")) {
						System.out.println("data[11] is true");
					}
					if (key.equals(data[1])) {
						System.out.println(" key.equals(data[1])");
					}
					if (compflag == false) {
						System.out.println("compflag =false");
					}
					if (data[8].equals("true") && data[1].equals(key)) {
						String forTableName = data[9];
						String forColName = data[1];
						Table t;
						boolean foundForKey = false;
						for (Object item : MEM) {
							if (item instanceof Table) {
								t = (Table) item;
								System.out.println("table found in mem");
								if (t.strTableName.equals(forTableName)) {
									System.out.println("productid table");

									for (int i = 0; i < t.Pages.size(); i++) {
										String number = t.Pages.get(i).getPagenumnber() + "";
										ArrayList<String[]> tuples_csv = Page.ReadfromCsvFile(number);
										// MEM.add(tuples_csv);

										for (String[] s : tuples_csv) {
											System.out.println("S[0]: " + s[0]);
											System.out.println("valueeeeeeeeeeeeeeeeeeeeeeeeeeeee:"
													+ htblColNameValue.get(key).toString());
											if (s[0].equals(htblColNameValue.get(key).toString())) {
												foundForKey = true;

											}

										}

									}
									if (foundForKey == false) {
										throw new DBAppException("Invalid Foreign Key !!");
									}

								}

							}
						}

					}
					if (data[11].equals("true") && compflag == false) {

						System.out.println("key here : " + key);

						quantity = htblColNameValue.get("Quantity").toString();
						System.out.println("quan: " + htblColNameValue.get("Quantity").toString());

						for (String[] meta2 : meta) {

							if (meta2[8].equals("true")) {

								int comp = 0;

								Set<String> set_type1 = htblColNameValue.keySet();
								Iterator<String> itr1 = set_type1.iterator();
								String clustering_Value = "";
								ArrayList<String[]> meta_data = metaData.LoadMetaData();
								String forTableName = "";
								String forColName = "";

								while (itr1.hasNext()) {
									String key1 = itr1.next();
									for (String[] row_meta : meta_data) {
										if (row_meta[0].equals(strTableName) && row_meta[8].equals("true")) {
											forTableName = row_meta[9];
											System.out.println("fortable name :" + forTableName);

											forColName = row_meta[10];
											System.out.println("fortable name :" + forColName);
											break;

										}
									}
									if (key1.equals(forColName)) {
										clustering_Value = (String) htblColNameValue.get(key1).toString();
										break;
									}

								}

								Table t;
								String price = "1";
								for (Object item : MEM) {
									if (item instanceof Table) {
										t = (Table) item;
										System.out.println("table found in mem");
										if (t.strTableName.equals(forTableName)) {
											System.out.println("productid table");

											for (int i = 0; i < t.Pages.size(); i++) {
												String number = "" + t.Pages.get(i).getPagenumnber();
												ArrayList<String[]> tuples_csv = Page.ReadfromCsvFile(number);
												// MEM.add(tuples_csv);
												for (String[] s : tuples_csv) {
													if (s[0].equals(clustering_Value)) {
														System.out.println("updated price :" + s[1]);

														price = s[1];

													}

												}

											}

										}

									}
								}
								double total_amount = Integer.parseInt(quantity) * Double.parseDouble(price);
								System.out.println("totalamoount  : " + total_amount);
								row += total_amount + ",";
								compflag = true;
								break;
							}
						}
					}

					if (key.equals(data[1])) {
						System.out.println("key in data in data [1] : " + key + "data [1] : +" + data[1]);
						if (data[2].equals(htblColNameValue.get(key).getClass().getName())) {

							if (compvalue.compareTo(compMin) >= 0 && compvalue.compareTo(compMax) <= 0) {
								if (compvalue instanceof Date) {
									Date d = (Date) compvalue;
									String date = "";

									int month = 0;
									if (d.getMonth() == 0) {
										month = 12;
										System.out.println(
												"Dateeeeeee " + d.getDate() + "." + month + "." + (d.getYear() - 1));
										date = "" + d.getDate() + "." + month + "." + (d.getYear() - 1);

									} else {
										System.out.println("Dateeeeeee " + d.getDate() + "." + (d.getMonth()) + "."
												+ (d.getYear()));
										date = "" + d.getDate() + "." + (d.getMonth()) + "." + (d.getYear());

									}
									row += date;

								} else {

									row += htblColNameValue.get(key).toString();
								}

								if (itr.hasNext()) {
									row += ",";
								}
							} else
								throw new DBAppException("Value Out Of Allowed Range");
						} else
							throw new DBAppException("Invalid Data Type");

					}

				}

			}

		}

		System.out.println(row);
		Table t = null;

		for (Object item : MEM) {
			if (item instanceof Table) {
				t = (Table) item;

				if (t.strTableName.equals(strTableName)) {
					ArrayList<String[]> tuples_csv = new ArrayList<>();
					ArrayList<String> tuples_page = new ArrayList<String>();

					tuples_csv = tableTuples;
					Page p = null;

					if (tuples_csv.size() == 0) {
						tuples_page.add(row);
						p = t.Pages.get(0);
						String filepath = "./src/fileSystem/Page" + (p.getPagenumnber()) + ".csv";
						p.writeIntoPage(row, filepath);
//						p.setNumberOfTuples(p.getNumberOfTuples() + 1);
						MEM.remove(tuples_csv);
					} else {
						tuples_csv.add(row.split(","));
						if (strTableName.equals("Product")) {
							Collections.sort(tuples_csv, new SortingOnKey(0));

						} else {
							Collections.sort(tuples_csv, new SortingOnKey(2));

						}
						tuples_page.clear();

						for (String[] array : tuples_csv) {
							tuples_page.add(String.join(",", array));
						}
						int j = 0;

//						for (int i = 0; i < t.Pages.size(); i++) {
//							for (int k = 0; k < t.Pages.get(i).maxTuples; k++) {
//								String filepath = "./src/fileSystem/Page" + (t.Pages.get(i).getPagenumnber()) + ".csv";
//								if (j < tuples_page.size()) {
//									t.Pages.get(i).writeIntoPage(tuples_page.get(j), filepath);
//									j++;
//								}
//							}
//						}
						boolean dontEnter = false;

						for (int i = 0; i < t.Pages.size(); i++) {
							System.out.println("maxTuples :" + t.Pages.get(i).maxTuples);
							System.out.println("getNumberOfTuples :" + t.Pages.get(i).getNumberOfTuples());
							while (t.Pages.get(i).getNumberOfTuples() < t.Pages.get(i).maxTuples
									&& t.Pages.get(i).getNumberOfTuples() < tuples_page.size() && j<tuples_page.size()) {
								String filepath = "./src/fileSystem/Page" + (t.Pages.get(i).getPagenumnber()) + ".csv";
								System.out.println("i: "+i+"j "+j+"t.Pages.size()i bounds "+t.Pages.size()+"tuples_page size: "+tuples_page.size());
							
								t.Pages.get(i).writeIntoPage(tuples_page.get(j), filepath);
								j++;
								

							}

						}
						if (j < tuples_page.size()) {
							dontEnter = true;
						}

						if (dontEnter == true) {
							p = new Page();
							t.Pages.add(p);
							String filepath = "./src/fileSystem/Page" + (p.getPagenumnber()) + ".csv";
							p.writeIntoPage(tuples_page.get(j), filepath);
							System.out.println("create and insert one rec into a new page :)");
							dontEnter = false;

						}

//						Page p = t.Pages.get(i);

//						MEM.add(tuples_csv);
////							t.Pages.get(i).getTuples().add(row);
//						
//							
////						p.getSortingTuples().add(row.split(","));
//						Collections.sort(tuples_csv, new SortingOnKey(0));
////						tuples_page.clear();
//						for (String[] array : tuples_csv) {
//							tuples_page.add(String.join(",", array));
//						}

						// sort somehow on key
//						
//						String filepath = "./src/fileSystem/Page" + (p.getPagenumnber()) + ".csv";
//						//p.writeIntoPage(tuples_page, filepath ,t);
//						p.setNumberOfTuples(p.getNumberOfTuples() + 1);
//
//						MEM.remove(tuples_csv);

//						else System.out.println("add page yaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");

					}

				}
			}
//				break;

		}
	}

	
	public void DeleteHelper(String strTableName,ArrayList<String[]> tableTuples )
			throws DBAppException, ClassNotFoundException, InstantiationException, IllegalAccessException,
			IllegalArgumentException, InvocationTargetException, NoSuchMethodException, SecurityException, IOException {
		

		

		Table t = null;

		for (Object item : MEM) {
			if (item instanceof Table) {
				t = (Table) item;

				if (t.strTableName.equals(strTableName)) {
					ArrayList<String[]> tuples_csv = new ArrayList<>();
					ArrayList<String> tuples_page = new ArrayList<String>();

					tuples_csv = tableTuples;
					Page p = null;

					if (tuples_csv.size() == 0) {
					
//						p = t.Pages.get(0);
//						String filepath = "./src/fileSystem/Page" + (p.getPagenumnber()) + ".csv";
//						p.writeIntoPage(row, filepath);
////						p.setNumberOfTuples(p.getNumberOfTuples() + 1);
//						MEM.remove(tuples_csv);
					} else {
						
						if (strTableName.equals("Product")) {
							Collections.sort(tuples_csv, new SortingOnKey(0));

						} else {
							Collections.sort(tuples_csv, new SortingOnKey(2));

						}
						tuples_page.clear();

						for (String[] array : tuples_csv) {
							tuples_page.add(String.join(",", array));
						}
						int j = 0;


						boolean dontEnter = false;

						for (int i = 0; i < t.Pages.size(); i++) {
							System.out.println("maxTuples :" + t.Pages.get(i).maxTuples);
							System.out.println("getNumberOfTuples :" + t.Pages.get(i).getNumberOfTuples());
							while (t.Pages.get(i).getNumberOfTuples() < t.Pages.get(i).maxTuples
									&& t.Pages.get(i).getNumberOfTuples() < tuples_page.size() && j<tuples_page.size()) {
								String filepath = "./src/fileSystem/Page" + (t.Pages.get(i).getPagenumnber()) + ".csv";
								System.out.println("i: "+i+"j "+j+"t.Pages.size()i bounds "+t.Pages.size()+"tuples_page size: "+tuples_page.size());
							
								t.Pages.get(i).writeIntoPage(tuples_page.get(j), filepath);
								j++;
								

							}

						}
						if (j < tuples_page.size()) {
							dontEnter = true;
						}

						if (dontEnter == true) {
							p = new Page();
							t.Pages.add(p);
							String filepath = "./src/fileSystem/Page" + (p.getPagenumnber()) + ".csv";
							p.writeIntoPage(tuples_page.get(j), filepath);
							System.out.println("create and insert one rec into a new page :)");
							dontEnter = false;

						}

					}

				}
			}
//				break;

		}
	}


	    public static void deleteCSVFile(String filePath) {
	        File file = new File(filePath);

	        if (file.exists()) {
	        	 System.out.println("The CSV file deleted.");
	        } else {
	            System.out.println("The CSV file does not exist.");
	           
	        }
	    }
	
	
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub

	}

}
