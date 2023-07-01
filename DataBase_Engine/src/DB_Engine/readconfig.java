package DB_Engine;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class readconfig {
    private static final String CONFIG_FILE_PATH = "config.properties";
    private Properties properties;

    public readconfig() {
        properties = new Properties();
        try {
            FileInputStream input = new FileInputStream(CONFIG_FILE_PATH);
            properties.load(input);
            input.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getMaxNumOfRows() {
        return properties.getProperty("maxnumofrows");
    }
    public String getMinNumOfRows() {
        return properties.getProperty("minnumofrows");
    }
    public static void main (String [] args){
    	readconfig configReader = new readconfig();
         String maxNumOfRows = configReader.getMaxNumOfRows();
         System.out.println("Max Number of Rows: " + maxNumOfRows);	
         String minNumOfRows = configReader.getMinNumOfRows();
         System.out.println("Max Number of Rows: " + minNumOfRows);	
    }
}
