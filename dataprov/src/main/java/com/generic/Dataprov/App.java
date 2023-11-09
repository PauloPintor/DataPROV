package com.generic.Dataprov;

import org.apache.commons.validator.routines.UrlValidator;

public class App 
{
    public static void main( String[] args ) throws Exception
    {

		String database = "";
		String databaseURL = "";
		String query = "";
		boolean withWhy = false;

		for (int i = 0; i < args.length; i++) {
            if (i < args.length - 1) {
                if (args[i].toLowerCase().equals("-d")) {
                    database = args[i + 1];
                } else if (args[i].toLowerCase().equals("-u")) {
                    databaseURL = args[i + 1];
                } else if (args[i].toLowerCase().equals("-q")) {
                    query = args[i + 1];
                }else if (args[i].toLowerCase().equals("-wp")) {
                    if(args[i + 1].toLowerCase().equals("true") || args[i + 1].toLowerCase().equals("t"))
						withWhy = true;
                }
            }
        }
		
		if(database.compareTo("") == 0) {
			throw new IllegalArgumentException("The database (-d) argument cannot be empty");
		}else if(databaseURL.compareTo("") == 0){
			throw new IllegalArgumentException("The database URL (-u) argument cannot be empty");
		}else if( query.compareTo("") == 0){
			throw new IllegalArgumentException("The query (-q) argument cannot be empty");
		}
		
		AppManager app = new AppManager(database, databaseURL, query, withWhy);
    }
}