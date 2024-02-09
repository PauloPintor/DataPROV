package com.generic.Dataprov;

public class App 
{
    public static void main( String[] args ) throws Exception
    {

		String database = "";
		String databaseURL = "";
		String query = "";
		boolean withWhy = false;
		boolean withTime = false;
		boolean withTrasnform = false;
		boolean noProvenance = false;
		boolean printHelp = false;

		System.out.println();
		System.out.println("**************************DATAPROV**************************");
		System.out.println();

		for (int i = 0; i < args.length; i++) {
            if (i <= args.length - 1) {
                if (args[i].toLowerCase().equals("-d")) {
                    database = args[i + 1];
                } if (args[i].toLowerCase().equals("-h") || args[i].toLowerCase().equals("--help")) {
					printHelp = true;
                }else if (args[i].toLowerCase().equals("-u")) {
                    databaseURL = args[i + 1];
                } else if (args[i].toLowerCase().equals("-q")) {
                    query = args[i + 1];
                }else if (args[i].toLowerCase().equals("-wp")) {
                    if(args[i + 1].toLowerCase().equals("true") || args[i + 1].toLowerCase().equals("t"))
						withWhy = true;
                }
				else if (args[i].toLowerCase().equals("-t")) {
                    if(args[i + 1].toLowerCase().equals("true") || args[i + 1].toLowerCase().equals("t"))
						withTime = true;
                }
				else if (args[i].toLowerCase().equals("-nq")) {
                    if(args[i + 1].toLowerCase().equals("true") || args[i + 1].toLowerCase().equals("t"))
						withTrasnform = true;
                }else if (args[i].toLowerCase().equals("-np")) {
                    if(args[i + 1].toLowerCase().equals("true") || args[i + 1].toLowerCase().equals("t"))
						noProvenance = true;
                }
            }
        }
		
		if(printHelp){
			System.out.println(getHelp());
		}else{
			if(database.compareTo("") == 0) {
				throw new IllegalArgumentException("The database (-d) argument cannot be empty");
			}else if(databaseURL.compareTo("") == 0){
				throw new IllegalArgumentException("The database URL (-u) argument cannot be empty");
			}else if( query.compareTo("") == 0){
				throw new IllegalArgumentException("The query (-q) argument cannot be empty");
			}
			
			AppManager app = new AppManager(database, databaseURL, query, withWhy,withTrasnform, withTime, noProvenance);
		}
    }

	public static String getHelp(){
		return "Usage: java -jar dataprov.jar -d <database> -u <databaseURL> -q <query> [-wp <withWhy> -t <executionTime> -nq <printTrasnformedQuery> -np <noProvenance>]\n" +
				"  -d <database>      The database name\n" +
				"  -u <databaseURL>   The database URL\n" +
				"  -q <query>         The query to execute\n" +
				"  -wp <withWhy>      If true, the why-provenance is computed\n" +
				"  -t <executionTime> If true, the execution time is computed\n" +
				"  -nq <printTrasnformedQuery> If true, the transformed query is printed\n" +
				"  -np <noProvenance> If true, the provenance is not computed\n";
	}
}