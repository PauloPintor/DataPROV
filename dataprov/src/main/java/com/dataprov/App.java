package com.dataprov;

public class App 
{
    public static void main( String[] args ) throws Exception
    {

		String database = "";
		String databaseURL = "";
		String query = "";
		boolean withWhy = false;
		boolean withBoolean = false;
		boolean withTrio = false;
		boolean withPos = false;
		boolean withLineage = false;
		boolean withTime = false;
		boolean withTrasnform = false;
		boolean withBdInfo = false;
		boolean noProvenance = false;
		boolean printHelp = false;
        boolean noResult = false;
		boolean yael = false;

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
                }else if (args[i].toLowerCase().equals("-wb")) {
                    if(args[i + 1].toLowerCase().equals("true") || args[i + 1].toLowerCase().equals("t"))
						withBoolean = true;
                }else if (args[i].toLowerCase().equals("-wt")) {
					if(args[i + 1].toLowerCase().equals("true") || args[i + 1].toLowerCase().equals("t"))
						withTrio = true;
				}else if (args[i].toLowerCase().equals("-wpos")) {
					if(args[i + 1].toLowerCase().equals("true") || args[i + 1].toLowerCase().equals("t"))
						withPos = true;
				}else if (args[i].toLowerCase().equals("-wl")) {
					if(args[i + 1].toLowerCase().equals("true") || args[i + 1].toLowerCase().equals("t"))
						withLineage = true;
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
                }else if (args[i].toLowerCase().equals("-nr")) {
                    if(args[i + 1].toLowerCase().equals("true") || args[i + 1].toLowerCase().equals("t"))
						noResult = true;
                }
                
            }
        }
		
		if(printHelp){
			System.out.println(getHelp());
		}else{
			if(database.compareTo("") == 0) {
				throw new IllegalArgumentException("The database (-d) argument cannot be empty");
			}else if(databaseURL.compareTo("") == 0 && noResult == false){
				throw new IllegalArgumentException("The database URL (-u) argument cannot be empty");
			}else if( query.compareTo("") == 0){
				throw new IllegalArgumentException("The query (-q) argument cannot be empty");
			}

            if(noResult == false){
                System.out.println();
                System.out.println("**************************DATAPROV**************************");
                System.out.println();
            }
			
			AppManager app = new AppManager(database, databaseURL, query, withWhy, withBoolean, withTrio, withPos, withLineage, withTrasnform, withTime, noProvenance, noResult, withBdInfo, yael);
		}
    }

	public static String getHelp(){
		return "Usage: java -jar dataprov.jar -d <database> -u <databaseURL> -q <query> [-wp <withWhy> -t <executionTime> -nq <printTrasnformedQuery> -np <noProvenance>]\n" +
				"  -d <database>      The database name\n" +
				"  -u <databaseURL>   The database URL\n" +
				"  -q <query>         The query to execute\n" +
				"  -wb <withBoolean>      If true, the boolean provenance (B[X]) is computed\n" +
				"  -wt <withTrio>      If true, the trio provenance (Trio(X)) is computed\n" +
				"  -wp <withWhy>      If true, the why-provenance (Why(X)) is computed\n" +
				"  -wpos <withPos>      If true, the positive boolean provenance (PosBool(X)) is computed\n" +
				"  -wl <withLineage>      If true, the lineage provenance (Lin(X)) is computed\n" +
				"  -t <executionTime> If true, the execution time is computed\n" +
				" -dbi <databaseInfo> If true, the provenance tokens will contain database information (database:schema:table:token)\n" +
				"  -nq <printTrasnformedQuery> If true, the transformed query is printed\n" +
				"  -np <noProvenance> If true, the provenance is not computed\n" +
                "  -nr <noResult> If true, the result is not printed\n";
	}
}
