import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;



/**
 * @author Chris Irwin Davis
 * @version 1.0
 * <b>This is an example of how to read/write binary data files using RandomAccessFile class</b>
 *
 */
public class MainDavisBase {

	/* This can be changed to whatever you like */
	static String prompt = "davisql> ";

	/* 
	 *  The Scanner class is used to collect user commands from the prompt
	 *  There are many ways to do this. This is just one.
	 *
	 *  Each time the semicolon (;) delimiter is entered, the inputString 
	 *  String is re-populated.
	 */
	static Scanner scanner = new Scanner(System.in).useDelimiter(";");
	
	
    public static void main(String[] args) {
    	init();
		/* Display the welcome screen */
		splashScreen();

		/* Variable to collect user input from the prompt */
		String inputString = ""; 

		while(!inputString.equals("exit")) {
			System.out.print(prompt);
			/* toLowerCase() renders command case insensitive */
			inputString = scanner.next().replace("\n", " ").replace("\r", "").trim().toLowerCase();
			inputCommandParser(inputString);
		}
		System.out.println("MainDavisBase Exit Successfully");


	}
	
	public static void init(){
		try {
			File directory = new File("data");
			File directory1 = new File("data/catalog");
			File directory2 = new File("data/user_tables");
			boolean result=directory.mkdir();
			directory1.mkdirs();
			directory2.mkdirs();
			if(result){				
				DatabaseMethods.Create_Meta_Files();
			}else {
				String meta_col = "davisbase_columns.tbl";
				String meta_tab = "davisbase_tables.tbl";
				String[] existFiles = directory.list();
				boolean flag = false;
				for (int i=0; i<existFiles.length; i++) {
					if(existFiles[i].equals(meta_col))
						flag = true;
				}
				if(!flag){
					
					DatabaseMethods.Create_Meta_Files();
				}
				flag = false;
				for (int i=0; i<existFiles.length; i++) {
					if(existFiles[i].equals(meta_tab))
						flag = true;
				}
				if(!flag){
					
					DatabaseMethods.Create_Meta_Files();
				}
			}
		}catch (SecurityException e) {
					System.out.println(e);
		}

	}


	/**
	 *  Display the splash screen
	 */
	public static void splashScreen() {
		System.out.println(line("-",100));
        System.out.println("Welcome to DavisBaseLite"); // Display the string.
		version();
		System.out.println("Type \"help;\" to display supported commands.");
		System.out.println(line("-",100));
	}
	
	/**
	 * @param s The String to be repeated
	 * @param num The number of time to repeat String s.
	 * @return String A String object, which is the String s appended to itself num times.
	 */
	public static String line(String s,int num) {
		String a = "";
		for(int i=0;i<num;i++) {
			a += s;
		}
		return a;
	}
	
	/**
	 *  Help: Display supported commands
	 */
	public static void help() {
		System.out.println(line("*",100));
		System.out.println("SUPPORTED COMMANDS");
		System.out.println("All commands below are case insensitive");
		System.out.println();
		System.out.println("\tSELECT * FROM table_name;                                 Display all records in the table.");
		System.out.println("\tSELECT * FROM table_name WHERE rowid = <value>;           Display records whose rowid is <id>.");
		System.out.println("\tINSERT INTO table_name VALUES ();                         Insert records in table where rowid is <value>.");
		System.out.println("\tDROP TABLE table_name;                                    Remove table data and its schema.");
		System.out.println("\tDELETE FROM table_name WHERE row_id =value;               Remove table values.");
		System.out.println("\tCREATE TABLE table_name (DataType ColName PK NotNull);    Remove table data and its schema.");
		System.out.println("\tUPDATE table_name SET col_name= value WHERE condition;    Update Records in table");
		System.out.println("\tSHOW TABLES;                                              Show all tables.");
		System.out.println("\tVERSION;                                                  Show the program version.");
		System.out.println("\tHELP;                                                     Show this help information");
		System.out.println("\tEXIT;                                                     Exit the program");
		System.out.println();
		System.out.println();
		System.out.println(line("*",100));
	}

	/** Display the MainDavisBase version */
	public static void version() {
		System.out.println("DavisBaseLite v1.0\n");
	}


	public static void inputCommandParser(String inputString) {
		
		String[] inputTokens = inputString.split(" ");
			System.out.println("Parsing the Input Command...");

			
			if(inputTokens[0].equals("init")){
				
				System.out.println("Creating Meta Files...");
				DatabaseMethods.Create_Meta_Files();
				
			}
			else if(inputTokens[0].equals("show")){
				
				System.out.println("Displaying All Tables...");
				DatabaseMethods.showAllTables();
				
			}
			else if(inputTokens[0].equals("select")){
		
				String[] select_cmp;
				String[] select_column;
				String[] select_temp = inputString.split("where");
				if(select_temp.length > 1){
					String filter = select_temp[1].trim();
					select_cmp = commandParser(filter);
				}else{
					select_cmp = new String[0];
				}
				String[] select = select_temp[0].split("from");
				String select_table = select[1].trim();
				String select_cols = select[0].replace("select", "").trim();
				if(select_cols.contains("*")){
					select_column = new String[1];
					select_column[0] = "*";
				}
				else{
					select_column = select_cols.split(",");
					for(int i = 0; i < select_column.length; i++)
						select_column[i] = select_column[i].trim();
				}
				if(!DatabaseMethods.ifTableFound(select_table)){
					System.out.println("Table "+select_table+" does not exist.");
					System.out.println();
					
				}else{
				System.out.println("Displaying All Records from Table: "+select_table);
				DatabaseMethods.selectTable(select_table, select_column, select_cmp);
				}
			}
			else if(inputTokens[0].equals("insert")){//'abc'
			
				String insert_table = inputTokens[2];
				String insert_vals = inputString.split("values")[1].trim();
				insert_vals = insert_vals.substring(1, insert_vals.length()-1);
				String[] insert_values = insert_vals.split(",");
				for(int i = 0; i < insert_values.length; i++)
				{
					insert_values[i] = insert_values[i].trim();
					if(insert_values[i].charAt(0)=='\'')
						insert_values[i] =insert_values[i].substring(1,insert_values[i].length()-1);
				
				}
				if(!DatabaseMethods.ifTableFound(insert_table)){
					System.out.println("table "+insert_table+" does not exist.");
					System.out.println();
					
				}
				else{
				System.out.println("Inserting Records in Table: "+ insert_table);
				DatabaseMethods.insertValuesInTable(insert_table, insert_values);
				
				}
			}
			else if(inputTokens[0].equals("create")){
			
				String create_table = inputTokens[2];
				String[] create_temp = inputString.split(create_table);
				String col_temp = create_temp[1].trim();
				String[] create_cols = col_temp.substring(1, col_temp.length()-1).split(",");
				for(int i = 0; i < create_cols.length; i++)
				{
					create_cols[i] = create_cols[i].trim();
				
				//System.out.print("columns recorded are:" + create_cols[i] + " ");
				}
				if(DatabaseMethods.ifTableFound(create_table)){
					System.out.println("table "+create_table+" already exists.");
					System.out.println();
					
				}else{
				System.out.println("Creating New Table: "+ create_table);
				DatabaseMethods.createNewTable(create_table, create_cols);		
				
				}
			}
			else if(inputTokens[0].equals("drop")){
			
				String drop_table = inputTokens[2];
				if(!DatabaseMethods.ifTableFound(drop_table)){
					System.out.println("table "+drop_table+" does not exist.");
					System.out.println();
					
				}
				else{
				System.out.println("Dropping Table: "+ drop_table);
				DatabaseMethods.dropSelectedTable(drop_table);
				}
			}
			else if(inputTokens[0].equals("update")){	
			
				String update_table = inputTokens[1];
				String[] update_temp1 = inputString.split("set");
				String[] update_temp2 = update_temp1[1].split("where");
				String update_cmp_s = update_temp2[1];
				String update_set_s = update_temp2[0];
				String[] set = commandParser(update_set_s);
				String[] update_cmp = commandParser(update_cmp_s);
				if(!DatabaseMethods.ifTableFound(update_table)){
					System.out.println("Table "+update_table+" does not exist.");
					System.out.println();
					
				}else{
				System.out.println("Updating Table: "+ update_table);
				DatabaseMethods.updateTableValues(update_table, set, update_cmp);
				}
			}
			else if(inputTokens[0].equals("delete")){
			
			  	String[] tokens=inputString.split(" ");
						String table = tokens[2];
						String[] temp = inputString.split("where");
						String cmpTemp = temp[1];
						String[] cmp = commandParser(cmpTemp);
						if(!DatabaseMethods.ifTableFound(table)){
							System.out.println("Table "+table+" does not exist.");
						}
						else
						{
							System.out.println("Creating New Table: " + table);
							DatabaseMethods.delete(table, cmp);
						}
			}
			else if(inputTokens[0].equals("help")){			
			
				help();
				
			}
			else if(inputTokens[0].equals("version")){
			
				version();
				
			}
			else if(inputTokens[0].equals("exit")){
			
				
			}
			else{
			
				System.out.println("Command cannot be parsed. Please Re-enter (with proper spacing) !!\"" + inputString + "\"");
				System.out.println();
				
			}
		}
	
	
	public static String[] commandParser(String equ){
		
		String cmp[] = new String[3];
		String temp[] = new String[2];
		if(equ.contains("=")) {
			temp = equ.split("=");
			cmp[0] = temp[0].trim();
			cmp[1] = "=";
			cmp[2] = temp[1].trim();
		}

		if(equ.contains(">")) {
			temp = equ.split(">");
			cmp[0] = temp[0].trim();
			cmp[1] = ">";
			cmp[2] = temp[1].trim();
		}

		if(equ.contains("<")) {
			temp = equ.split("<");
			cmp[0] = temp[0].trim();
			cmp[1] = "<";
			cmp[2] = temp[1].trim();
		}

		if(equ.contains(">=")) {
			temp = equ.split(">=");
			cmp[0] = temp[0].trim();
			cmp[1] = ">=";
			cmp[2] = temp[1].trim();
		}

		if(equ.contains("<=")) {
			temp = equ.split("<=");
			cmp[0] = temp[0].trim();
			cmp[1] = "<=";
			cmp[2] = temp[1].trim();
		}

		if(equ.contains("<>")) {
			temp = equ.split("<>");
			cmp[0] = temp[0].trim();
			cmp[1] = "<>";
			cmp[2] = temp[1].trim();
		}

		return cmp;
	}// end commandParser
		
	
}