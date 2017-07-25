import java.io.RandomAccessFile;
import java.io.FileReader;
import java.io.File;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;

public class DatabaseMethods{
	
	public static int off = -1;
	private static RandomAccessFile davisbasetable;
	private static RandomAccessFile davisbasecolumn;
	public static int pageSize = 512;
	// test case
	public static void main(String[] args){}

	// function to show all tables in the database
	public static void showAllTables(){
		
		String[] compare = new String[0];
		String[] columns = {"table_name"};
		String tablename = "davisbase_tables";
		selectTable(tablename, columns, compare);
	}

    //DatabaseMethods creation
	public static void createNewTable(String table, String[] columns){
		try{	
			//file
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			file.setLength(512);
			file.seek(0);
			file.writeByte(0x0D);
			file.close();
			// table
			file = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			int numPages = pages(file);
			int page = 1;
			for(int p = 1; p <= numPages; p++){
				int rm = BTreeLeafPage.getRightpointer(file, p);
				if(rm == 0)
					page = p;
			}
			int[] keyArray = BTreeLeafPage.getKeys(file, page);
			int l = keyArray[0];
			for(int i = 0; i < keyArray.length; i++)
				if(l < keyArray[i])
					l = keyArray[i];
			file.close();
			String[] values = {Integer.toString(l+1), table};
			//System.out.println("table values: ");
			//for(String j:values)
				//System.out.print(j +  " ");
			//System.out.println();
			insertValuesInTable("davisbase_tables", values);

			RandomAccessFile create_file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			CurrentData item = new CurrentData();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {};
			filter(create_file, cmp, columnName, item);
			l = item.map_data.size();

			for(int i = 0; i < columns.length; i++){
				l = l + 1;
				String[] token = columns[i].split(" ");
				String n = "YES";
				if(token.length > 2)
					n = "NO";
				String col_name = token[0];
				String dt = token[1].toUpperCase();
				String pos = Integer.toString(i+1);
				String[] v = {Integer.toString(l), table, col_name, dt, pos, n};
				//for(String k:values)
			     //System.out.println("values: "+ k);
				//System.out.println("column values: ");
				//for(String j:v)
					//System.out.print(j +  " ");
				//System.out.println();
				insertValuesInTable("davisbase_columns", v);
			}
			file.close();
		}catch(Exception e){
			System.out.println("Error occoured while creating a new table !! View All errors:");
			e.printStackTrace();
		}
	}
	public static int pages(RandomAccessFile file){
		int num_pages = 0;
		try{
			num_pages = (int)(file.length()/(new Long(pageSize)));
		}catch(Exception e){
			System.out.println("Catch error: "+e);
		}

		return num_pages;
	}
	public static void delete(String table, String[] cmp){
		try{
		int key = new Integer(cmp[2]);
		RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
		int numPages = pages(file);
		int page = 0;
		for(int p = 1; p <= numPages; p++)
			if(BTreeLeafPage.hasKey(file, p, key)&BTreeLeafPage.getPageType(file, p)==0x0D){
				page = p;
				break;
			}

		if(page==0)
		{
			System.out.println(" HashMap Error: key-value does pair not exist.");
			return;
		}
		short[] cellsAddr = BTreeLeafPage.getCellArray(file, page);
		int k = 0;
		for(int i = 0; i < cellsAddr.length; i++)
		{
			long loc = BTreeLeafPage.getCellLoc(file, page, i);
			String[] vals = retrievePayload(file, loc);
			int x = new Integer(vals[0]);
			if(x!=key)
			{
				BTreeLeafPage.setCellOffset(file, page, k, cellsAddr[i]);
				k++;
			}
		}
		BTreeLeafPage.setCellNumber(file, page, (byte)k);
		}catch(Exception e)
		{
			System.out.println(e);
		}
	}

	/*
	 * Deletes the table entries from the database...
	 */
	public static void dropSelectedTable(String table){
		try{
			// clear meta-table
			RandomAccessFile file = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			int numPages = pages(file);
			for(int page = 1; page <= numPages; page ++){
				file.seek((page-1)*512);
				byte type = file.readByte();
				if(type == 0x05)
					continue;
				else{
					short[] cells = BTreeLeafPage.getCells(file, page);
					int i = 0;
					for(int j = 0; j < cells.length; j++){
						long loc = BTreeLeafPage.getCelloffnum(file, page, j);
						String[] pl = retrievePayload(file, loc);
						String tb = pl[1];
						if(!tb.equals(table)){
							BTreeLeafPage.setCellOffset(file, page, i, cells[j]);
							i++;
						}
					}
					BTreeLeafPage.settotalcells(file, page, (byte)i);
				}
			}

			// clear meta-column
			file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			numPages = pages(file);
			for(int page = 1; page <= numPages; page ++){
				file.seek((page-1)*512);
				byte type = file.readByte();
				if(type == 0x05)
					continue;
				else{
					short[] cells = BTreeLeafPage.getCells(file, page);
					int i = 0;
					for(int j = 0; j < cells.length; j++){
						long loc = BTreeLeafPage.getCelloffnum(file, page, j);
						String[] pl = retrievePayload(file, loc);
						String tb = pl[1];
						if(!tb.equals(table)){
							BTreeLeafPage.setCellOffset(file, page, i, cells[j]);
							i++;
						}
					}
					BTreeLeafPage.settotalcells(file, page, (byte)i);
				}
			}

			//delete file
			try{
			File anOldFile = new File("data", table+".tbl"); 
			anOldFile.delete();
			}
			catch(Exception e){
				System.out.println(e);
			}
			
		}catch(Exception e){
			System.out.println("Error occoured while Dopping the table. View StackTrace....");
			System.out.println(e);
		}

	}

	public static String[] retrievePayload(RandomAccessFile file, long loc){
		String[] payload = new String[0];
		final String dp = "yyyy-MM-dd_HH:mm:ss";
		try{
			Long tmp;
			SimpleDateFormat formater = new SimpleDateFormat (dp);

			// get stc
			file.seek(loc);
			int plsize = file.readShort();
			int key = file.readInt();
			int num_cols = file.readByte();
			byte[] stc = new byte[num_cols];
			int temp = file.read(stc);
			payload = new String[num_cols+1];
			payload[0] = Integer.toString(key);
			// get payLoad
			for(int i=1; i <= num_cols; i++){
				switch(stc[i-1]){
					case 0x00:  payload[i] = Integer.toString(file.readByte());
								payload[i] = "null";
								break;

					case 0x01:  payload[i] = Integer.toString(file.readShort());
								payload[i] = "null";
								break;

					case 0x02:  payload[i] = Integer.toString(file.readInt());
								payload[i] = "null";
								break;

					case 0x03:  payload[i] = Long.toString(file.readLong());
								payload[i] = "null";
								break;

					case 0x04:  payload[i] = Integer.toString(file.readByte());
								break;

					case 0x05:  payload[i] = Integer.toString(file.readShort());
								break;

					case 0x06:  payload[i] = Integer.toString(file.readInt());
								break;

					case 0x07:  payload[i] = Long.toString(file.readLong());
								break;

					case 0x08:  payload[i] = String.valueOf(file.readFloat());
								break;

					case 0x09:  payload[i] = String.valueOf(file.readDouble());
								break;

					case 0x0A:  tmp = file.readLong();
								Date dateTime = new Date(tmp);
								payload[i] = formater.format(dateTime);
								break;

					case 0x0B:  tmp = file.readLong();
								Date date = new Date(tmp);
								payload[i] = formater.format(date).substring(0,10);
								break;

					default:    int len = new Integer(stc[i-1]-0x0C);
								byte[] bytes = new byte[len];
								for(int j = 0; j < len; j++)
									bytes[j] = file.readByte();
								payload[i] = new String(bytes);
								break;
				}
			}

		}catch(Exception e){
			System.out.println("Error occoured while retrieving Page Load Information.");
		}

		return payload;
	}
	public static boolean ifTableFound(String table){
		boolean e = false;
		table = table+".tbl";
		try {
			File directory = new File("data");
			String[] existFiles;
			existFiles = directory.list();
			for (int i=0; i<existFiles.length; i++) {
				if(existFiles[i].equals(table))
					return true;
			}
		}
		catch (SecurityException se) {
			System.out.println("Error occoured while creating diretory! Stack Trace....");
			System.out.println(se);
		}

		return e;
	}


	public static void updateTableValues(String table, String[] set, String[] cmp){
		try{
			int key = new Integer(cmp[2]);
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			int numPages = pages(file);
			int page = 1;

			for(int p = 1; p <= numPages; p++)
				if(BTreeLeafPage.hasKey(file, p, key)){
					page = p;
				}
			int[] array = BTreeLeafPage.getKeys(file, page);
			int id = 0;
			for(int i = 0; i < array.length; i++)
				if(array[i] == key)
					id = i;
			int offset = BTreeLeafPage.getCellOffset(file, page, id);
			long loc = BTreeLeafPage.getCelloffnum(file, page, id);
			String[] array_s = getColName(table);
			int num_cols = array_s.length - 1;
			String[] values = retrievePayload(file, loc);


			// fix date time type value to string format before update
			String[] type = getDataType(table);
			for(int i=0; i < type.length; i++)
				if(type[i].equals("DATE") || type[i].equals("DATETIME"))
					values[i] = "'"+values[i]+"'";


			// update value on a column
			for(int i = 0; i < array_s.length; i++)
				if(array_s[i].equals(set[0]))
					id = i;
			values[id] = set[2];

			// check null value violation
			String[] nullable = CheckNullCondition(table);

			for(int i = 0; i < nullable.length; i++){
				if(values[i].equals("null") && nullable[i].equals("NO")){
					System.out.println("Error: due to NULL value constraint ");
					System.out.println();
					return;
				}
			}


			byte[] stc = new byte[array_s.length-1];
			int plsize = calPayloadSize(table, values, stc);
			BTreeLeafPage.updateLeafnode(file, page, offset, plsize, key, stc, values);

			file.close();

		}catch(Exception e){
			System.out.println("Error occoured while updating table records.");
			System.out.println(e);
		}
	}

	public static void insertValuesInTable(RandomAccessFile file, String table, String[] values){
		String[] dtype = getDataType(table);
		//for(String i:values)
		 //System.out.println(i);
		String[] nullable = CheckNullCondition(table);

		for(int i = 0; i < nullable.length; i++)
			if(values[i].equals("null") && nullable[i].equals("NO")){
				System.out.println("Error: due to NULL value constraint ");
				System.out.println();
				return;
			}


		int key = new Integer(values[0]);
		int page = searchKey(file, key);
		if(page != 0)
			if(BTreeLeafPage.hasKey(file, page, key)){
				System.out.println("Error Occoured Due to Existing Key Constraint ");
				System.out.println();
				return;
			}
		if(page == 0)
			page = 1;


		byte[] stc = new byte[dtype.length-1];//-1
		short plSize = (short) calPayloadSize(table, values, stc);
		int cellSize = plSize + 6;
		int offset = BTreeLeafPage.checkLeafSpace(file, page, cellSize);
		/*if(off==-1)
		{	System.out.println("im here..");
			BTreeLeafPage.addLeafNode(file, page, 0, plSize, key, stc, values);
			off=0;
		}*/ 
		if(offset != -1){
			//System.out.println("im here 2");
			BTreeLeafPage.addLeafNode(file, page, offset, plSize, key, stc, values);
			
		}else{
			//System.out.println("im here 3");
			//System.out.println("OverFlow In page occoured..");
			BTreeLeafPage.ExpandOverflowLeaf(file, page);
			insertValuesInTable(file, table, values);
		}
	}

	public static void insertValuesInTable(String table, String[] values){
		try{
			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			insertValuesInTable(file, table, values);
			file.close();

		}catch(Exception e){
			System.out.println("Error occoured in function: insertValuesInTable()");
			e.printStackTrace();
		}
	}

	// calculate the size of payload 
	public static int calPayloadSize(String table, String[] vals, byte[] stc){
		//for(String i:vals)
		 //System.out.println(i);
		String[] dataType = getDataType(table);
		int size = 1;
		size = size + dataType.length - 1;
		for(int i = 1; i < dataType.length; i++){
			byte tmp = stcCode(vals[i], dataType[i]);
			stc[i - 1] = tmp;
			size = size + feildLength(tmp);
			//System.out.println(size);
		}
		return size;
	}

	//calculate value length by STC code
	public static short feildLength(byte stc){
		switch(stc){
			case 0x00: return 1;
			case 0x01: return 2;
			case 0x02: return 4;
			case 0x03: return 8;
			case 0x04: return 1;
			case 0x05: return 2;
			case 0x06: return 4;
			case 0x07: return 8;
			case 0x08: return 4;
			case 0x09: return 8;
			case 0x0A: return 8;
			case 0x0B: return 8;
			default:   return (short)(stc - 0x0C);
		}
	}

	// return STC
	public static byte stcCode(String val, String dataType){
		if(val.equals("null")){
			switch(dataType){
				case "TINYINT":     return 0x00;
				case "SMALLINT":    return 0x01;
				case "INT":			return 0x02;
				case "BIGINT":      return 0x03;
				case "REAL":        return 0x02;//same as FLOAT
				case "FLOAT":       return 0x02;//same as REAL
				case "DOUBLE":      return 0x03;
				case "DATETIME":    return 0x03;
				case "DATE":        return 0x03;
				case "TEXT":        return 0x03;
				default:			return 0x00;
			}							
		}else{
			switch(dataType){
				case "TINYINT":     return 0x04;
				case "SMALLINT":    return 0x05;
				case "INT":			return 0x06;
				case "BIGINT":      return 0x07;
				case "FLOAT":       return 0x08;//same as REAL
				case "REAL":        return 0x08;//same as FLOAT
				case "DOUBLE":      return 0x09;
				case "DATETIME":    return 0x0A;
				case "DATE":        return 0x0B;
				case "TEXT":        return (byte)(val.length()+0x0C);
				default:			return 0x00;
			}
		}
	}

	public static int searchKey(RandomAccessFile file, int key){
		int val = 1;
		try{
			int numPages = pages(file);
			for(int page = 1; page <= numPages; page++){
				file.seek((page - 1)*512);
				byte pageType = file.readByte();
				if(pageType == 0x0D){
					int[] keys = BTreeLeafPage.getKeys(file, page);
					if(keys.length == 0)
						return 0;
					int rm = BTreeLeafPage.getRightpointer(file, page);
					if(keys[0] <= key && key <= keys[keys.length - 1]){
						return page;
					}else if(rm == 0 && keys[keys.length - 1] < key){
						return page;
					}
				}
			}
		}catch(Exception e){
			System.out.println("Error occoured while searching a page.");
			System.out.println(e);
		}

		return val;
	}


	public static String[] getDataType(String table){
		String[] dataType = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			CurrentData item = new CurrentData();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, item);
			HashMap<Integer, String[]> map_data = item.map_data;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : map_data.values()){
				array.add(i[3]);
			}
			dataType = array.toArray(new String[array.size()]);
			file.close();
			return dataType;
		}catch(Exception e){
			System.out.println("Error occoured in function: getDataType()");
			System.out.println(e);
		}
		return dataType;
	}

	public static String[] getColName(String table){
		String[] c = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			CurrentData item = new CurrentData();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, item);
			HashMap<Integer, String[]> map_data = item.map_data;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : map_data.values()){
				array.add(i[2]);
			}
			c = array.toArray(new String[array.size()]);
			file.close();
			return c;
		}catch(Exception e){
			System.out.println("Error occoured in function:  getColName()");
			System.out.println(e);
		}
		return c;
	}

	public static String[] CheckNullCondition(String table){
		String[] n = new String[0];
		try{
			RandomAccessFile file = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			CurrentData item = new CurrentData();
			String[] columnName = {"rowid", "table_name", "column_name", "data_type", "ordinal_position", "is_nullable"};
			String[] cmp = {"table_name","=",table};
			filter(file, cmp, columnName, item);
			HashMap<Integer, String[]> map_data = item.map_data;
			ArrayList<String> array = new ArrayList<String>();
			for(String[] i : map_data.values()){
				array.add(i[5]);
			}
			n = array.toArray(new String[array.size()]);
			file.close();
			return n;
		}catch(Exception e){
			System.out.println("Error occoured in function:  CheckNullCondition()");
			System.out.println(e);
		}
		return n;
	}

	public static void selectTable(String table, String[] cols, String[] cmp){
		try{
			CurrentData item = new CurrentData();
			String[] columnName = getColName(table);
			String[] type = getDataType(table);

			RandomAccessFile file = new RandomAccessFile("data/"+table+".tbl", "rw");
			filter(file, cmp, columnName, type, item);
			item.showData(cols);
			file.close();
		}catch(Exception e){
			System.out.println("Error occoured in function: selectTable()");
			System.out.println(e);
		}
	}

	// filter fuction for select
	public static void filter(RandomAccessFile file, String[] cmp, String[] columnName, String[] type, CurrentData item){
		try{
			int numPages = pages(file);
			// get column_name
			for(int page = 1; page <= numPages; page++){
				file.seek((page-1)*512);
				byte pageType = file.readByte();
				if(pageType == 0x05)
					continue;
				else{
					byte totalCells = BTreeLeafPage.gettotalcells(file, page);

					for(int i=0; i < totalCells; i++){
						
						long loc = BTreeLeafPage.getCelloffnum(file, page, i);
						file.seek(loc+2); // seek to rowid
						int rowid = file.readInt(); // read rowid
						int num_cols = new Integer(file.readByte()); 

						String[] payload = retrievePayload(file, loc);

						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								payload[j] = "'"+payload[j]+"'";
						// check
						boolean check = cmpCheck(payload, rowid, cmp, columnName);

						// convert back date type
						for(int j=0; j < type.length; j++)
							if(type[j].equals("DATE") || type[j].equals("DATETIME"))
								payload[j] = payload[j].substring(1, payload[j].length()-1);

						if(check)
							item.add(rowid, payload);
					}
				}
			}

			item.columnName = columnName;
			item.format = new int[columnName.length];

		}catch(Exception e){
			System.out.println("Error occoured in function: filter()");
			e.printStackTrace();
		}

	}

	// filter function for getDT getNull
	public static void filter(RandomAccessFile file, String[] cmp, String[] columnName, CurrentData item){
		try{
			int numPages = pages(file);
			// get column_name
			for(int page = 1; page <= numPages; page++){
				file.seek((page-1)*512);
				byte pageType = file.readByte();
				if(pageType == 0x05)
					continue;
				else{
					byte numCells = BTreeLeafPage.gettotalcells(file, page);

					for(int i=0; i < numCells; i++){
						long loc = BTreeLeafPage.getCelloffnum(file, page, i);
						file.seek(loc+2); // seek to rowid
						int rowid = file.readInt(); // read rowid
						int num_cols = new Integer(file.readByte()); // read # of columns other than rowid
						String[] payload = retrievePayload(file, loc);

						boolean check = cmpCheck(payload, rowid, cmp, columnName);
						if(check)
							item.add(rowid, payload);
					}
				}
			}

			item.columnName = columnName;
			item.format = new int[columnName.length];

		}catch(Exception e){
			System.out.println("Error occoured in function: filter()");
			e.printStackTrace();
		}

	}


	// to check the select condition as per the operator...
	public static boolean cmpCheck(String[] payload, int rowid, String[] cmp, String[] columnName){

		boolean check = false;
		if(cmp.length == 0){
			check = true;
		}else{
			int colPos = 1;
			for(int i = 0; i < columnName.length; i++){
				if(columnName[i].equals(cmp[0])){
					colPos = i + 1;
					break;
				}
			}
			String opt = cmp[1];
			String val = cmp[2];
			if(colPos == 1){
				switch(opt){
					case "=": if(rowid == Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">": if(rowid > Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case "<": if(rowid < Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;
							  break;
					case ">=": if(rowid >= Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;	
							  break;
					case "<=": if(rowid <= Integer.parseInt(val)) 
								check = true;
							  else
							  	check = false;	
							  break;
					case "<>": if(rowid != Integer.parseInt(val))  // TODO: check the operator
								check = true;
							  else
							  	check = false;	
							  break;						  							  							  							
				}
			}else{
				if(val.equals(payload[colPos-1]))
					check = true;
				else
					check = false;
			}
		}
		return check;
	}

	public static void Create_Meta_Files() {

		/** Create data directory at the current OS location to hold */
		try {
			File dataDir = new File("data");
			dataDir.mkdir();
			String[] oldTableFiles;
			oldTableFiles = dataDir.list();
			for (int i=0; i<oldTableFiles.length; i++) {
				File anOldFile = new File(dataDir, oldTableFiles[i]); 
				anOldFile.delete();
			}
		}
		catch (SecurityException se) {
			System.out.println(" Error- Directory cannot be created.");
			System.out.println(se);
		}

		try {
			davisbasetable = new RandomAccessFile("data/davisbase_tables.tbl", "rw");
			davisbasetable.setLength(512);
			davisbasetable.seek(0);
			davisbasetable.write(0x0D);// page type
			davisbasetable.write(0x02);// num cell
			int[] offset=new int[2];
			int size1=24;//table size
			int size2=25;// column size
			offset[0]=512-size1;
			offset[1]=offset[0]-size2;
			davisbasetable.writeShort(offset[1]);// map_data offset
			davisbasetable.writeInt(0);// rightmost
			davisbasetable.writeInt(10);// parent
			davisbasetable.writeShort(offset[1]);// cell arrary 1
			davisbasetable.writeShort(offset[0]);// cell arrary 2
			davisbasetable.seek(offset[0]);
			davisbasetable.writeShort(20);
			davisbasetable.writeInt(1); 
			davisbasetable.writeByte(1);
			davisbasetable.writeByte(28);
			davisbasetable.writeBytes("davisbase_tables");
			davisbasetable.seek(offset[1]);
			davisbasetable.writeShort(21);
			davisbasetable.writeInt(2); 
			davisbasetable.writeByte(1);
			davisbasetable.writeByte(29);
			davisbasetable.writeBytes("davisbase_columns");
		}
		catch (Exception e) {
			System.out.println("Error: database_tables file not created");
			System.out.println(e);
		}
		try {
			davisbasecolumn = new RandomAccessFile("data/davisbase_columns.tbl", "rw");
			davisbasecolumn.setLength(512);
			davisbasecolumn.seek(0);       
			davisbasecolumn.writeByte(0x0D); // page type: leaf page
			davisbasecolumn.writeByte(0x08); // number of cells
			int[] offset=new int[10];
			offset[0]=512-43;
			offset[1]=offset[0]-47;
			offset[2]=offset[1]-44;
			offset[3]=offset[2]-48;
			offset[4]=offset[3]-49;
			offset[5]=offset[4]-47;
			offset[6]=offset[5]-57;
			offset[7]=offset[6]-49;
			offset[8]=offset[7]-49;
			davisbasecolumn.writeShort(offset[8]); // map_data offset
			davisbasecolumn.writeInt(0); // rightmost
			davisbasecolumn.writeInt(0); // parent
			// cell array
			for(int i=0;i<9;i++)
				davisbasecolumn.writeShort(offset[i]);

			// data
			davisbasecolumn.seek(offset[0]);
			davisbasecolumn.writeShort(33); // 34
			davisbasecolumn.writeInt(1); 
			davisbasecolumn.writeByte(5);
			davisbasecolumn.writeByte(28);
			davisbasecolumn.writeByte(17);
			davisbasecolumn.writeByte(15);
			davisbasecolumn.writeByte(4);
			davisbasecolumn.writeByte(14);
			//davisbasecolumn.writeByte(15);
			davisbasecolumn.writeBytes("davisbase_tables"); // 16
			davisbasecolumn.writeBytes("rowid"); // 5
			davisbasecolumn.writeBytes("INT"); // 3
			davisbasecolumn.writeByte(1); // 1
			davisbasecolumn.writeBytes("NO"); // 2
			//davisbasecolumn.writeBytes("PRI");
			
			davisbasecolumn.seek(offset[1]);
			davisbasecolumn.writeShort(39); // 38
			davisbasecolumn.writeInt(2); 
			davisbasecolumn.writeByte(5);
			davisbasecolumn.writeByte(28);
			davisbasecolumn.writeByte(22);
			davisbasecolumn.writeByte(16);
			davisbasecolumn.writeByte(4);
			davisbasecolumn.writeByte(14);
			//davisbasecolumn.writeByte(0);
			davisbasecolumn.writeBytes("davisbase_tables"); // 16
			davisbasecolumn.writeBytes("table_name"); // 10  
			davisbasecolumn.writeBytes("TEXT"); // 4
			davisbasecolumn.writeByte(2); // 1
			davisbasecolumn.writeBytes("NO"); // 2
			//davisbasecolumn.writeByte(0);
			
			davisbasecolumn.seek(offset[2]);
			davisbasecolumn.writeShort(34); // 35
			davisbasecolumn.writeInt(3); 
			davisbasecolumn.writeByte(5);
			davisbasecolumn.writeByte(29);
			davisbasecolumn.writeByte(17);
			davisbasecolumn.writeByte(15);
			davisbasecolumn.writeByte(4);
			davisbasecolumn.writeByte(14);
			//davisbasecolumn.writeByte(15);
			davisbasecolumn.writeBytes("davisbase_columns");
			davisbasecolumn.writeBytes("rowid");
			davisbasecolumn.writeBytes("INT");
			davisbasecolumn.writeByte(1);
			davisbasecolumn.writeBytes("NO");
			//davisbasecolumn.writeBytes("PRI");
			
			davisbasecolumn.seek(offset[3]);
			davisbasecolumn.writeShort(40); // 39
			davisbasecolumn.writeInt(4); 
			davisbasecolumn.writeByte(5);
			davisbasecolumn.writeByte(29);
			davisbasecolumn.writeByte(22);
			davisbasecolumn.writeByte(16);
			davisbasecolumn.writeByte(4);
			davisbasecolumn.writeByte(14);
			//davisbasecolumn.writeByte(0);
			davisbasecolumn.writeBytes("davisbase_columns");
			davisbasecolumn.writeBytes("table_name");
			davisbasecolumn.writeBytes("TEXT");
			davisbasecolumn.writeByte(2);
			davisbasecolumn.writeBytes("NO");
			//davisbasecolumn.writeByte(0);

			
			davisbasecolumn.seek(offset[4]);
			davisbasecolumn.writeShort(41); // 40
			davisbasecolumn.writeInt(5); 
			davisbasecolumn.writeByte(5);
			davisbasecolumn.writeByte(29);
			davisbasecolumn.writeByte(23);
			davisbasecolumn.writeByte(16);
			davisbasecolumn.writeByte(4);
			davisbasecolumn.writeByte(14);
			//davisbasecolumn.writeByte(0);
			davisbasecolumn.writeBytes("davisbase_columns");
			davisbasecolumn.writeBytes("column_name");
			davisbasecolumn.writeBytes("TEXT");
			davisbasecolumn.writeByte(3);
			davisbasecolumn.writeBytes("NO");
			//davisbasecolumn.writeByte(0);
			
			davisbasecolumn.seek(offset[5]);
			davisbasecolumn.writeShort(39); // 38
			davisbasecolumn.writeInt(6); 
			davisbasecolumn.writeByte(5);
			davisbasecolumn.writeByte(29);
			davisbasecolumn.writeByte(21);
			davisbasecolumn.writeByte(16);
			davisbasecolumn.writeByte(4);
			davisbasecolumn.writeByte(14);
			//davisbasecolumn.writeByte(0);
			davisbasecolumn.writeBytes("davisbase_columns");
			davisbasecolumn.writeBytes("data_type");
			davisbasecolumn.writeBytes("TEXT");
			davisbasecolumn.writeByte(4);
			davisbasecolumn.writeBytes("NO");
			//davisbasecolumn.writeByte(0);
			
			davisbasecolumn.seek(offset[6]);
			davisbasecolumn.writeShort(49); // 48
			davisbasecolumn.writeInt(7); 
			davisbasecolumn.writeByte(5);
			davisbasecolumn.writeByte(29);
			davisbasecolumn.writeByte(28);
			davisbasecolumn.writeByte(19);
			davisbasecolumn.writeByte(4);
			davisbasecolumn.writeByte(14);
			//davisbasecolumn.writeByte(0);
			davisbasecolumn.writeBytes("davisbase_columns");
			davisbasecolumn.writeBytes("ordinal_position");
			davisbasecolumn.writeBytes("TINYINT");
			davisbasecolumn.writeByte(5);
			davisbasecolumn.writeBytes("NO");
			//davisbasecolumn.writeByte(0);
			
			davisbasecolumn.seek(offset[7]);
			davisbasecolumn.writeShort(41); // 40
			davisbasecolumn.writeInt(8); 
			davisbasecolumn.writeByte(5);
			davisbasecolumn.writeByte(29);
			davisbasecolumn.writeByte(23);
			davisbasecolumn.writeByte(16);
			davisbasecolumn.writeByte(4);
			davisbasecolumn.writeByte(14);
			davisbasecolumn.writeBytes("davisbase_columns");
			davisbasecolumn.writeBytes("is_nullable");
			davisbasecolumn.writeBytes("TEXT");
			davisbasecolumn.writeByte(6);
			davisbasecolumn.writeBytes("NO");
		}
		catch (Exception e) {
			System.out.println("Error: database_columns file was not created");
			System.out.println(e);
		}
	}
}

