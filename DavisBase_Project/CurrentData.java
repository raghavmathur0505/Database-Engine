import java.util.SortedMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.RandomAccessFile;
import java.io.FileReader;
import java.io.File;
import java.util.Scanner;



// information of current data
class CurrentData{
	public int[] format; // info of view format 
	public String[] columnName; // column name of all columns
	public int row_count; // count_rows in the CurrentData
	public HashMap<Integer, String[]> map_data;
	// constructor for initialization
	public CurrentData(){
		row_count = 0;
		map_data = new HashMap<Integer, String[]>();
	}

	// add data into map_data container. increase stored row number
	public void add(int row_no, String[] data){
		map_data.put(row_no, data);
		row_count = row_count + 1;
	}

	// update the view format
	public void updateFormat(){
		for(int i = 0; i < format.length; i++)
			format[i] = columnName[i].length();
		for(String[] i : map_data.values()){
			for(int j = 0; j < i.length; j++)
				if(format[j] < i[j].length())
					format[j] = i[j].length();
		}
	}

	// make the string s to be fix length of len. filled with space
	public String fix(int len, String s) {
		return String.format("%-"+(len+3)+"s", s);
	}

	// make a length of len using compose with stirng s
	public String line(String s,int len) {
		String a = "";
		for(int i=0;i<len;i++) {
			a += s;
		}
		return a;
	}

	// display map_data according to the format controller. col specify selected columns to display
	public void showData(String[] col){
		// if the map_data container if empty, output info
		if(row_count == 0){
			System.out.println("No records Found.");
		}else{
			// called function to update format controller
			updateFormat();
			// if selectd column is "*", means display all columns
			if(col[0].equals("*")){
				// print line
				for(int l: format)
					System.out.print(line("=", l+3));
				System.out.println();
				// print column name
				for(int j = 0; j < columnName.length; j++)
					System.out.print(fix(format[j], columnName[j])+"||");
				System.out.println();
				// print line
				for(int l: format)
					System.out.print(line("=", l+3));
				System.out.println();
				// print data 
				for(String[] i : map_data.values()){
					for(int j = 0; j < i.length; j++)
						System.out.print(fix(format[j], i[j])+"||");
					System.out.println();
				}
				System.out.println();
			// else output selected column
			}else{
				int[] control = new int[col.length];
				for(int j = 0; j < col.length; j++)
					for(int i = 0; i < columnName.length; i++)
						if(col[j].equals(columnName[i]))
							control[j] = i;
				// print line
				for(int j = 0; j < control.length; j++)
					System.out.print(line("=", format[control[j]]+3));
				System.out.println();
				// print column name
				for(int j = 0; j < control.length; j++)
					System.out.print(fix(format[control[j]], columnName[control[j]])+"||");
				System.out.println();
				// print line
				for(int j = 0; j < control.length; j++)
					System.out.print(line("=", format[control[j]]+3));
				System.out.println();
				// print data
				for(String[] i : map_data.values()){
					for(int j = 0; j < control.length; j++)
						System.out.print(fix(format[control[j]], i[control[j]])+"||");
					System.out.println();
				}
				System.out.println();
			}
		}
	}
}