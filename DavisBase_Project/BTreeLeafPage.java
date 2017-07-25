import java.util.HashMap;
import java.util.Locale;
import java.util.Arrays;
import java.util.Date;
import java.text.SimpleDateFormat;
import java.io.RandomAccessFile;
import java.io.File;
import java.io.FileReader;
import java.util.Scanner;
import java.util.SortedMap;
import java.util.ArrayList;


public class BTreeLeafPage{
	
	public static final String dp = "yyyy-MM-dd_HH:mm:ss";
	public static int pageSize = 512;
	public static void main(String[] args){}
	
	// Overflow will create a new leaf 
				public static int makeLeafPage(RandomAccessFile tablefile){
					int record_count = 0;
					try{
						record_count = (int)(tablefile.length()/(new Long(512)));
						record_count+=1;
						tablefile.setLength(512 * record_count);
						tablefile.seek((record_count-1)*512);
						tablefile.writeByte(0x0D);  // leaf page
					}catch(Exception e){
						
						System.out.println("Error at makeLeafPage...");
						System.out.println(e);
					}

					return record_count;

				}
	// return page number after changing lead node to inner node
	public static int ConvertInnerNode(RandomAccessFile tableFile){
		int num_rec = 0;
		try{
			num_rec = (int)(tableFile.length()/(new Long(512)));
			num_rec+=1;
			tableFile.setLength(512 * num_rec);
			tableFile.seek((num_rec-1)*512);
			tableFile.writeByte(0x05);  // inner node
		}catch(Exception e){
			e.printStackTrace();
		}

		return num_rec;
	}

	
	// to return middle key value
	public static int fetchIndexKeyMid(int pagenum,RandomAccessFile Tablefile){
		int val = 0;
		try{
			Tablefile.seek((pagenum-1)*512);
			byte pageType = Tablefile.readByte();
			// number of cells in current page
			int numCells = gettotalcells(Tablefile, pagenum);
			// id of mid cell
			int index = (int) Math.ceil((double) numCells / 2);
			long loc = getCelloffnum (Tablefile, pagenum, index-1);
			Tablefile.seek(loc);

			switch(pageType){
				case 0x05:
					val = Tablefile.readInt(); 
					val = Tablefile.readInt();
					break;
				case 0x0D:
					val = Tablefile.readShort();
					val = Tablefile.readInt();
					break;
			}

		}catch(Exception e){
			e.printStackTrace();
		}

		return val;
	}

	 
	// when overflow ocours..
	public static void ExpandOverflowLeafNode(RandomAccessFile tablefile, int origPagenum, int newPagenum){
		try{
			// number of cells in curent page
			int num_records = gettotalcells(tablefile, origPagenum);
			// id of mid cell
			int middlekey = (int) Math.ceil((double) num_records / 2);

			int leftoffset = middlekey - 1;
			int RightOffset = num_records - middlekey;
			int totalbytes = 512;

			for(int i = leftoffset; i < num_records; i++){
				long location = getCelloffnum (tablefile, origPagenum, i);
				// read cell size
				tablefile.seek(location);
				int data_cell = tablefile.readShort()+6;
				totalbytes = totalbytes - data_cell;
				// reading the data from the parent node...
				tablefile.seek(location);
				byte[] tempb = new byte[data_cell];
				tablefile.readFully(tempb);
				// writing the data to left child
				tablefile.seek((newPagenum-1)*512+totalbytes);
				tablefile.write(tempb);
				// fix cell array in the new page TODO
				setCellOffset(tablefile, newPagenum, i - leftoffset, totalbytes);
			}

			// write  the offsetss to new page node...
			tablefile.seek((newPagenum-1)*512+2);
			tablefile.writeShort(totalbytes);

			// rewrite offset in the original node...  
			short offset = getCellOffset(tablefile, origPagenum, leftoffset-1);
			tablefile.seek((origPagenum-1)*512+2);
			tablefile.writeShort(offset);

			// re-point  pointer for right most nodes..
			int rightpointer = getRightpointer(tablefile, origPagenum);
			setRightpointer(tablefile, newPagenum, rightpointer);
			setRightpointer(tablefile, origPagenum, newPagenum);

			// adjust the parent pointer
			int parent = getParent(tablefile, origPagenum);
			setParent(tablefile, newPagenum, parent);

			// rewrite starting offset values....
			byte temp = (byte) leftoffset;
			settotalcells(tablefile, origPagenum, temp);
			temp = (byte) RightOffset;
			settotalcells(tablefile, newPagenum, temp);
			
		}catch(Exception e){			
			e.printStackTrace();
		}
	}
	
	// split leaf node when overflow occurs..
		public static void ExpandOverflowLeaf(RandomAccessFile tablefile, int pagenum){
			int newPage = makeLeafPage(tablefile);
			int midindexKey = fetchIndexKeyMid(pagenum, tablefile);
			ExpandOverflowLeafNode(tablefile, pagenum, newPage);
			int parent = getParent(tablefile, pagenum);
			if(parent == 0){
				int root = ConvertInnerNode(tablefile);
				setParent(tablefile, pagenum, root);
				setParent(tablefile, newPage, root);
				setRightpointer(tablefile, root, newPage);
				addInnerNode (tablefile, root, pagenum, midindexKey);
			}else{
				long ploc = getPointerLocation(tablefile, pagenum, parent);
				setPointerLocation(tablefile, ploc, parent, newPage);
				addInnerNode (tablefile, parent, pagenum, midindexKey);
				SortDatCells(tablefile, parent);
				while(checkInteriorSpace(tablefile, parent)){
					parent = ExpandOverflowInterior(tablefile, parent);
				}
			}
		}
		
		
		
		
	
	// Modify pointer to new inner node
	public static void ExpandOverflowInnernode(RandomAccessFile tablefile, int origPagenum, int newPagenum){
		try{
			
			int num_records = gettotalcells(tablefile, origPagenum);			
			int middlekey = (int) Math.ceil((double) num_records / 2);

			int leftoffset = middlekey - 1;
			int rigthtoffset = num_records - leftoffset  - 1;
			short totalbytes = 512;

			for(int i = leftoffset+1; i < num_records; i++){
				long location = getCelloffnum (tablefile, origPagenum, i);
				// read cell size
				short data_len = 8;
				totalbytes = (short)(totalbytes - data_len);
				// read cell data
				tablefile.seek(location);
				byte[] cell = new byte[data_len];
				tablefile.read(cell);
				// write cell data
				tablefile.seek((newPagenum-1)*512+totalbytes);
				tablefile.write(cell);
				// fix parent pointer in target page
				tablefile.seek(location);
				int pagnum = tablefile.readInt();
				setParent(tablefile, pagnum, newPagenum);
				// fix cell arrary in new page
				setCellOffset(tablefile, newPagenum, i - (leftoffset + 1), totalbytes);
			}
			
			// re-point  pointer for right most nodes..
			int tmp = getRightpointer(tablefile, origPagenum);
			setRightpointer(tablefile, newPagenum, tmp);
			
			long midLocation = getCelloffnum (tablefile, origPagenum, middlekey - 1);
			tablefile.seek(midLocation);
			tmp = tablefile.readInt();
			setRightpointer(tablefile, origPagenum, tmp);
			
			
			// write content offset to new page
			tablefile.seek((newPagenum-1)*512+2);
			tablefile.writeShort(totalbytes);
			
			//rewrite the contents of offsets...
			short offset = getCellOffset(tablefile, origPagenum, leftoffset-1);
			tablefile.seek((origPagenum-1)*512+2);
			tablefile.writeShort(offset);

			// adjust parent pointer...
			int parent = getParent(tablefile, origPagenum);
			setParent(tablefile, newPagenum, parent);
			// fix cell number
			byte temp = (byte) leftoffset;
			settotalcells(tablefile, origPagenum, temp);
			temp = (byte) rigthtoffset;
			settotalcells(tablefile, newPagenum, temp);
			
		}catch(Exception e){
			e.printStackTrace();
		}
	}

		

	// expand inner nodes...
	public static int ExpandOverflowInterior(RandomAccessFile tablefile, int page){
		int newPage = ConvertInnerNode(tablefile);
		int midindexKey = fetchIndexKeyMid(page,tablefile);
		ExpandOverflowInnernode (tablefile, page, newPage);
		int parent = getParent(tablefile, page);
		if(parent == 0){
			int rootPage = ConvertInnerNode(tablefile);
			setParent(tablefile, page, rootPage);
			setParent(tablefile, newPage, rootPage);
			setRightpointer(tablefile, rootPage, newPage);
			addInnerNode (tablefile, rootPage, page, midindexKey);
			return rootPage;
		}else{
			long location = getPointerLocation(tablefile, page, parent);
			setPointerLocation(tablefile, location, parent, newPage);
			addInnerNode (tablefile, parent, page, midindexKey);
			SortDatCells(tablefile, parent);
			return parent;
		}
	}

	
	// set the offset as child pointer...
	public static void setPointerLocation(RandomAccessFile tablefile, long location, int parent, int page){
		try{
			if(location == 0){
				tablefile.seek((parent-1)*512+4);
			}else{
				tablefile.seek(location);
			}
			tablefile.writeInt(page);
		}catch(Exception e){
			System.out.println("Error at setPointerLoc");
		}
	} 
	
	// get child pointer file from parent page
		public static long getPointerLocation(RandomAccessFile tablefile, int page, int parent){
			long value = 0;
			try{
				int numCells = new Integer(gettotalcells(tablefile, parent));
				for(int i=0; i < numCells; i++){
					long loc = getCelloffnum (tablefile, parent, i);
					tablefile.seek(loc);
					int childPage = tablefile.readInt();
					if(childPage == page){
						value = loc;
					}
				}
			}catch(Exception e){
				System.out.println("Error at getPointerLoc");
			}

			return value;
		}
	
	public static void SortDatCells(RandomAccessFile tablefile, int page){
		 byte num = gettotalcells(tablefile, page);
		 short[] cells = getCells(tablefile, page);
		 int[] keys = getKeys(tablefile, page);
		 
		 int ltmp; short rtmp;
		
		 for (int i = 1; i < num; i++) {
            for(int j = i ; j > 0 ; j--){
                if(keys[j] < keys[j-1]){
              // swap the keys....
                    ltmp = keys[j];
                    keys[j] = keys[j-1];
                    keys[j-1] = ltmp;
              //swap the data cells as well..
                    rtmp = cells[j];
                    cells[j] = cells[j-1];
                    cells[j-1] = rtmp;
                }
            }
         }

         try{
        	 tablefile.seek((page-1)*512+12);
         	for(int i = 0; i < num; i++){
         		tablefile.writeShort(cells[i]);
			}
         }catch(Exception e){
         	System.out.println("Error at SortDatCells");
         }
	}

	public static short[] getCells(RandomAccessFile tablefile, int page){
		int num = new Integer(gettotalcells(tablefile, page));
		short[] cells = new short[num];

		try{
			tablefile.seek((page-1)*512+12);
			for(int i = 0; i < num; i++){
				cells[i] = tablefile.readShort();
			}
		}catch(Exception e){
			e.printStackTrace();
		}

		return cells;
	}
	
	public static int[] getKeys(RandomAccessFile tablefile, int page){
		int num = new Integer(gettotalcells(tablefile, page));
		int[] keys = new int[num];

		try{
			tablefile.seek((page-1)*512);
			byte pageType = tablefile.readByte();
			byte offset = 0;
			switch(pageType){
			    case 0x0d: // leaf node
				offset = 2;
				break;
				case 0x05://inner node..
					offset = 4;
					break;
				default:
					offset = 2;
					break;
			}

			for(int i = 0; i < num; i++){
				long loc = getCelloffnum (tablefile, page, i);
				tablefile.seek(loc+offset);
				keys[i] = tablefile.readInt();
			}

		}catch(Exception e){
			e.printStackTrace();
		}

		return keys;
	}

	

	// return the parent page number of page
	public static int getParent(RandomAccessFile tablefile, int page){
		int value = 0;

		try{
			tablefile.seek((page-1)*512+8);
			value = tablefile.readInt();
		}catch(Exception e){
			System.out.println("Error at getParent");
		}

		return value;
	}

	public static void setParent(RandomAccessFile tablefile, int page, int parent){
		try{
			tablefile.seek((page-1)*512+8);
			tablefile.writeInt(parent);
		}catch(Exception e){
			System.out.println("Error at setParent");
		}
	}



	// inserting the key pairs to inner node..
	public static void addInnerNode(RandomAccessFile tablefile, int page, int child, int key){
		try{
			// find location
			tablefile.seek((page-1)*512+2);
			short content = tablefile.readShort();
			if(content == 0)
				content = 512;
			content = (short)(content - 8);
			// write data
			tablefile.seek((page-1)*512+content);
			tablefile.writeInt(child);
			tablefile.writeInt(key);
			// fix content
			tablefile.seek((page-1)*512+2);
			tablefile.writeShort(content);
			byte temp = gettotalcells(tablefile, page);
			setCellOffset(tablefile, page ,temp, content);
			// fix number of cell
			temp = (byte)(temp + 1);
			settotalcells(tablefile, page, temp);

		}catch(Exception e){
			e.printStackTrace();
		}
	}

	// insert a cell in to leaf page
	public static void addLeafNode(RandomAccessFile tablefile, int page, int offset, short Sz, int key, byte[] stc, String[] vals){
		try{
			//for(byte i:stc)
				//System.out.println(i);
			String s;
			tablefile.seek((page-1)*512+offset);
			tablefile.writeShort(Sz);
			tablefile.writeInt(key);
			int col = vals.length - 1;
			tablefile.writeByte(col);
			tablefile.write(stc);
			for(int i = 1; i < vals.length; i++){
				switch(stc[i-1]){
					case 0x00:
						tablefile.writeByte(0);
						break;
					case 0x01:
						tablefile.writeShort(0);
						break;
					case 0x02:
						tablefile.writeInt(0);
						break;
					case 0x03:
						tablefile.writeLong(0);
						break;
					case 0x04:
						tablefile.writeByte(new Byte(vals[i]));
						break;
					case 0x05:
						tablefile.writeShort(new Short(vals[i]));
						break;
					case 0x06:
						tablefile.writeInt(new Integer(vals[i]));
						break;
					case 0x07:
						tablefile.writeLong(new Long(vals[i]));
						break;
					case 0x08:
						tablefile.writeFloat(new Float(vals[i]));
						break;
					case 0x09:
						tablefile.writeDouble(new Double(vals[i]));
						break;
					case 0x0A:
						s = vals[i];
						Date temp = new SimpleDateFormat(dp).parse(s.substring(1, s.length()-1));
						long time = temp.getTime();
						tablefile.writeLong(time);
						break;
					case 0x0B:
						s = vals[i];
						s = s.substring(1, s.length()-1);
						System.out.println(s);
						s = s+"_00:00:00";
						Date temp2 = new SimpleDateFormat(dp,Locale.US).parse(s);
						long time2 = temp2.getTime();
						tablefile.writeLong(time2);
						break;
					default:
						tablefile.writeBytes(vals[i]);
						break;
				}
			}
			int n = gettotalcells(tablefile, page);
			byte tmp = (byte) (n+1);
			settotalcells(tablefile, page, tmp);
			tablefile.seek((page-1)*512+12+n*2);
			tablefile.writeShort(offset);
			tablefile.seek((page-1)*512+2);
			int content = tablefile.readShort();
			if(content >= offset || content == 0){
				tablefile.seek((page-1)*512+2);
				tablefile.writeShort(offset);
			}
		}catch(Exception e){
			System.out.println("Error at insertLeafCell");
			e.printStackTrace();
		}
	}

	public static void updateLeafnode(RandomAccessFile tablefile, int page, int offset, int plsize, int key, byte[] stc, String[] vals){
		try{
			String s;
			tablefile.seek((page-1)*512+offset);
			tablefile.writeShort(plsize);
			tablefile.writeInt(key);
			int col = vals.length - 1;
			tablefile.writeByte(col);
			tablefile.write(stc);
			for(int i = 1; i < vals.length; i++){
				switch(stc[i-1]){
					case 0x00:
						tablefile.writeByte(0);
						break;
					case 0x01:
						tablefile.writeShort(0);
						break;
					case 0x02:
						tablefile.writeInt(0);
						break;
					case 0x03:
						tablefile.writeLong(0);
						break;
					case 0x04:
						tablefile.writeByte(new Byte(vals[i]));
						break;
					case 0x05:
						tablefile.writeShort(new Short(vals[i]));
						break;
					case 0x06:
						tablefile.writeInt(new Integer(vals[i]));
						break;
					case 0x07:
						tablefile.writeLong(new Long(vals[i]));
						break;
					case 0x08:
						tablefile.writeFloat(new Float(vals[i]));
						break;
					case 0x09:
						tablefile.writeDouble(new Double(vals[i]));
						break;
					case 0x0A:
						s = vals[i];
						Date temp = new SimpleDateFormat(dp).parse(s.substring(1, s.length()-1));
						long time = temp.getTime();
						tablefile.writeLong(time);
						break;
					case 0x0B:
						s = vals[i];
						s = s.substring(1, s.length()-1);
						s = s+"_00:00:00";
						Date temp2 = new SimpleDateFormat(dp).parse(s);
						long time2 = temp2.getTime();
						tablefile.writeLong(time2);
						break;
					default:
						tablefile.writeBytes(vals[i]);
						break;
				}
			}
		}catch(Exception e){
			e.printStackTrace();
		}
	}
  
	public static int getRightpointer(RandomAccessFile tablefile, int page){
		int value = 0;

		try{
			tablefile.seek((page-1)*512+4);
			value = tablefile.readInt();
		}catch(Exception e){
			e.printStackTrace();
		}

		return value;
	}

	public static void setRightpointer(RandomAccessFile file, int page, int rightMost){

		try{
			file.seek((page-1)*512+4);
			file.writeInt(rightMost);
		}catch(Exception e){
			System.out.println("Error at setRightMost");
		}

	}
	public static long getCellLoc(RandomAccessFile file, int page, int id){
		long loc = 0;
		try{
			file.seek((page-1)*pageSize+12+id*2);
			short offset = file.readShort();
			long orig = (page-1)*pageSize;
			loc = orig + offset;
		}catch(Exception e){
			System.out.println(e);
		}
		return loc;
	}
	// total number of records...
	public static byte gettotalcells(RandomAccessFile tablefile, int page){
		byte totalcells = 0;

		try{
			tablefile.seek((page-1)*512+1);
			totalcells = tablefile.readByte();
		}catch(Exception e){
			System.out.println(e);
			System.out.println("Error at gettotalcells");
		}

		return totalcells;
	}

	public static void settotalcells(RandomAccessFile file, int page, byte num){
		try{
			file.seek((page-1)*512+1);
			file.writeByte(num);
		}catch(Exception e){
			System.out.println("Error at settotalcells");
		}
	}
	public static short[] getCellArray(RandomAccessFile file, int page){
		int num = new Integer(getCellNumber(file, page));
		short[] array = new short[num];

		try{
			file.seek((page-1)*pageSize+12);
			for(int i = 0; i < num; i++){
				array[i] = file.readShort();
			}
		}catch(Exception e){
			System.out.println(e);
		}

		return array;
	}
	public static byte getPageType(RandomAccessFile file, int page){
		byte type=0x05;
		try {
			file.seek((page-1)*pageSize);
			type = file.readByte();
		} catch (Exception e) {
			System.out.println(e);
		}
		return type;
	}
	// assmue interior page has 10 more key implys full
	
	public static boolean checkInteriorSpace(RandomAccessFile file, int page){
		byte numCells = gettotalcells(file, page);
		if(numCells > 30)
			return true;
		else
			return false;
	}

	// 
	public static int checkLeafSpace(RandomAccessFile file, int page, int size){
		int val = -1;

		try{
			file.seek((page-1)*512+2);
			int content = file.readShort();
			if(content == 0)
				return 512 - size;
			int numCells = gettotalcells(file, page);
			int space = content - 20 - 2*numCells;
			if(size < space)
				return content - size;
			
		}catch(Exception e){
			System.out.println("Error at checkLeafSpace");
		}

		return val;
	}

	// to search for a key...
	public static boolean hasKey(RandomAccessFile tablefile, int page, int key){
		int[] array = getKeys(tablefile, page);
		for(int i : array)
			if(key == i)
				return true;
		return false;
	}
	public static void setCellNumber(RandomAccessFile file, int page, byte num){
		try{
			file.seek((page-1)*pageSize+1);
			file.writeByte(num);
		}catch(Exception e){
			System.out.println(e);
		}
	}
	// read location of cell in the page, for inner nodes
	public static long getCelloffnum(RandomAccessFile tablefile, int page, int id){
		long location = 0;
		try{
			tablefile.seek((page-1)*512+12+id*2);
			short offset = tablefile.readShort();
			long original = (page-1)*512;
			location = original + offset;
		}catch(Exception e){
			e.printStackTrace();
		}
		return location;
	}
public static byte getCellNumber(RandomAccessFile file, int page){
		byte val = 0;

		try{
			file.seek((page-1)*pageSize+1);
			val = file.readByte();
		}catch(Exception e){
			System.out.println(e);
		}

		return val;
	}
	// this is for leaf nodess..
	public static short getCellOffset(RandomAccessFile tablefile, int page, int id){
		short offset = 0;
		try{
			tablefile.seek((page-1)*512+12+id*2);
			offset = tablefile.readShort();
		}catch(Exception e){
			System.out.println("Error at getCelloffnum ");
		}
		return offset;
	}

	public static void setCellOffset(RandomAccessFile tablefile, int page, int id, int offset){
		try{
			tablefile.seek((page-1)*512+12+id*2);
			tablefile.writeShort(offset);
		}catch(Exception e){
			System.out.println("Error at setCellOffset");
		}
	}
	
	
}