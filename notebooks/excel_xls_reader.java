package automation.suite.util;

import org.apache.poi.common.usermodel.HyperlinkType;
import org.apache.poi.hssf.util.HSSFColor;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.FillPatternType;
import org.apache.poi.ss.usermodel.IndexedColors;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFCreationHelper;
import org.apache.poi.xssf.usermodel.XSSFFont;
import org.apache.poi.xssf.usermodel.XSSFHyperlink;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;


public class Xls_Reader {
	public static String filename = System.getProperty("user.dir")+"\\src\\test\\resources\\TestData.xlsx";
	public  static String path;
	public  static FileInputStream fis = null;
	public  static FileOutputStream fileOut =null;
	private static XSSFWorkbook workbook = null;
	private static XSSFSheet sheet = null;
	private static XSSFRow row   =null;
	private static XSSFCell cell = null;
	
	public Xls_Reader(String path) {
		
		this.path=path;
		try {
			fis = new FileInputStream(path);
			workbook = new XSSFWorkbook(fis);
			sheet = workbook.getSheetAt(0);
			fis.close();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} 
		
	}
	// returns the row count in a sheet
	public int getRowCount(String sheetName){
		int index = workbook.getSheetIndex(sheetName);
		if(index==-1)
			return 0;
		else{
		sheet = workbook.getSheetAt(index);
		int number=sheet.getLastRowNum()+1;
		return number;
		}
		
	}
	
	// returns the data from a cell
	public String getCellData(String sheetName, String colName, int rowNum) {
		try {
			if (rowNum <= 0)
				return "";

			int index = workbook.getSheetIndex(sheetName);
			int col_Num = -1;
			if (index == -1)
				return "";

			sheet = workbook.getSheetAt(index);
			row = sheet.getRow(0);
			for (int i = 0; i < row.getLastCellNum(); i++) {
				// System.out.println(row.getCell(i).getStringCellValue().trim());
				if (row.getCell(i).getStringCellValue().trim().equals(colName.trim()))
					col_Num = i;
			}
			if (col_Num == -1)
				return "";

			sheet = workbook.getSheetAt(index);
			row = sheet.getRow(rowNum - 1);
			if (row == null)
				return "";
			cell = row.getCell(col_Num);

			if (cell == null)
				return "";
			// System.out.println(cell.getCellType());
			if (cell.getCellType() == CellType.STRING) {
				return cell.getStringCellValue();
			}else if (cell.getCellType() == CellType.NUMERIC || cell.getCellType() == CellType.FORMULA){

				String cellText = String.valueOf(cell.getNumericCellValue());
				
				if (DateUtil.isCellDateFormatted(cell)) {
					// format in form of M/D/YY
					double d = cell.getNumericCellValue();

					Calendar cal = Calendar.getInstance();
					cal.setTime(DateUtil.getJavaDate(d));
					cellText = (String.valueOf(cal.get(Calendar.YEAR))).substring(2);
					cellText = cal.get(Calendar.DAY_OF_MONTH) + "/" + cal.get(Calendar.MONTH) + 1 + "/" + cellText;

					// System.out.println(cellText);

				}

				return cellText;
			} else if (cell.getCellType() == CellType.BLANK)
				return "";
			else
				return String.valueOf(cell.getBooleanCellValue());

		} catch (Exception e) {

			e.printStackTrace();
			return "row " + rowNum + " or column " + colName + " does not exist in xls";
		}
	}
	
	// returns the data from a cell
	public String getCellData(String sheetName,int colNum,int rowNum){
		try{
			if(rowNum <=0)
				return "";
		
		int index = workbook.getSheetIndex(sheetName);

		if(index==-1)
			return "";
		
	
		sheet = workbook.getSheetAt(index);
		row = sheet.getRow(rowNum-1);
		if(row==null)
			return "";
		cell = row.getCell(colNum);
		if(cell==null)
			return "";
		
	  if(cell.getCellType()==CellType.STRING)
		  return cell.getStringCellValue();
	  else if(cell.getCellType()==CellType.NUMERIC || cell.getCellType()==CellType.FORMULA ){
		  
		  String cellText  = String.valueOf(cell.getNumericCellValue());
		  if (DateUtil.isCellDateFormatted(cell)) {
	           // format in form of M/D/YY
			  double d = cell.getNumericCellValue();

			  Calendar cal =Calendar.getInstance();
			  cal.setTime(DateUtil.getJavaDate(d));
	            cellText =
	             (String.valueOf(cal.get(Calendar.YEAR))).substring(2);
	           cellText = cal.get(Calendar.MONTH)+1 + "/" +
	                      cal.get(Calendar.DAY_OF_MONTH) + "/" +
	                      cellText;
	           
	          // System.out.println(cellText);

	         }

		  
		  
		  return cellText;
	  }else if(cell.getCellType()==CellType.BLANK)
	      return "";
	  else 
		  return String.valueOf(cell.getBooleanCellValue());
		}
		catch(Exception e){
			
			e.printStackTrace();
			return "row "+rowNum+" or column "+colNum +" does not exist  in xls";
		}
	}
	
	// returns true if data is set successfully else false
	public boolean setCellData(String sheetName,String colName,int rowNum, String data){
		try{
		fis = new FileInputStream(path); 
		workbook = new XSSFWorkbook(fis);

		if(rowNum<=0)
			return false;
		
		int index = workbook.getSheetIndex(sheetName);
		int colNum=-1;
		if(index==-1)
			return false;
		
		
		sheet = workbook.getSheetAt(index);
		

		row=sheet.getRow(0);
		for(int i=0;i<row.getLastCellNum();i++){
			//System.out.println(row.getCell(i).getStringCellValue().trim());
			if(row.getCell(i).getStringCellValue().trim().equals(colName))
				colNum=i;
		}
		if(colNum==-1)
			return false;

		sheet.autoSizeColumn(colNum); 
		row = sheet.getRow(rowNum-1);
		if (row == null)
			row = sheet.createRow(rowNum-1);
		
		cell = row.getCell(colNum);	
		if (cell == null)
	        cell = row.createCell(colNum);

	    // cell style
	    //CellStyle cs = workbook.createCellStyle();
	    //cs.setWrapText(true);
	    //cell.setCellStyle(cs);
	    cell.setCellValue(data);

	    fileOut = new FileOutputStream(path);

		workbook.write(fileOut);

	    fileOut.close();	

		}
		catch(Exception e){
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	
	// returns true if data is set successfully else false
	public boolean setCellData(String sheetName,String colName,int rowNum, String data,String url){
		//System.out.println("setCellData setCellData******************");
		try{
		fis = new FileInputStream(path); 
		workbook = new XSSFWorkbook(fis);

		if(rowNum<=0)
			return false;
		
		int index = workbook.getSheetIndex(sheetName);
		int colNum=-1;
		if(index==-1)
			return false;
		
		
		sheet = workbook.getSheetAt(index);
		//System.out.println("A");
		row=sheet.getRow(0);
		for(int i=0;i<row.getLastCellNum();i++){
			//System.out.println(row.getCell(i).getStringCellValue().trim());
			if(row.getCell(i).getStringCellValue().trim().equalsIgnoreCase(colName))
				colNum=i;
		}
		
		if(colNum==-1)
			return false;
		sheet.autoSizeColumn(colNum); //ashish
		row = sheet.getRow(rowNum-1);
		if (row == null)
			row = sheet.createRow(rowNum-1);
		
		cell = row.getCell(colNum);	
		if (cell == null)
	        cell = row.createCell(colNum);
			
	    cell.setCellValue(data);
	    XSSFCreationHelper createHelper = workbook.getCreationHelper();

	    //cell style for hyperlinks
	    //by default hypelrinks are blue and underlined
	    CellStyle hlink_style = workbook.createCellStyle();
	    XSSFFont hlink_font = workbook.createFont();
	    hlink_font.setUnderline(XSSFFont.U_SINGLE);
	    hlink_font.setColor(IndexedColors.BLUE.getIndex());
	    hlink_style.setFont(hlink_font);
	    //hlink_style.setWrapText(true);

	    XSSFHyperlink link = createHelper.createHyperlink(HyperlinkType.FILE);
	    link.setAddress(url);
	    cell.setHyperlink(link);
	    cell.setCellStyle(hlink_style);
	      
	    fileOut = new FileOutputStream(path);
		workbook.write(fileOut);

	    fileOut.close();	

		}
		catch(Exception e){
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	
	
	// returns true if sheet is created successfully else false
	public boolean addSheet(String  sheetname){		
		
		FileOutputStream fileOut;
		try {
			 workbook.createSheet(sheetname);	
			 fileOut = new FileOutputStream(path);
			 workbook.write(fileOut);
		     fileOut.close();		    
		} catch (Exception e) {			
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	// returns true if sheet is removed successfully else false if sheet does not exist
	public boolean removeSheet(String sheetName){		
		int index = workbook.getSheetIndex(sheetName);
		if(index==-1)
			return false;
		
		FileOutputStream fileOut;
		try {
			workbook.removeSheetAt(index);
			fileOut = new FileOutputStream(path);
			workbook.write(fileOut);
		    fileOut.close();		    
		} catch (Exception e) {			
			e.printStackTrace();
			return false;
		}
		return true;
	}
	// returns true if column is created successfully
	public boolean addColumn(String sheetName,String colName){
		//System.out.println("**************addColumn*********************");
		
		try{				
			fis = new FileInputStream(path); 
			workbook = new XSSFWorkbook(fis);
			int index = workbook.getSheetIndex(sheetName);
			if(index==-1)
				return false;
			
		XSSFCellStyle style = workbook.createCellStyle();
		style.setFillForegroundColor(HSSFColor.HSSFColorPredefined.GREY_40_PERCENT.getIndex());
		style.setFillPattern(FillPatternType.SOLID_FOREGROUND);
		
		sheet=workbook.getSheetAt(index);
		
		row = sheet.getRow(0);
		if (row == null)
			row = sheet.createRow(0);
		
		//cell = row.getCell();	
		//if (cell == null)
		//System.out.println(row.getLastCellNum());
		if(row.getLastCellNum() == -1)
			cell = row.createCell(0);
		else
			cell = row.createCell(row.getLastCellNum());
	        
	        cell.setCellValue(colName);
	        cell.setCellStyle(style);
	        
	        fileOut = new FileOutputStream(path);
			workbook.write(fileOut);
		    fileOut.close();		    

		}catch(Exception e){
			e.printStackTrace();
			return false;
		}
		
		return true;
		
		
	}
	// removes a column and all the contents
	public boolean removeColumn(String sheetName, int colNum) {
		try{
		if(!isSheetExist(sheetName))
			return false;
		fis = new FileInputStream(path); 
		workbook = new XSSFWorkbook(fis);
		sheet=workbook.getSheet(sheetName);
		XSSFCellStyle style = workbook.createCellStyle();
		style.setFillForegroundColor(HSSFColor.HSSFColorPredefined.GREY_40_PERCENT.getIndex());
		XSSFCreationHelper createHelper = workbook.getCreationHelper();
		style.setFillPattern(FillPatternType.NO_FILL);
		
		
	    
	
		for(int i =0;i<getRowCount(sheetName);i++){
			row=sheet.getRow(i);	
			if(row!=null){
				cell=row.getCell(colNum);
				if(cell!=null){
					cell.setCellStyle(style);
					row.removeCell(cell);
				}
			}
		}
		fileOut = new FileOutputStream(path);
		workbook.write(fileOut);
	    fileOut.close();
		}
		catch(Exception e){
			e.printStackTrace();
			return false;
		}
		return true;
		
	}
  // find whether sheets exists	
	public boolean isSheetExist(String sheetName){
		int index = workbook.getSheetIndex(sheetName);
		if(index==-1){
			index=workbook.getSheetIndex(sheetName.toUpperCase());
				if(index==-1)
					return false;
				else
					return true;
		}
		else
			return true;
	}
	
	// returns number of columns in a sheet	
	public int getColumnCount(String sheetName){
		// check if sheet exists
		if(!isSheetExist(sheetName))
		 return -1;
		
		sheet = workbook.getSheet(sheetName);
		row = sheet.getRow(0);
		
		if(row==null)
			return -1;
		
		return row.getLastCellNum();
		
		
		
	}
	//String sheetName, String testCaseName,String keyword ,String URL,String message
	public boolean addHyperLink(String sheetName,String screenShotColName,String testCaseName,int index,String url,String message){
		//System.out.println("ADDING addHyperLink******************");
		
		url=url.replace('\\', '/');
		if(!isSheetExist(sheetName))
			 return false;
		
	    sheet = workbook.getSheet(sheetName);
	    
	    for(int i=2;i<=getRowCount(sheetName);i++){
	    	if(getCellData(sheetName, 0, i).equalsIgnoreCase(testCaseName)){
	    		//System.out.println("**caught "+(i+index));
	    		setCellData(sheetName, screenShotColName, i+index, message,url);
	    		break;
	    	}
	    }


		return true; 
	}
	public int getCellRowNum(String sheetName,String colName,String cellValue){
		for(int i=2;i<=getRowCount(sheetName);i++){
	    	if(getCellData(sheetName,colName , i).equalsIgnoreCase(cellValue)){
	    		return i;
	    	}
	    }
		return -1;
	}
		

	
	public static void setExcelFile(String Path,String SheetName) throws Exception {
		try {
			// Open the Excel file
			FileInputStream ExcelFile = new FileInputStream(Path);
			// Access the required test data sheet
			workbook = new XSSFWorkbook(ExcelFile);
			sheet = workbook.getSheet(SheetName);
		} catch (Exception e){
			throw (e);
		}
	}

	public static Object[][] getTableArray_Conditional(String FilePath, String SheetName, String dataset, String environment, String priority) throws Exception {   
		String[][] tabArray = null;
		try {
			FileInputStream ExcelFile = new FileInputStream(FilePath);
			// Access the required test data sheet
			workbook = new XSSFWorkbook(ExcelFile);
			sheet = workbook.getSheet(SheetName);
			int startRow = 1;
			int startCol = 0;
			int ci,cj;
			int totalRows = sheet.getLastRowNum();
			int totalCols = (sheet.getRow(0).getLastCellNum());

			//get the number of rows to be used as a data set based on dataset and environment
			int countRow = 0;
			for (int k = 1; k <= totalRows; k++) {
				if (getCellData(k,0).equalsIgnoreCase(dataset) && (getCellData(k, 1).equalsIgnoreCase(environment))) {
					countRow++;
				}
			}

			System.out.println("Count row: "+countRow);
			tabArray = new String[countRow][totalCols];
			ci=0;

			for (int i=startRow;i<=totalRows ;i++) {           	   
				cj=0;
				if(getCellData(i,0).equalsIgnoreCase(dataset) && (getCellData(i, 1).equalsIgnoreCase(environment))) {
					for (int j=startCol;j<totalCols;j++, cj++){
						tabArray[ci][cj]=getCellData(i,j);
						System.out.println(tabArray[ci][cj]);
					}
					ci++;
				}
			}
			System.out.println("TabArray length : "+tabArray.length);
		}
		catch (FileNotFoundException e){
			System.out.println("Could not read the Excel sheet");
			e.printStackTrace();
		}
		catch (IOException e){
			System.out.println("Could not read the Excel sheet");
			e.printStackTrace();
		}
		return(tabArray);

	}
	
	//overloaded function
	public static Object[][] getTableArray_Conditional(String FilePath, String SheetName, String dataset, String priority) throws Exception {   
		String[][] tabArray = null;
		try {
			FileInputStream ExcelFile = new FileInputStream(FilePath);
			// Access the required test data sheet
			workbook = new XSSFWorkbook(ExcelFile);
			sheet = workbook.getSheet(SheetName);
			int startRow = 1;
			int startCol = 0;
			int ci,cj;
			int totalRows = sheet.getLastRowNum();
			int totalCols = (sheet.getRow(0).getLastCellNum());

			//get the number of rows to be used as a data set based on dataset and environment
			int countRow = 0;
			int priorityDetermined=0;
			if (priority.equalsIgnoreCase("High")) {
				priorityDetermined=11;
			}
			else if (priority.equalsIgnoreCase("Medium")) {
				priorityDetermined=12;
			}
			else if (priority.equalsIgnoreCase("Low")) {
				priorityDetermined=13;
			}			
			for (int k = 1; k <= totalRows; k++) {
				if (getCellData(k,0).equalsIgnoreCase(dataset) && getCellData(k, priorityDetermined).equalsIgnoreCase("Y")) {
					countRow++;
				}
			}

			System.out.println("Count row: "+countRow);
			tabArray = new String[countRow][totalCols];
			ci=0;
			for (int i=startRow;i<=totalRows ;i++) {           	   
				cj=0;

//				if(getCellData(i,0).equalsIgnoreCase(dataset) ) {
//
//					for (int j=startCol;j<totalCols;j++, cj++){
//
//						tabArray[ci][cj]=getCellData(i,j);
//					}
//					ci++;
//				}

				if (priority.equalsIgnoreCase("High")) {
					if (getCellData(i, 0).equalsIgnoreCase(dataset) && getCellData(i, 11).equalsIgnoreCase("Y")) {
						for (int j = startCol; j < totalCols; j++, cj++) {
							tabArray[ci][cj] = getCellData(i, j);
						}
						ci++;
					}
				} else if (priority.equalsIgnoreCase("Medium")) {
					if (getCellData(i, 0).equalsIgnoreCase(dataset) && getCellData(i, 12).equalsIgnoreCase("Y")) {
						for (int j = startCol; j < totalCols; j++, cj++) {
							tabArray[ci][cj] = getCellData(i, j);
						}
						ci++;
					}

				} else if (priority.equalsIgnoreCase("Low")) {
					if (getCellData(i, 0).equalsIgnoreCase(dataset) && getCellData(i, 13).equalsIgnoreCase("Y")) {
						for (int j = startCol; j < totalCols; j++, cj++) {
							tabArray[ci][cj] = getCellData(i, j);
						}
						ci++;
					}
				}
			}

			System.out.println("TabArray length : "+tabArray.length);

		}

		catch (FileNotFoundException e){
			System.out.println("Could not read the Excel sheet");
			e.printStackTrace();

		}

		catch (IOException e){
			System.out.println("Could not read the Excel sheet");
			e.printStackTrace();

		}
		return(tabArray);
	}


	public static String getCellData(int RowNum, int ColNum) throws Exception {
		try{
			cell = sheet.getRow(RowNum).getCell(ColNum);
			if(cell == null) {
				return "";
			}
			else {
				if (cell.getCellType() == CellType.NUMERIC)
					{
					return "";
				}else{
					String CellData = cell.getStringCellValue();
					return CellData;
				}
			}
		}
		catch (Exception e){
			System.out.println(e.getMessage());
			throw (e);
		}

	}

	public static String getTestCaseName(String sTestCase)throws Exception{
		String value = sTestCase;
		try{
			int posi = value.indexOf("@");
			value = value.substring(0, posi);
			posi = value.lastIndexOf("."); 
			value = value.substring(posi + 1);
			return value;

		}catch (Exception e){
			throw (e);

		}

	}

	public static int getRowContains(String sTestCaseName, int colNum) throws Exception{
		int i;
		try {
			int rowCount = Xls_Reader.getRowUsed();
			for ( i=0 ; i<rowCount; i++){
				if  (Xls_Reader.getCellData(i,colNum).equalsIgnoreCase(sTestCaseName)){
					break;
				}
			}
			return i;
		}catch (Exception e){
			throw(e);
		}

	}
	public static int getRowUsed() throws Exception {
		try{
			int RowCount = sheet.getLastRowNum();
			return RowCount;
		}catch (Exception e){
			System.out.println(e.getMessage());
			throw (e);
		}

	}
	public static List<String> getColumnData(String colName, String FilePath, String SheetName) throws Exception {
		FileInputStream ExcelFile = new FileInputStream(FilePath);
		// Access the required test data sheet
		workbook = new XSSFWorkbook(ExcelFile);
		sheet = workbook.getSheet(SheetName);
		int totalRows = sheet.getLastRowNum();
		int totalCols = (sheet.getRow(0).getLastCellNum()
		List <String> colData= new ArrayList<String>();

		//iterate over columns to get the column name
		for(int i=0; i<=totalCols; i++) {
			String columnName = getCellData(0, i);

			// if column name matches with desired column
			if (colName.equalsIgnoreCase(columnName)) {
				//iterate over all rows to get the data 
				for(int j=1; j<=totalRows; j++) {
					colData.add(getCellData(j, i));
				}
			}
		}
		return colData;
	}

	public static Object[][] getTableArray(String FilePath, String SheetName) throws Exception {   
		String[][] tabArray = null;
		try {
			FileInputStream ExcelFile = new FileInputStream(FilePath);
			// Access the required test data sheet
			workbook = new XSSFWorkbook(ExcelFile);
			sheet = workbook.getSheet(SheetName);
			int startRow = 1;
			int startCol = 0;
			int totalRows = sheet.getLastRowNum();
			int totalCols = (sheet.getRow(0).getLastCellNum());
			tabArray = new String[totalRows][totalCols];
			int ci=0;
			for (int i=startRow;i<=totalRows ;i++, ci++) {  
				int cj=0;
				for (int j=startCol;j<totalCols;j++, cj++){
					tabArray[ci][cj]=getCellData(i,j);
				}

			}
			System.out.println("Excel Array length : "+tabArray.length);

		}
		catch (FileNotFoundException e){
			System.out.println("Could not read the Excel sheet");
			e.printStackTrace();
		}

		catch (IOException e){
			System.out.println("Could not read the Excel sheet");
			e.printStackTrace();

		}

		return(tabArray);
	}

}