/**
* Project Name:Kettle Engine
* File Name:ExcelUtil.java
* Package Name:org.pentaho.di.trans.steps.excelinput
* Date:2015年6月24日下午8:50:10
* Copyright (c) 2015, jingma@iflytek.com All Rights Reserved.
*/
/**
* Project Name:Kettle Engine
* File Name:ExcelUtil.java
* Package Name:org.pentaho.di.trans.steps.excelinput
* Date:2015年6月24日下午8:50:10
* Copyright (c) 2015, jingma@iflytek.com All Rights Reserved.
*/

package org.pentaho.di.trans.steps.excelinput;
import java.io.InputStream;   
import java.util.ArrayList;   
import java.util.Iterator;   
import java.util.List;   
  


import org.apache.poi.openxml4j.opc.OPCPackage;   
import org.apache.poi.xssf.eventusermodel.XSSFReader;   
import org.apache.poi.xssf.model.SharedStringsTable;   
import org.apache.poi.xssf.usermodel.XSSFRichTextString;   
import org.junit.Test;
import org.xml.sax.Attributes;   
import org.xml.sax.InputSource;   
import org.xml.sax.SAXException;   
import org.xml.sax.XMLReader;   
import org.xml.sax.helpers.DefaultHandler;   
import org.xml.sax.helpers.XMLReaderFactory;   
  
public class ExcelUtil extends DefaultHandler {   
       
    private SharedStringsTable sst;   
    private String lastContents;   
    private boolean nextIsString;   
  
    private int sheetIndex = -1;   
    private List<String> rowlist = new ArrayList<String>();   
    private int curRow = 0;   
    private int curCol = 0;   
       
       
    /**  
     * 读取第一个工作簿的入口方法  
     * @param path  
     */  
    public void readOneSheet(String path) throws Exception {   
        OPCPackage pkg = OPCPackage.open(path);        
        XSSFReader r = new XSSFReader(pkg);   
        SharedStringsTable sst = r.getSharedStringsTable();   
               
        XMLReader parser = fetchSheetParser(sst);   
               
        InputStream sheet = r.getSheet("rId1");   
  
        InputSource sheetSource = new InputSource(sheet);   
        parser.parse(sheetSource);   
               
        sheet.close();         
    }   
       
       
    /**  
     * 读取所有工作簿的入口方法  
     * @param path  
     * @throws Exception 
     */  
    public void process(String path) throws Exception {   
        OPCPackage pkg = OPCPackage.open(path);   
        XSSFReader r = new XSSFReader(pkg);   
        SharedStringsTable sst = r.getSharedStringsTable();   
  
        XMLReader parser = fetchSheetParser(sst);   
  
        Iterator<InputStream> sheets = r.getSheetsData();   
        while (sheets.hasNext()) {   
            curRow = 0;   
            sheetIndex++;   
            InputStream sheet = sheets.next();   
            InputSource sheetSource = new InputSource(sheet);   
            parser.parse(sheetSource);   
            sheet.close();   
        }   
    }   
       
    /**  
     * 该方法自动被调用，每读一行调用一次，在方法中写自己的业务逻辑即可 
     * @param sheetIndex 工作簿序号 
     * @param curRow 处理到第几行 
     * @param rowList 当前数据行的数据集合 
     */  
    public void optRow(int sheetIndex, int curRow, List<String> rowList) {   
        String temp = "";   
        for(String str : rowList) {   
            temp += str + "_";   
        }   
        System.out.println(curRow+"||"+temp);   
    }   
       
       
    public XMLReader fetchSheetParser(SharedStringsTable sst) throws SAXException {   
        XMLReader parser = XMLReaderFactory   
                .createXMLReader("org.apache.xerces.parsers.SAXParser");   
        this.sst = sst;   
        parser.setContentHandler(this);   
        return parser;   
    }   
       
    public void startElement(String uri, String localName, String name,   
            Attributes attributes) throws SAXException {   
        // c => 单元格  
        if (name.equals("c")) {   
            // 如果下一个元素是 SST 的索引，则将nextIsString标记为true  
            String cellType = attributes.getValue("t");   
            if (cellType != null && cellType.equals("s")) {   
                nextIsString = true;   
            } else {   
                nextIsString = false;   
            }   
        }   
        // 置空   
        lastContents = "";   
    }   
       
       
    public void endElement(String uri, String localName, String name)   
            throws SAXException {
        // 根据SST的索引值的到单元格的真正要存储的字符串  
        // 这时characters()方法可能会被调用多次  
        if (nextIsString) {   
            try {   
                int idx = Integer.parseInt(lastContents);   
                lastContents = new XSSFRichTextString(sst.getEntryAt(idx))   
                        .toString();   
            } catch (Exception e) {   
  
            }   
        }   
  
        // v => 单元格的值，如果单元格是字符串则v标签的值为该字符串在SST中的索引  
        // 将单元格内容加入rowlist中，在这之前先去掉字符串前后的空白符  
        if (name.equals("v")) {   
            String value = lastContents.trim();   
            value = value.equals("") ? " " : value;   
            rowlist.add(curCol, value);   
            curCol++;   
        } else {   
            // 如果标签名称为 row ，这说明已到行尾，调用 optRows() 方法  
            if (name.equals("row")) {   
                optRow(sheetIndex, curRow, rowlist);   
                rowlist.clear();   
                curRow++;   
                curCol = 0;   
            }   
        }   
    }   
  
    public void characters(char[] ch, int start, int length)   
            throws SAXException {   
        // 得到单元格内容的值  
        lastContents += new String(ch, start, length);   
    }   
    @Test
    public void readXlsxTest(){
    	ExcelUtil eu = new ExcelUtil();
    	try {
			eu.readOneSheet("E:\\情报信息中心2015-1-19日数据提取-基本信息表.xlsx");
		} catch (Exception e) {
			e.printStackTrace();
			// TODO Auto-generated catch block
		}
    }
}  