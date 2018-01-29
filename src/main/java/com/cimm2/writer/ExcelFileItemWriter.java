package com.cimm2.writer;

import com.cimm2.util.ModelUtil;
import org.apache.commons.lang.time.DateFormatUtils;
import org.apache.poi.ss.usermodel.*;
import org.apache.poi.ss.util.CellRangeAddress;
import org.apache.poi.xssf.streaming.SXSSFSheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import org.springframework.batch.core.StepExecution;
import org.springframework.batch.core.annotation.AfterStep;
import org.springframework.batch.core.annotation.BeforeStep;
import org.springframework.batch.item.ItemWriter;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Calendar;
import java.util.List;

public class ExcelFileItemWriter implements ItemWriter<String[]> {

    private  final String fileName ;
    private  final String[] headers ;

    private SXSSFSheet sheet;
    private SXSSFWorkbook workbook;
    private int currRow = 0;


    public ExcelFileItemWriter(String fileName,String[] headers ){
        this.fileName = fileName;
        this.headers = headers;

        workbook = new SXSSFWorkbook(100);
        workbook.setCompressTempFiles(true);

        sheet =  workbook.getSheetAt(0);
        sheet.setRandomAccessWindowSize(100);
        addHeaders(sheet);
    }

    private void addHeaders(Sheet sheet) {
        Row row = sheet.createRow(currRow++);
        int col = 0;

        for (String header : headers) {
            Cell cell = row.createCell(col);
            cell.setCellValue(header);
            col++;
        }
    }




    public void close() throws IOException {
        try {
            FileOutputStream outputStream = new FileOutputStream(fileName);
            workbook.write(outputStream);
            workbook.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }



    @Override
    public void write(List<? extends String[]> items) throws Exception {
        for (String[] item : items) {
            System.out.println("ExcelFileItemWriter  :" + currRow + " - Thread name:" + Thread.currentThread().getName() + " Object hashcode:" + this);
            int colIndx =0;
            Row row = sheet.createRow(currRow++);
            for (String col : headers) {
                createStringCell(row, ModelUtil.getCellValue(col, item, headers), colIndx++);
            }
        }
    }

    private void createStringCell(Row row, String val, int colIndx) {
        Cell cell = row.createCell(colIndx);
        cell.setCellType(Cell.CELL_TYPE_STRING);
        cell.setCellValue(val);
    }

}


