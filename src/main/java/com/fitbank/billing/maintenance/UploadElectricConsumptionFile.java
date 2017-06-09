package com.fitbank.billing.maintenance;

import com.fitbank.common.ApplicationDates;
import com.fitbank.common.Helper;
import com.fitbank.common.conectivity.HbSession;
import com.fitbank.common.exception.FitbankException;
import com.fitbank.common.helper.Dates;
import com.fitbank.common.logger.FitbankLogger;
import com.fitbank.dto.GeneralResponse;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.StringUtils;
import org.apache.poi.hssf.usermodel.HSSFCell;
import org.apache.poi.hssf.usermodel.HSSFRow;
import org.apache.poi.hssf.usermodel.HSSFSheet;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import com.fitbank.dto.management.Detail;
import com.fitbank.dto.management.Record;
import com.fitbank.dto.management.Table;
import com.fitbank.processor.maintenance.MaintenanceCommand;
import com.fitbank.hb.persistence.billing.elec.TelectricserviceidKey;
import com.fitbank.hb.persistence.billing.elec.Telectricserviceid;
import com.fitbank.hb.persistence.billing.elec.Telectricconsumption;
import com.fitbank.hb.persistence.billing.elec.TelectricconsumptionKey;
import java.util.Date;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import org.apache.log4j.Logger;

/**
 * Clase que permite subir una plantilla financiera asociada a una transacción
 * asociada
 * 
 * @author Software House.
 */
@Slf4j
public class UploadElectricConsumptionFile extends MaintenanceCommand {
    
    private static final Logger LOGGER = FitbankLogger.getLogger();
    /**
     * variable estática con id necesario en una serialización
     */
    private static final long serialVersionUID = 1L;
    /**
     * variable lhead con referencia a las columnas del archivo excel
     */
    private final Map<Integer, String> lheads = new HashMap<Integer, String>();

    /**
     * Constante con el nombre de la hoja que contiene la definición de la
     * plantilla
     */
    private static final String TEMPLATE = "TCONSUMOSELECTRICOS";
    private static final String DATE_FORMAT = "yyyy-MM-dd";

    @Override
    public Detail executeNormal(Detail pDetail) throws Exception {
        
        ConsumptionFileThread cft = new ConsumptionFileThread(pDetail);
        cft.start();
        
        GeneralResponse generalResponse = new GeneralResponse("BILLE99",
                "GENERACIÓN Y FACTURACIÓN DE CONSUMOS ELÉCTRICOS EN PROCESO ....");

        pDetail.setResponse(generalResponse);
        return pDetail;
    }

    @Override
    public Detail executeReverse(Detail detail) throws Exception {
        return detail;
    }
    
    private class ConsumptionFileThread extends Thread{
        
        private final Detail pDetail;
        
        public ConsumptionFileThread(Detail pDetail){
            this.pDetail = pDetail;
        }
        
        @Override
        public void run() {

            try {

                Helper.setSession(HbSession.getInstance().openSession());
                Helper.beginTransaction();
                Table tplantilla = pDetail.findTableByName("TFORMATOPLANTILLAXLS");
                Integer processed = 0;
                if (tplantilla != null) {
                    tplantilla.setSpecial(true);

                    for (Record record : tplantilla.getRecords()) {
                        processed++;
                        Object template = record.findFieldByName("ARCHIVO")
                                .getValue();
                        byte[] data = this.verifyNull(template);
                        this.obtainXmlTemplate(data, pDetail);
                        if(processed%1000 == 0) {
                            LOGGER.info(processed+" REGISTROS YA SE HAN PROCESADO");
                        }
                    }
                }

                Helper.commitTransaction();
            } catch(Exception e) {
                Helper.rollbackTransaction();
                LOGGER.error("ERROR AL CARGAR EL ARCHIVO", e);
            } finally {
                Helper.closeSession();
            }

        }
        
        /**
        * Metodo que verifica si obteto una instancia de byte o String y si su
        * valor no es null.
        */
       public byte[] verifyNull(Object objeto) throws Exception {
           byte[] rulebyte = null;
           Integer zice = 0;
           if (objeto instanceof byte[]) {
               zice = ((byte[]) objeto).length;
           } else if (objeto instanceof String) {
               zice = 2;
           }
           if (zice > 1) {
               rulebyte = getRulBytes(objeto);
           }
           return rulebyte;
       }
       
       /**
        * Decodifica de base 64
        */
       public byte[] getRulBytes(Object ruleObject) throws Exception {
           byte[] ruleBytes = null;
           String rule;
           if (ruleObject instanceof byte[]) {
               ruleBytes = (byte[]) ruleObject;
           } else if (ruleObject instanceof String) {
               rule = (String) ruleObject;
               ruleBytes = Base64.decodeBase64(rule);
           }
           return ruleBytes;
       }
       
        private void obtainXmlTemplate(byte[] data, Detail detail) throws Exception {
            if (data != null) {
                InputStream is = new ByteArrayInputStream(data);
                POIFSFileSystem fSFileSystem = new POIFSFileSystem(is);
                HSSFWorkbook workbook = new HSSFWorkbook(fSFileSystem);
                this.obtainItemDefinitionXML(workbook, detail);
            }
        }
        
        private void obtainItemDefinitionXML(HSSFWorkbook workbook, Detail pDetail) throws Exception {
            HSSFSheet sheet;
            try {
                sheet = workbook.getSheet(UploadElectricConsumptionFile.TEMPLATE);
                if (sheet == null) {
                    sheet = workbook.getSheetAt(0);
                }
            } catch (Exception e) {
                sheet = workbook.getSheetAt(0);
            }
            Iterator<?> rowIterator = sheet.rowIterator();
            this.managementHeadColumns(rowIterator);
            while (rowIterator.hasNext()) {
                HSSFRow row = (HSSFRow) rowIterator.next();
                this.processItem(row, pDetail);
            }
        }
        
        private void managementHeadColumns(Iterator<?> iteratorRow)
            throws Exception {
            if (iteratorRow.hasNext()) {
                HSSFRow row = (HSSFRow) iteratorRow.next();
                Iterator<?> iterator = row.cellIterator();
                int column = 0;
                while (iterator.hasNext()) {
                    HSSFCell cell = (HSSFCell) iterator.next();
                    String data = this.manageData(cell);
                    lheads.put(column, data);
                    column++;
                }
            }
        }
        
        private void processItem(HSSFRow row, Detail pDetail) throws Exception {
            String code = "";
            BigDecimal consumption = new BigDecimal("0");
            Date consumptionDate = new Date();
            DateFormat format = new SimpleDateFormat(DATE_FORMAT);
            for (Integer key : lheads.keySet()) {
                HSSFCell cell = (HSSFCell) row.getCell(key);
                String data = this.manageData(cell);
                String column = lheads.get(key);
                if(column.compareTo("CSERVICIO")==0){
                    code = data;
                }else if(column.compareTo("CONSUMO")==0){
                    consumption = new BigDecimal(data);
                }else if(column.compareTo("FCONSUMO")==0){
                    try{
                        consumptionDate = format.parse(data);
                    }catch(ParseException e){
                        throw new FitbankException("ERR0001", "EL CAMPO FCONSUMO DEBE ESTAR CON FORMATO {0}", DATE_FORMAT);
                    }

                }
            }
            this.validateConsumptionDate(consumptionDate, pDetail);
            TelectricserviceidKey telectricserviceidKey = new TelectricserviceidKey(pDetail.getCompany(), code);
            Telectricserviceid telectricserviceid = new Telectricserviceid(telectricserviceidKey);
            TelectricconsumptionKey telectricconsumptionKey = new TelectricconsumptionKey(pDetail.getCompany(), code, new java.sql.Date(consumptionDate.getTime()), ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP);
            Telectricconsumption telectricconsumption = Helper.getBean(Telectricconsumption.class, telectricconsumptionKey);
            if(telectricconsumption == null){
                telectricconsumption = new Telectricconsumption(telectricconsumptionKey, consumption, ApplicationDates.getDBTimestamp(),"ING");
            }else{
                telectricconsumption.setFproceso(null);
                telectricconsumption.setTotal(null);
                telectricconsumption.setConsumo(consumption);
                telectricconsumption.setEstado("ING");
            }
            Helper.saveOrUpdate(telectricserviceid);
            Helper.saveOrUpdate(telectricconsumption);
        }
        
        private String manageData(HSSFCell cell) {
            String data = StringUtils.EMPTY;
            if (cell != null) {
                switch (cell.getCellType()) {
                case HSSFCell.CELL_TYPE_BLANK:
                    data = StringUtils.EMPTY;
                    break;
                case HSSFCell.CELL_TYPE_NUMERIC:
                    data = (new BigDecimal(cell.getNumericCellValue())).toString();
                    break;
                case HSSFCell.CELL_TYPE_STRING:
                    data = cell.getStringCellValue();
                    break;
                default:
                    break;
                }
            }
            return data;
        }

        private void validateConsumptionDate(Date consumptionDate, Detail pDetail){
            try {
                Date processDate = pDetail.findFieldByName("FCARGA").getRealDateValue();
                Dates processDates = new Dates(new java.sql.Date(processDate.getTime()));
                String processPeriod = (processDates.getField(Calendar.MONTH) + 1) + "" + processDates.getField(Calendar.YEAR);
                Dates consumptionDates = new Dates(new java.sql.Date(consumptionDate.getTime()));
                String consumptionPeriod = (consumptionDates.getField(Calendar.MONTH) + 1) + "" + consumptionDates.getField(Calendar.YEAR);
                if(processPeriod.compareTo(consumptionPeriod)!=0){
                    throw new FitbankException("ERR0003", "LA FECHA {0} EN EL ARCHIVO NO PERTENECE AL PERIODO {1}", consumptionDate, processPeriod);
                }
            } catch (Exception ex) {
                throw new FitbankException("ERR0002", ex.getMessage());
            }

        }
    }
}
