package com.fitbank.billing.maintenance;

import com.fitbank.common.ApplicationDates;
import com.fitbank.common.Helper;
import com.fitbank.common.conectivity.HbSession;
import com.fitbank.common.exception.FitbankException;
import com.fitbank.common.hb.UtilHB;
import com.fitbank.common.properties.PropertiesHandler;
import com.fitbank.dto.management.Detail;
import com.fitbank.dto.management.Field;
import com.fitbank.dto.management.Record;
import com.fitbank.dto.management.Table;
import com.fitbank.hb.persistence.billing.cBill.TcBill;
import com.fitbank.hb.persistence.billing.dBill.TdBill;
import com.fitbank.hb.persistence.billing.dBill.TdBillKey;
import com.fitbank.hb.persistence.billing.elec.Telectricconsumption;
import com.fitbank.hb.persistence.safe.Tusercompany;
import com.fitbank.processor.maintenance.MaintenanceProcessor;
import java.math.BigDecimal;

/**
 * Clase que envia documentos al sri
 *
 * @author SoftWare House S.A.
 */
public class SendDocumentSRI implements Runnable {

    private final Detail pDetail;
    private final Tusercompany user;
    private final Telectricconsumption telectricconsumption; 
    private final String identification;
    private final TdBill dBill;
    private static final String FACTURAELECTRONICA = "FacturaElectronica";
        
    private static final String HQL_PERSON = "select p.pk.cpersona "
            + "from com.fitbank.hb.persistence.person.Tperson p "
            + "where p.identificacion = :identification and p.pk.fhasta = :expireDate";
    
    public SendDocumentSRI(Detail pDetail, Tusercompany user, 
            Telectricconsumption telectricconsumption, String identification, TdBill dBill){
        this.pDetail = pDetail;
        this.user = user;
        this.telectricconsumption = telectricconsumption;
        this.identification = identification;
        this.dBill = dBill;
    }
    
    @Override
    public void run() {
        /*if(ProcessEnergyConsumptionValues.threads_counter >= ProcessEnergyConsumptionValues.MAX_THREADS) {
            try {
                Thread.sleep(this.DELAY);
            } catch(Exception e) {
                throw new FitbankException("ERR0001", "ERROR AL ENVIAR LA FACTURA DEL SERVICIO {0}", e, telectricconsumption.getPk().getCservicio());
            }
            this.run();
            return;
        }

        ProcessEnergyConsumptionValues.threads_counter++;*/
        try {
            Helper.setSession(HbSession.getInstance().openSession());
            Helper.beginTransaction();
            Detail newDetail = pDetail.cloneMe();
            newDetail.setTransaction("7500");
            String docType = pDetail.findFieldByName(TcBill.CTIPODOCUMENTOFACTURACION).getStringValue();
            String str_table = PropertiesHandler.getConfig(FACTURAELECTRONICA).getString(docType + "tablaDoc");
            String str_field = PropertiesHandler.getConfig(FACTURAELECTRONICA).getString(docType + "campoDoc");
            Table headerTable = new Table(str_table, str_table);
            Record headerRecord = new Record(0);
            headerRecord.addField(new Field(str_field, telectricconsumption.getNumerodocumento()));
            headerRecord.addField(new Field("VALORDESCUENTO", new BigDecimal("0")));
            headerRecord.addField(new Field("TOTALGENERAL", telectricconsumption.getTotal()));
            headerTable.addRecord(headerRecord);
            newDetail.addTable(headerTable);
            Table detailTable = new Table(TdBillKey.TABLE_NAME, TdBillKey.TABLE_NAME);
            Record detailRecord = new Record(0);
            detailRecord.addField(new Field(TdBillKey.CCUENTA, dBill.getPk().getCcuenta()));
            detailRecord.addField(new Field(TdBill.PRECIOUNITARIO, dBill.getPreciounitario()));
            detailRecord.addField(new Field(TdBill.CANTIDAD, dBill.getCantidad()));
            detailTable.addRecord(detailRecord);
            newDetail.addTable(detailTable);
            newDetail.findFieldByNameCreate(TcBill.CPUNTOTRABAJO).setValue(user.getCpuntotrabajo());
            newDetail.findFieldByNameCreate("FECHA").setValue(pDetail.getAccountingDate());
            newDetail.findFieldByNameCreate("TOTALSINDESC").setValue(telectricconsumption.getTotal());
            newDetail.findFieldByNameCreate("ID").setValue(identification);
            newDetail.findFieldByNameCreate("_MONTO").setValue(telectricconsumption.getTotal());
            newDetail.findFieldByNameCreate("CPERSONA").setValue(getCperson(identification));
            newDetail.findFieldByNameCreate("NUMERODOCUMENTO").setValue(dBill.getPk().getNumerodocumento());
            newDetail.findFieldByNameCreate("FACTURAADIC");
            newDetail.findFieldByNameCreate("TIPOIDADIC");
            newDetail.findFieldByNameCreate("IDADIC");
            Table infoTable = new Table("TINFOADICIONAL", "TINFOADICIONAL");
            Record infoRecord = new Record(0);
            infoRecord.addField(new Field("NOMBRE", "CSERVICIO"));
            infoRecord.addField(new Field("VALOR", telectricconsumption.getPk().getCservicio()));
            infoTable.addRecord(infoRecord);
            newDetail.addTable(infoTable);
            this.telectricconsumption.setEstado("EMI");
            Helper.saveOrUpdate(this.telectricconsumption);
            MaintenanceProcessor mp = new MaintenanceProcessor();
            mp.process(newDetail);
            Helper.commitTransaction();
        } catch (Exception ex) {
            Helper.rollbackTransaction();
            throw new FitbankException("ERR0001", "ERROR AL ENVIAR LA FACTURA DEL SERVICIO {0}", ex, telectricconsumption.getPk().getCservicio());
        } finally {
            Helper.flushTransaction();
            Helper.closeSession();
            //ProcessEnergyConsumptionValues.threads_counter--;
        } 
    }
    
    private static Integer getCperson(String id){
        UtilHB utilHB = new UtilHB(HQL_PERSON);
        utilHB.setString("identification", id);
        utilHB.setTimestamp("expireDate", ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP);
        return (Integer) utilHB.getObject();
    }
}
