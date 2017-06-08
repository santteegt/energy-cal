/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.fitbank.billing.maintenance;

import com.fitbank.common.ApplicationDates;
import com.fitbank.common.Helper;
import com.fitbank.common.conectivity.HbSession;
import com.fitbank.common.exception.FitbankException;
import com.fitbank.common.hb.UtilHB;
import com.fitbank.dto.GeneralResponse;
import com.fitbank.dto.management.Detail;
import com.fitbank.hb.persistence.billing.dBill.TdBill;
import com.fitbank.hb.persistence.billing.dBill.TdBillKey;
import com.fitbank.hb.persistence.billing.elec.Telectricconsumption;
import com.fitbank.hb.persistence.billing.elec.TelectricconsumptionKey;
import com.fitbank.hb.persistence.billing.elec.Telectricservice;
import com.fitbank.hb.persistence.billing.elec.TelectricserviceKey;
import com.fitbank.hb.persistence.safe.Tusercompany;
import com.fitbank.hb.persistence.safe.TusercompanyKey;
import com.fitbank.processor.maintenance.MaintenanceCommand;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Envío de facturas en batch al SRI
 * @author Software House S.A.
 */
public class ProcessEnergyConsumptionValues extends MaintenanceCommand{
    
    public static final int MAX_THREADS = 10;
    //public static int threads_counter = 0;
    
    ExecutorService executorService = Executors.newFixedThreadPool(MAX_THREADS);
    
    private static final String HQL_CONSUMPTION = "from com.fitbank.hb.persistence.billing.elec.Telectricconsumption t "
            + "where t.pk.fhasta = :expireDate and t.pk.cpersona_compania = :cia "
            + "and t.pk.fconsumo = :consumptionDate and t.estado = 'ING'";
    
    @Override
    public Detail executeNormal(Detail pDetail) throws Exception {
        
        ProcessEnergyConsumptionThread pect = new ProcessEnergyConsumptionThread(pDetail);
        pect.start();
        GeneralResponse generalResponse = new GeneralResponse("BILLE99",
                "ENVIO DE FACTURACIÓN DE CONSUMOS ELÉCTRICOS EN PROCESO ....");

        pDetail.setResponse(generalResponse);
        return pDetail;
    }
    
    private class ProcessEnergyConsumptionThread extends Thread{
        
        Detail pDetail;
        
        public ProcessEnergyConsumptionThread(Detail pDetail){
            this.pDetail = pDetail;
        }
        
        @Override
        public void run(){
            try {
                Helper.setSession(HbSession.getInstance().openSession());
                Helper.beginTransaction();
                TusercompanyKey tusercompanyKey = new TusercompanyKey(pDetail.getCompany(), pDetail.getUser(), ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP);
                Tusercompany tusercompany = Helper.getBean(Tusercompany.class, tusercompanyKey);
                UtilHB utilHB = new UtilHB(HQL_CONSUMPTION);
                utilHB.setTimestamp("expireDate", ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP);
                utilHB.setInteger("cia", pDetail.getCompany());
                utilHB.setDate("consumptionDate", pDetail.findFieldByNameCreate(TelectricconsumptionKey.PK_FCONSUMO).getRealDateValue());
                List<Telectricconsumption> telectricconsumptions = utilHB.getList();
                for(Telectricconsumption telectricconsumption:telectricconsumptions) {
                    TelectricserviceKey telectricserviceKey = new TelectricserviceKey(pDetail.getCompany(), telectricconsumption.getPk().getCservicio(), ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP);
                    Telectricservice telectricservice = Helper.getBean(Telectricservice.class, telectricserviceKey);
                    TdBillKey tdbillKey = new TdBillKey(pDetail.getCompany(), telectricconsumption.getNumerodocumento(), telectricconsumption.getCperiodo(), pDetail.findFieldByNameCreate("CCUENTA").getStringValue(), ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP);
                    TdBill tdBill = Helper.getBean(TdBill.class, tdbillKey);
                    if(telectricservice!=null && tdBill!=null) {
                        SendDocumentSRI sdsri = new SendDocumentSRI(pDetail, tusercompany, telectricconsumption, telectricservice.getIdentificacion(), tdBill);
                        executorService.execute(sdsri);
                    } else if (telectricservice!=null) {
                        throw new FitbankException("BILLE04","CODIGO {0} NO DEFINIDO EN LA TSERVICIOSELECTRICOS", telectricconsumption.getPk().getCservicio());
                    } else {
                        throw new FitbankException("BILLE05","CODIGO {0} NO TIENE UNA FACTURA ASOCIADA", telectricconsumption.getPk().getCservicio());
                    }
                }
            } catch (Exception ex) {
                throw new FitbankException("BILLE06","ERROR AL PROCESAR LOS CONSUMOS", ex);
            } finally {
                executorService.shutdown();
                Helper.commitTransaction();
                Helper.closeSession();
            }
        }
                
    }

    @Override
    public Detail executeReverse(Detail pDetail) throws Exception {
        return pDetail;
    }
    
}
