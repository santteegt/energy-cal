package com.fitbank.billing.energy;

import com.fitbank.billing.maintenance.GenerateEnergyConsumptionValues;
import com.fitbank.common.ApplicationDates;
import com.fitbank.common.Helper;
import com.fitbank.common.conectivity.HbSession;
import com.fitbank.common.exception.FitbankException;
import com.fitbank.common.logger.FitbankLogger;
import com.fitbank.dto.management.Detail;
import com.fitbank.hb.persistence.billing.cBill.TcBill;
import com.fitbank.hb.persistence.billing.cBill.TcBillKey;
import com.fitbank.hb.persistence.billing.cBill.TcBillid;
import com.fitbank.hb.persistence.billing.cBill.TcBillidKey;
import com.fitbank.hb.persistence.billing.dBill.TdBill;
import com.fitbank.hb.persistence.billing.dBill.TdBillKey;
import com.fitbank.hb.persistence.billing.dQuBill.TdQuotaBill;
import com.fitbank.hb.persistence.billing.dQuBill.TdQuotaBillKey;
import com.fitbank.hb.persistence.billing.elec.Telectricconsumption;
import com.fitbank.hb.persistence.billing.elec.Telectricservice;
import com.fitbank.hb.persistence.billing.elec.TelectricserviceKey;
import com.fitbank.hb.persistence.gene.Trangebillingpoints;
import com.fitbank.hb.persistence.safe.Tusercompany;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.math.BigDecimal;
import java.text.MessageFormat;

/**
 * Genera los valores por consumo electrico en base a la ordenanza municipal
 *
 *
 * @author SoftwareHouse S.A.
 */
public class EnergyConsumptionBillingGenerator implements Runnable {

    private static final Logger LOGGER = FitbankLogger.getLogger();

    private Detail pDetail;
    private Telectricconsumption pConsumption;
    private String documentType;
    private String statusDoc;
    private String serie;
    private Tusercompany user;
    private String pPeriod;
    private String energyAccountItem;
    private Trangebillingpoints billingPoint;
    private String cellar;

    public EnergyConsumptionBillingGenerator(Detail pDetail, Telectricconsumption pConsumption,
                                             String documentType, String statusDoc, String serie,
                                             Tusercompany user, String pPeriod, String energyAccountItem,
                                             Trangebillingpoints billingPoint, String cellar) {

        this.pDetail = pDetail;
        this.pConsumption = pConsumption;
        this.documentType = documentType;
        this.statusDoc = statusDoc;
        this.serie = serie;
        this.user = user;
        this.pPeriod = pPeriod;
        this.energyAccountItem = energyAccountItem;
        this.billingPoint = billingPoint;
        this.cellar = cellar;

    }

    @Override
    public void run() {

//        GenerateEnergyConsumptionValues.threads_counter++;
//        LOGGER.info("==>New Thread. Total threads being used: " + GenerateEnergyConsumptionValues.threads_counter);
        try {
            Helper.setSession(HbSession.getInstance().openSession());
            Helper.beginTransaction();
            pDetail = pDetail.cloneMe();

            String sequence = StringUtils.leftPad(String.valueOf(serie), 9, "0");
            String branch = StringUtils.leftPad(String.valueOf(user.getCsucursal()), 3, "0");
            String docNumber = MessageFormat.format("{0}-{1}-{2}-{3}", documentType, branch, user.getCpuntotrabajo(),
                    sequence);

            Integer cia = pConsumption.getPk().getCpersona_compania();
            String cService = pConsumption.getPk().getCservicio();
            TelectricserviceKey servicePk = new TelectricserviceKey(cia, cService,
                    ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP);
            Telectricservice pService = Helper.getBean(Telectricservice.class, servicePk);

            if(pService != null) {

                BigDecimal total = EnergyConsumptionCalculator.calculateEnergyConsumption(pService, pConsumption);
                pConsumption.setTotal(total);
                pConsumption.setFproceso(ApplicationDates.getDBDate()); // TODO

                processBillRecord(docNumber, pService);

                pConsumption.setNumerodocumento(docNumber);
                pConsumption.setCperiodo(pPeriod);
                Helper.saveOrUpdate(pConsumption);
                Helper.commitTransaction();

            } else {
                throw new FitbankException("BILLE04","CODIGO {0} NO DEFINIDO EN LA TSERVICIOSELECTRICOS", cService);
            }
        } catch (Exception ex) {
//            LOGGER.info("Releasing thread on Energy Consumption generation due to an error. Total threads being used: "
//                    + GenerateEnergyConsumptionValues.threads_counter);
            Helper.rollbackTransaction();
            throw new FitbankException("ERR0001", "ERROR AL ENVIAR LA FACTURA DEL SERVICIO {0}", ex, pConsumption.getPk().getCservicio());
        } finally {
//            if(GenerateEnergyConsumptionValues.threads_counter > 0)
//                GenerateEnergyConsumptionValues.threads_counter--;
            Helper.closeSession();
        }
    }

    private void processBillRecord(String docNumber, Telectricservice pService) throws Exception {

        TcBillidKey billIdPk = new TcBillidKey(pService.getPk().getCpersona_compania(), docNumber, pPeriod);

        TcBillid billId = new TcBillid(billIdPk, user.getCsucursal(), user.getCpuntotrabajo(), documentType,
                Long.valueOf(serie));

        Helper.save(billId);

        TcBillKey billPk = new TcBillKey(pService.getPk().getCpersona_compania(), docNumber, pPeriod,
                ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP);
        TcBill bill = new TcBill();
        bill.setPk(billPk);
        bill.setCsucursal(user.getCsucursal());
        bill.setCpuntotrabajo(user.getCpuntotrabajo());
        bill.setCtipodocumentofacturacion(documentType);
        bill.setSecuencia(Long.valueOf(serie));
        bill.setNumeroautorizacion(String.valueOf(billingPoint.getNumeroautorizacion()));
        bill.setNumeroserie(MessageFormat.format("{0}{1}", billingPoint.getEstablecimiento(),
                billingPoint.getPk().getCpuntotrabajo()));
        bill.setCestatusdocumento(statusDoc); // TODO: Guia por remitir
        bill.setCcuenta_cliente(pService.getIdentificacion());
        bill.setCusuario_facturador(user.getPk().getCusuario());
        bill.setFregistro(ApplicationDates.getDBDate());
        bill.setFfactura(ApplicationDates.getDBDate());
        bill.setObservaciones("FACTURA"); //TODO

        bill.setTottarifacero(pConsumption.getTotal());
        bill.setTottarifaiva(BigDecimal.ZERO);
        bill.setValoriva(BigDecimal.ZERO);
        bill.setValordescuento(BigDecimal.ZERO);
        bill.setTotalgeneral(pConsumption.getTotal());
        bill.setCfrecuencia(0); // TODO
        bill.setCbodega(cellar);
        bill.setClugarentrega("A"); // TODO
        bill.setPlazo(1);
        bill.setContieneguia("0");

        Helper.saveOrUpdate(bill);

        TdBill dBill = processBillDetail(pService.getPk().getCpersona_compania(), docNumber);

        processBillQuota(pService.getPk().getCpersona_compania(), docNumber);

    }

    private TdBill processBillDetail(Integer pCia, String docNumber) throws Exception {

        TdBillKey dBillPk = new TdBillKey(pCia, docNumber, pPeriod, energyAccountItem, ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP);
        TdBill dBill = new TdBill(dBillPk, ApplicationDates.getDBTimestamp());
        dBill.setCantidad(new BigDecimal(1));
        dBill.setPreciounitario(pConsumption.getTotal());
        dBill.setDescuento(BigDecimal.ZERO);
        dBill.setValoriva(BigDecimal.ZERO);
        dBill.setTotal(pConsumption.getTotal());

        Helper.saveOrUpdate(dBill);
        return dBill;
    }

    private void processBillQuota(Integer pCia, String docNumber) throws Exception {

        TdQuotaBillKey pk = new TdQuotaBillKey(pCia, docNumber, 1,
                ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP, pPeriod);
        TdQuotaBill quota = new TdQuotaBill();
        quota.setPk(pk);
        quota.setFvencimiento(ApplicationDates.DEFAULT_EXPIRY_DATE); // TODO
        quota.setFdesde(ApplicationDates.getDBTimestamp());
        quota.setValorcuota(pConsumption.getTotal());
        quota.setPagadocuota(BigDecimal.ZERO);
        quota.setSaldocuota(pConsumption.getTotal());

        Helper.saveOrUpdate(quota);

    }

}
