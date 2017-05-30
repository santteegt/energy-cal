package com.fitbank.billing.energy;

import com.fitbank.common.ApplicationDates;

import com.fitbank.common.Helper;
import com.fitbank.dto.management.Detail;
import com.fitbank.dto.management.Field;
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
import com.fitbank.hb.persistence.gene.Trangebillingpoints;
import com.fitbank.hb.persistence.safe.Tusercompany;
import com.fitbank.billing.sequence.BillingSequence;
import org.apache.commons.lang.StringUtils;

import java.math.BigDecimal;
import java.text.MessageFormat;

/**
 * Genera los valores por consumo electrico en base a la ordenanza municipal
 *
 *
 * @author SoftwareHouse S.A.
 */
public class EnergyConsumptionBillingGenerator {

    public static void generateBillingRecord(Detail pDetail, Telectricservice pService, Telectricconsumption pConsumption,
                                             String documentType, String statusDoc,
                                             Tusercompany user, String pPeriod, String energyAccountItem,
                                             Trangebillingpoints billingPoint, String cellar) throws Exception {

        String serie = new BillingSequence().execute(pDetail);

        procesBillRecord(pService, pConsumption, documentType, statusDoc, serie, pPeriod, cellar, user, billingPoint, energyAccountItem);

    }

    private static void procesBillRecord(Telectricservice pService, Telectricconsumption pConsumption,
                                         String documentType, String statusDoc, String serie, String pPeriod, String cellar,
                                         Tusercompany user, Trangebillingpoints billingPoint,
                                         String energyAccountItem) throws Exception {

        String sequence = StringUtils.leftPad(String.valueOf(serie), 9, "0");
        String branch = StringUtils.leftPad(String.valueOf(user.getCsucursal()), 3, "0");
        String docNumber = MessageFormat.format("{0}-{1}-{2}-{3}", documentType, branch, user.getCpuntotrabajo(),
                sequence);

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

        processBillDetail(pService.getPk().getCpersona_compania(), docNumber, pPeriod, energyAccountItem, pConsumption);

        processBillQuota(pService.getPk().getCpersona_compania(), docNumber, pPeriod, pConsumption);




    }

    private static void processBillDetail(Integer pCia, String docNumber, String pPeriod, String energyAccountItem,
                                   Telectricconsumption pConsumption) throws Exception {

        TdBillKey dBillPk = new TdBillKey(pCia, docNumber, pPeriod, energyAccountItem, ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP);
        TdBill dBill = new TdBill(dBillPk, ApplicationDates.getDBTimestamp());
        dBill.setCantidad(new BigDecimal(1));
        dBill.setPreciounitario(pConsumption.getTotal());
        dBill.setDescuento(BigDecimal.ZERO);
        dBill.setValoriva(BigDecimal.ZERO);
        dBill.setTotal(pConsumption.getTotal());

        Helper.saveOrUpdate(dBill);

    }

    private static void processBillQuota(Integer pCia, String docNumber, String pPeriod,
                                         Telectricconsumption pConsumption) throws Exception {

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
