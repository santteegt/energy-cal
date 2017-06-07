package com.fitbank.billing.maintenance;

import com.fitbank.billing.energy.EnergyConsumptionBillingGenerator;
import com.fitbank.billing.helper.BillingHelper;
import com.fitbank.common.ApplicationDates;
import com.fitbank.common.Helper;
import com.fitbank.common.conectivity.HbSession;
import com.fitbank.common.exception.FitbankException;
import com.fitbank.common.hb.UtilHB;
import com.fitbank.common.logger.FitbankLogger;
import com.fitbank.dto.GeneralResponse;
import com.fitbank.dto.management.Field;
import com.fitbank.hb.persistence.gene.Trangebillingpoints;
import com.fitbank.hb.persistence.safe.Tusercompany;
import com.fitbank.hb.persistence.safe.TusercompanyKey;
import com.fitbank.processor.maintenance.MaintenanceCommand;
import com.fitbank.dto.management.Detail;
import com.fitbank.hb.persistence.billing.elec.Telectricconsumption;
import com.fitbank.common.helper.Dates;

import java.util.List;
import java.util.Calendar;

import org.apache.log4j.Logger;

/**
 * Genera los valores por consumo electrico en base a la ordenanza municipal
 *
 *
 * @author SoftwareHouse S.A.
 */
public class GenerateEnergyConsumptionValues extends MaintenanceCommand {

    private static final Logger LOGGER = FitbankLogger.getLogger();

    public static final int MAX_THREADS = 5;
    public static int threads_counter = 0;

    private static final String HQL_ENERGY_CONSUMPTION = "from "
            + "com.fitbank.hb.persistence.billing.elec.Telectricconsumption tec "
            + "where tec.pk.fhasta=:expireDate and tec.total is null and tec.estado=:status";

    private static final String STATUS_FILTER = "ING";

    @Override
    public Detail executeNormal(Detail pDetail) throws Exception {

        Field docTypeField = pDetail.findFieldByName("CTIPODOC");
        if(docTypeField == null) {
            throw new FitbankException("BILLE01","CAMPO CTIPODOC NO ESTABLECIDO");
        }

        String documentType = docTypeField.getStringValue();

        Field statusDocField = pDetail.findFieldByName("ESTATUSDOC");
        if(statusDocField == null) {
            throw new FitbankException("BILLE01","CAMPO ESTATUSDOC NO ESTABLECIDO");
        }

        String statusDoc = statusDocField.getStringValue();

        Field cellarField = pDetail.findFieldByName("CBODEGA");
        if(cellarField == null) {
            throw new FitbankException("BILLE01","CAMPO CBODEGA NO ESTABLECIDO");
        }

        String cellar = cellarField.getStringValue();

        Field energyAccountItemField = pDetail.findFieldByName("CCUENTA");
        if(energyAccountItemField == null) {
            throw new FitbankException("BILLE01","CCUENTA PARA RUBRO DE ENERGIA NO ESTABLECIDO");
        }
        String energyAccountItem = energyAccountItemField.getStringValue();


        TusercompanyKey userCiaPk = new TusercompanyKey(pDetail.getCompany(), pDetail.getUser(), ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP);
        Tusercompany userCompany = Helper.getBean(Tusercompany.class, userCiaPk);

        if(userCompany.getCpuntotrabajo() == null) {
            throw new FitbankException("BILLE02","PUNTO DE TRABAJO NO DEIFINIDO EN LA TCOMPANIAUSUARIOS");
        }

        pDetail.addField(new Field("CPUNTOTRA", userCompany.getCpuntotrabajo()));

        String period = String.valueOf(new Dates(pDetail.getAccountingDate()).getField(Calendar.YEAR));

        Trangebillingpoints trangebillingpoints = BillingHelper.getInstance()
                .obtainAuthorization(userCompany.getCsucursal(), userCompany.getCpuntotrabajo(), documentType,
                        userCompany.getPk().getCpersona_compania());

        BillingGeneratorThread thread = new BillingGeneratorThread(pDetail, documentType, statusDoc, userCompany, period,
                energyAccountItem, trangebillingpoints, cellar);
        thread.start();

        GeneralResponse generalResponse = new GeneralResponse("BILLE99",
                "GENERACIÓN Y FACTURACIÓN DE CONSUMOS ELÉCTRICOS EN PROCESO ....");

        pDetail.setResponse(generalResponse);

        return pDetail;

    }

    @Override
    public Detail executeReverse(Detail pDetail) throws Exception {
        return pDetail;
    }

    private class BillingGeneratorThread extends Thread {

        private List<Telectricconsumption> consumptionList;

        private Detail pDetail;
        private String documentType;
        private String statusDoc;
        private Tusercompany userCompany;
        private String pPeriod;
        private String energyAccountItem;
        private Trangebillingpoints billingPoint;
        private String cellar;

        public BillingGeneratorThread(Detail pDetail, String documentType,
                                      String statusDoc, Tusercompany user, String pPeriod, String energyAccountItem,
                                      Trangebillingpoints billingPoint, String cellar) {

            this.pDetail = pDetail;
            this.documentType = documentType;
            this.statusDoc = statusDoc;
            this.pPeriod = pPeriod;
            this.cellar = cellar;
            this.userCompany = user;
            this.billingPoint = billingPoint;
            this.energyAccountItem = energyAccountItem;

            LOGGER.info("Iniciando Billing Generator");


        }

        private List<Telectricconsumption> getConsumptionListToProcess() throws Exception {
            UtilHB utilHB = new UtilHB(HQL_ENERGY_CONSUMPTION);
            utilHB.setTimestamp("expireDate", ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP);
            utilHB.setString("status", STATUS_FILTER);

            return utilHB.getList();
        }

        @Override
        public void run() {

            try {

                Helper.setSession(HbSession.getInstance().openSession());
                Helper.beginTransaction();

                this.consumptionList = this.getConsumptionListToProcess();

                LOGGER.info("Ejecutando Billing Generator para " + consumptionList.size() + " registros");

                for(Telectricconsumption consumption: consumptionList) {

                    EnergyConsumptionBillingGenerator.generateBillingRecord(pDetail, consumption,
                            documentType, statusDoc, userCompany, pPeriod, energyAccountItem, billingPoint, cellar);

                }
                LOGGER.info("Terminando Billing Generator para " + consumptionList.size() + " registros");
                Helper.commitTransaction();
            } catch(Exception e) {
                // TODO: manage errors while processing a register
                Helper.rollbackTransaction();
                LOGGER.info("ERROR Billing Generator: " + e.getMessage());
            } finally {
                Helper.closeSession();
            }

        }
    }

}