package com.fitbank.billing.maintenance;

import com.fitbank.billing.energy.EnergyConsumptionBillingGenerator;
import com.fitbank.billing.energy.EnergyConsumptionCalculator;
import com.fitbank.billing.helper.BillingHelper;
import com.fitbank.common.ApplicationDates;
import com.fitbank.common.Helper;
import com.fitbank.common.exception.FitbankException;
import com.fitbank.common.hb.UtilHB;
import com.fitbank.dto.management.Field;
import com.fitbank.hb.persistence.billing.elec.Telectricservice;
import com.fitbank.hb.persistence.billing.elec.TelectricserviceKey;
import com.fitbank.hb.persistence.gene.Trangebillingpoints;
import com.fitbank.hb.persistence.safe.Tusercompany;
import com.fitbank.hb.persistence.safe.TusercompanyKey;
import com.fitbank.processor.maintenance.MaintenanceCommand;
import com.fitbank.dto.management.Detail;
import com.fitbank.hb.persistence.billing.elec.Telectricconsumption;
import com.fitbank.common.helper.Dates;


import java.math.BigDecimal;
import java.util.List;
import java.util.Calendar;

/**
 * Genera los valores por consumo electrico en base a la ordenanza municipal
 *
 *
 * @author SoftwareHouse S.A.
 */
public class GenerateEnergyConsumptionValues extends MaintenanceCommand {

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

        List<Telectricconsumption> consummptionList = this.getConsumptionListToProcess();

        for(Telectricconsumption consumption: consummptionList) {
            Integer cia = consumption.getPk().getCpersona_compania();
            String cServicio = consumption.getPk().getCservicio();
            TelectricserviceKey servicePk = new TelectricserviceKey(cia, cServicio,
                    ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP);
            Telectricservice service = Helper.getBean(Telectricservice.class, servicePk);

            if(service != null) {

                BigDecimal total = EnergyConsumptionCalculator.calculateEnergyConsumption(service, consumption);
                consumption.setTotal(total);
                consumption.setFproceso(ApplicationDates.getDBDate());
                Helper.saveOrUpdate(consumption);

                EnergyConsumptionBillingGenerator.generateBillingRecord(pDetail, service, consumption,
                        documentType, statusDoc, userCompany, period, energyAccountItem, trangebillingpoints, cellar);

            } else {

                throw new FitbankException("BILLE04","CODIGO {0} NO DEFINIDO EN LA TSERVICIOSELECTRICOS", cServicio);

            }
        }

        return pDetail;

    }

    private List<Telectricconsumption> getConsumptionListToProcess() throws Exception {
        UtilHB utilHB = new UtilHB(HQL_ENERGY_CONSUMPTION);
        utilHB.setTimestamp("expireDate", ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP);
        utilHB.setString("status", STATUS_FILTER);

        return utilHB.getList();
    }

    @Override
    public Detail executeReverse(Detail pDetail) throws Exception {
        return pDetail;
    }

}
