package com.fitbank.billing.energy;

import com.fitbank.common.ApplicationDates;
import com.fitbank.common.Helper;
import com.fitbank.common.exception.FitbankException;
import com.fitbank.hb.persistence.billing.elec.Telectricconsumption;
import com.fitbank.hb.persistence.billing.elec.TelectricserviceKey;
import com.fitbank.hb.persistence.billing.elec.Telectricservice;
import com.fitbank.hb.persistence.billing.elec.TelectricparametersKey;
import com.fitbank.hb.persistence.billing.elec.Telectricparameters;

import java.math.BigDecimal;

/**
 * Calculo del valor total por consumo electrico
 *
 * @author SoftwareHouse S.A.
 */
public class EnergyConsumptionCalculator {

    private static String CONSUMPTION_TYPE = "RES";

    public static BigDecimal calculateEnergyConsumption(Telectricservice service, Telectricconsumption consumption)
            throws Exception {

        BigDecimal total = calculateTGC(service.getPk().getCpersona_compania(), consumption.getConsumo(),
                service.getTipotarifa().compareTo(CONSUMPTION_TYPE) == 0);

        return total;

    }

    private static BigDecimal calculateTGC(Integer pCia, BigDecimal consumption, Boolean isResidential)
            throws Exception {

        BigDecimal CO = getParameter(pCia, EnergyParameterTypes.CO.getParameterName());
        BigDecimal Fr = getFR(pCia);
        BigDecimal CF = getParameter(pCia, EnergyParameterTypes.CF.getParameterName());
        BigDecimal Fi = getFI(pCia);
        BigDecimal Ks = isResidential ?
                calculateResidetial(pCia, consumption):calculateNonResidetial(pCia, consumption);

        double total = ( CO.doubleValue() * Fr.doubleValue() + CF.doubleValue() * Fi.doubleValue() ) * Ks.doubleValue();

        return new BigDecimal(total);
    }

    private static BigDecimal getFR(Integer pCia) throws Exception {
        BigDecimal Bo = getParameter(pCia, EnergyParameterTypes.Bo.getParameterName());
        BigDecimal B1 = getParameter(pCia, EnergyParameterTypes.B1.getParameterName());

        BigDecimal Eo = getParameter(pCia, EnergyParameterTypes.Eo.getParameterName());
        BigDecimal E1 = getParameter(pCia, EnergyParameterTypes.E1.getParameterName());

        BigDecimal Ro = getParameter(pCia, EnergyParameterTypes.Ro.getParameterName());
        BigDecimal R1 = getParameter(pCia, EnergyParameterTypes.R1.getParameterName());

        BigDecimal Co = getParameter(pCia, EnergyParameterTypes.Eo.getParameterName());
        BigDecimal C1 = getParameter(pCia, EnergyParameterTypes.E1.getParameterName());

        BigDecimal Xo = getParameter(pCia, EnergyParameterTypes.Eo.getParameterName());
        BigDecimal X1 = getParameter(pCia, EnergyParameterTypes.E1.getParameterName());

        double fr  = 0.53 * (B1.doubleValue() / Bo.doubleValue()) + 0.15 * (E1.doubleValue() / Eo.doubleValue())
                + 0.05 * (R1.doubleValue() / Ro.doubleValue()) + 0.04 * (C1.doubleValue() / Co.doubleValue())
                + 0.23 * (X1.doubleValue() / Xo.doubleValue());

        return new BigDecimal(fr);
    }

    private static BigDecimal getFI(Integer pCia) throws Exception {
        BigDecimal ti = getParameter(pCia, EnergyParameterTypes.TI.getParameterName());
        BigDecimal tiv = getParameter(pCia, EnergyParameterTypes.TIV.getParameterName());

        return tiv.divide(ti);
    }

    private static BigDecimal calculateResidetial(Integer pCia, BigDecimal pConsumption) throws Exception {

        double ksr = 0.0;
        double residentialMinBound = getParameter(pCia, EnergyParameterTypes.RMIN.getParameterName()).doubleValue();
        double consumption = pConsumption.doubleValue();

        if(consumption < residentialMinBound) {
            ksr = getParameter(pCia, EnergyParameterTypes.KSR.getParameterName()).doubleValue();
        } else {
            ksr = 0.02 + 0.0049 * consumption;
        }

        return new BigDecimal(ksr);

    }

    private static BigDecimal calculateNonResidetial(Integer pCia, BigDecimal pConsumption) throws Exception {

        double ksnr = 0.0;
        double bound = getParameter(pCia, EnergyParameterTypes.NRB.getParameterName()).doubleValue();
        double consumption = pConsumption.doubleValue();

        if(bound <= consumption) {
            ksnr = 0.3 + 0.0049 * consumption;
        } else {
            ksnr = 3.46 + 0.0042 * consumption;
        }

        return new BigDecimal(ksnr);

    }

    private static BigDecimal getParameter(Integer pCia, String pName) throws Exception {
        TelectricparametersKey pk = new TelectricparametersKey(pCia, pName, ApplicationDates.DEFAULT_EXPIRY_TIMESTAMP);
        Telectricparameters parameter = Helper.getBean(Telectricparameters.class, pk);
        if(parameter != null) {
            return parameter.getValor();
        } else {
            throw new FitbankException("BILLE03","PARAMETRO {0} NO ENCONTRATO EN LA TPARAMETROSELECTRICOS", pName);
        }
    }
}
