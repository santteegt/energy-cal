package com.fitbank.billing.energy;

/**
 * Codigo de parametros para calculo de consumos de energia
 *
 * @author SoftwareHouse S.A.
 */
public enum EnergyParameterTypes {

    CO("CO"), // Costo Operativo
    CF("CF"), // Cargo Fijo Mensual por contribuyente
    KSR("KSR"), // Factor de subsidio residencial para consumos < RMIN (Kw/h)
    Bo("Bo"), // SMU de un auxiliar de limpieza vigente a Ene/2016
    B1("B1"), // SMU de un auxiliar de limpieza vigente a la fecha de actualizacion
    Eo("Eo"), // Indice de equipo y maquinaria para aseo publico, vigente a Junio/2016
    E1("E1"), // Indice de equipo y maquinaria para aseo publico, vigente a la fecha de actualizacion
    Ro("Ro"), // Indice de repuestos por maquinaria de la construccion, vigente a Junio/2016
    R1("R1"), // Indice de repuestos por maquinaria de la construccion, vigente la fecha de actualizacion
    Co("Co"), // Indice de combustible vigente a Junio/2016
    C1("C1"), // Indice de combustible vigente a la fecha de actualizacion
    Xo("Xo"), // Indice general de bienes y servicios diversos, vigente a Junio/2016
    X1("X1"), // Indice general de bienes y servicios diversos, vigente a la fecha de actualizacion
    TI("TI"), // Tasa de interes vigente a Junio/2016
    TIV("TIV"), // Tasa de interes vigente a la fecha de actualizacion
    RMIN("RmB"), // Residential minimum bound
    NRB("NRB"); // Non residential bound

    private String parameter;

    private EnergyParameterTypes(String pParameter) {
        this.parameter = pParameter;
    }

    public String getParameterName() throws Exception {
        return this.parameter;
    }
}
