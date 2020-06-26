package eu.uk.ncl.di.pet5o.PATH2iot.optimisation;

import eu.uk.ncl.di.pet5o.PATH2iot.input.dataStreams.InputStreams;
import eu.uk.ncl.di.pet5o.PATH2iot.input.infrastructure.InfrastructureDesc;
import eu.uk.ncl.di.pet5o.PATH2iot.optimisation.physical.PhysicalPlan;
import eu.uk.ncl.di.pet5o.PATH2iot.utils.EsperSodaInspector;

/**
 * EPL Realm is a holder for the physical plans, epl inspector instance,
 * infrastructure description and epl specific input streams.
 */

public class EplRealm {
    int id;
    PhysicalPlan plan;
    EsperSodaInspector eplInsperctor;
    InfrastructureDesc infraDescription;
    InputStreams inputStreams;
    String planOut;

    public EplRealm(int id, PhysicalPlan plan, EsperSodaInspector eplInsperctor, InfrastructureDesc infraDescription,
                    InputStreams inputStreams, String planOut) {
        this.id = id;
        this.plan = plan;
        this.eplInsperctor = eplInsperctor;
        this.infraDescription = infraDescription;
        this.inputStreams = inputStreams;
        this.planOut = planOut;
    }

    public int getId() {
        return id;
    }

    public PhysicalPlan getPlan() {
        return plan;
    }

    public EsperSodaInspector getEplInsperctor() {
        return eplInsperctor;
    }

    public InfrastructureDesc getInfraDescription() {
        return infraDescription;
    }

    public InputStreams getInputStreams() {
        return inputStreams;
    }

    public String getPlanOut() {
        return planOut;
    }
}
