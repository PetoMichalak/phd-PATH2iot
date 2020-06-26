package eu.uk.ncl.di.pet5o.PATH2iot.utils;

import java.io.Serializable;

public class PhysicalPlanStats implements Serializable {
    int id;

    public PhysicalPlanStats(int id) {
        this.id = id;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }
}
