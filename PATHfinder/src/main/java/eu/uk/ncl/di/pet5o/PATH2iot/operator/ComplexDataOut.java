package eu.uk.ncl.di.pet5o.PATH2iot.operator;

public class ComplexDataOut {
    private int eventCountPerTrigger;
    private int eventSize;               // B
    private int triggersPerSecond;       // ms
    private boolean calculated;

    public ComplexDataOut() {
        eventCountPerTrigger = -1;
        eventSize = -1;
        triggersPerSecond = -1;
        calculated = false;
    }

    public int getEventCountPerTrigger() {
        return eventCountPerTrigger;
    }

    public void setEventCountPerTrigger(int eventCountPerTrigger) {
        this.eventCountPerTrigger = eventCountPerTrigger;
    }

    public int getEventSize() {
        return eventSize;
    }

    public void setEventSize(int eventSize) {
        this.eventSize = eventSize;
    }

    public int getTriggersPerSecond() {
        return triggersPerSecond;
    }

    public void setTriggersPerSecond(int triggersPerSecond) {
        this.triggersPerSecond = triggersPerSecond;
    }

    public boolean isCalculated() {
        return calculated;
    }

    public void setCalculated(boolean calculated) {
        this.calculated = calculated;
    }
}
