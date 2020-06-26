package eu.uk.ncl.di.pet5o.PATH2iot.operator;

public class ComplexDataOut {
    private double eventCountPerTrigger;
    private int eventSize;               // B
    private double triggersPerSecond;
    private boolean calculated;

    public ComplexDataOut() {
        eventCountPerTrigger = -1;
        eventSize = -1;
        triggersPerSecond = -1;
        calculated = false;
    }

    public double getEventCountPerTrigger() {
        return eventCountPerTrigger;
    }

    public void setEventCountPerTrigger(double eventCountPerTrigger) {
        this.eventCountPerTrigger = eventCountPerTrigger;
    }

    public int getEventSize() {
        return eventSize;
    }

    public void setEventSize(int eventSize) {
        this.eventSize = eventSize;
    }

    public double getTriggersPerSecond() {
        return triggersPerSecond;
    }

    public void setTriggersPerSecond(double triggersPerSecond) {
        this.triggersPerSecond = triggersPerSecond;
    }

    public boolean isCalculated() {
        return calculated;
    }

    public void setCalculated(boolean calculated) {
        this.calculated = calculated;
    }
}
