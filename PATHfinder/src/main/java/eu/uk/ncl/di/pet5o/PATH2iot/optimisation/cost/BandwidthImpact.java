package eu.uk.ncl.di.pet5o.PATH2iot.optimisation.cost;

import eu.uk.ncl.di.pet5o.PATH2iot.input.LoraChannelConfig;

public class BandwidthImpact {
    LoraChannelConfig config;
    int payload;
    double msgsPerSecond;

    public BandwidthImpact(LoraChannelConfig loraChannelConfig, int payload, double msgsPerSecond) {
        this.config = loraChannelConfig;
        this.payload = payload;
        this.msgsPerSecond = msgsPerSecond;
    }

    /**
     * Calculate the airtime given the configuration according to LoRa modem Designer’s Guide AN1200.13 (wwww.rs-online.com)
     * returns duration (in ms) of airtime needed for packet transmission
     */
    public double getAirtime() {
        // calc T_sym - symbol duration
        double Tsym = (Math.pow(2, config.getSpreadingFactor()) / config.getBandwidth());

        // calc T_preamble
        double Tpreamble = (config.getPreambleSize() + 4.25) * Tsym;

        // calc payload airtime
        int H = 0;  // 0 if header is included; 1 if it is not present (really logical..)
        double Tpayload = (8 + Math.max(Math.ceil((8 * (payload + config.getHeaderSize()) -
                4 * config.getSpreadingFactor() + 28 + 16 - 20 * H) /
                (4.*(config.getSpreadingFactor() - 2 * config.getDE()))) *
                (config.getCodingRate() + 4), 0))*Tsym;

        return Tpreamble + Tpayload;
    }

    /**
     * Calculate number of messages that are needed per day
      */
    public double getNumberOfMessagesPerDay() {
        return msgsPerSecond * 60 * 60 * 24;
    }

    /**
     * Check whether the duty cycle restriction is satisfied
      */
    public boolean isDutyCycleCompliant() {
        double timeBetweenMessages = 1000 / msgsPerSecond;
        return (getAirtime() * (100 - config.getDutyCycle()) < timeBetweenMessages);
    }

    /**
     * Check whether the plan satisfies The Things Network fair policy rules.
     */
    public boolean isTTNcompliant() {
        return (getNumberOfMessagesPerDay() * getAirtime() <= config.getFairUsageCap() * 1000);
    }
}
