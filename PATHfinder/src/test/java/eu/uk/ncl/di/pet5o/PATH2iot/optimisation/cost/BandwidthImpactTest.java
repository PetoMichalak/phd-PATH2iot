package eu.uk.ncl.di.pet5o.PATH2iot.optimisation.cost;

import eu.uk.ncl.di.pet5o.PATH2iot.input.LoraChannelConfig;
import junit.framework.TestCase;

public class BandwidthImpactTest extends TestCase {

    /**
     * Validate bandwidth airtime calculations
     */
    public void testAirtimeCalculation01() {
        LoraChannelConfig config = new LoraChannelConfig(7, 125, 1, 30, 13, 8, 1, 0);
        BandwidthImpact bandwidthImpact = new BandwidthImpact(config, 1, 300);
        assertEquals(46.34, bandwidthImpact.getAirtime(), 0.01);
        assertFalse(bandwidthImpact.isDutyCycleCompliant());
        assertFalse(bandwidthImpact.isTTNcompliant());
    }

    public void testAirtimeCalculation02() {
        LoraChannelConfig config = new LoraChannelConfig(9, 125, 1, 30, 13, 8, 1, 0);
        BandwidthImpact bandwidthImpact = new BandwidthImpact(config, 25, 3);
        assertEquals(267.26, bandwidthImpact.getAirtime(), 0.01);
        assertFalse(bandwidthImpact.isDutyCycleCompliant());
        assertFalse(bandwidthImpact.isTTNcompliant());
    }

    public void testAirtimeCalculation03() {
        LoraChannelConfig config = new LoraChannelConfig(12, 125, 1, 30, 13, 8, 1, 1);
        BandwidthImpact bandwidthImpact = new BandwidthImpact(config, 50, 0.003);
        assertEquals(2793.47, bandwidthImpact.getAirtime(), 0.01);
        assertTrue(bandwidthImpact.isDutyCycleCompliant());
        assertFalse(bandwidthImpact.isTTNcompliant());
    }

    public void testAirtimeCalculation04() {
        LoraChannelConfig config = new LoraChannelConfig(7, 250, 1, 30, 13, 8, 1, 0);
        BandwidthImpact bandwidthImpact = new BandwidthImpact(config, 222, 1);
        assertEquals(184.45, bandwidthImpact.getAirtime(), 0.01);
        assertFalse(bandwidthImpact.isDutyCycleCompliant());
        assertFalse(bandwidthImpact.isTTNcompliant());
    }

    public void testAirtimeCalculation05() {
        LoraChannelConfig config = new LoraChannelConfig(10, 125, 1, 30, 13, 8, 1, 0);
        BandwidthImpact bandwidthImpact = new BandwidthImpact(config, 12, 1./1800);
        assertEquals(411.65, bandwidthImpact.getAirtime(), 0.01);
        assertTrue(bandwidthImpact.isDutyCycleCompliant());
        assertTrue(bandwidthImpact.isTTNcompliant());
    }

    public void testAirtimeCalculation07() {
        LoraChannelConfig config = new LoraChannelConfig(7, 125, 1, 30, 13, 8, 1, 0);
        BandwidthImpact bandwidthImpact = new BandwidthImpact(config, 8, 1./(5*60));
        assertEquals(56.58, bandwidthImpact.getAirtime(), 0.01);
        assertTrue(bandwidthImpact.isDutyCycleCompliant());
        assertTrue(bandwidthImpact.isTTNcompliant());
    }

}
