package eu.uk.ncl.di.pet5o.PATH2iot.input;

/**
 * Configuration holder for a LoRa channel settings.
 * @author Peter Michalák
 */

public class LoraChannelConfig {
     int spreadingFactor;     // usually 7-12 in Europe
     double bandwidth;        // kHz
     double dutyCycle;        // %
     double fairUsageCap;     // s
     int headerSize;          // B
     int preambleSize;        // B
     int codingRate;          // range 1-4; used for error correction - higher values lead to more overhead
     int DE;                  // 1 when data rate optimisation is enabled; 0 otherwise

     public LoraChannelConfig(int spreadingFactor, double bandwidth, double dutyCycle, double fairUsageCap, int headerSize, int preambleSize, int codingRate, int DE) {
          this.spreadingFactor = spreadingFactor;
          this.bandwidth = bandwidth;
          this.dutyCycle = dutyCycle;
          this.fairUsageCap = fairUsageCap;
          this.headerSize = headerSize;
          this.preambleSize = preambleSize;
          this.codingRate = codingRate;
          this.DE = DE;
     }

     public int getSpreadingFactor() {
          return spreadingFactor;
     }

     public double getBandwidth() {
          return bandwidth;
     }

     public double getDutyCycle() {
          return dutyCycle;
     }

     public double getFairUsageCap() {
          return fairUsageCap;
     }

     public int getHeaderSize() {
          return headerSize;
     }

     public int getPreambleSize() {
          return preambleSize;
     }

     public int getCodingRate() {
          return codingRate;
     }

     public int getDE() {
          return DE;
     }
}
