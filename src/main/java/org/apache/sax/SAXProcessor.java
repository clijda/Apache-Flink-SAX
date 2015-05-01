package org.apache.sax;

import java.util.ArrayList;
import java.util.Arrays;
import org.apache.commons.lang3.ArrayUtils;

import org.apache.sax.SAXRecords;

/**
 * Implements SAX algorithms.
 * 
 * @author Pavel Senin
 * 
 */
public final class SAXProcessor {

  private final TSProcessor tsProcessor;

  /**
   * Constructor.
   */
  public SAXProcessor() {
    super();
    this.tsProcessor = new TSProcessor();
  }

  /**
   * Converts the input time series into a SAX data structure via sliding window and Z
   * normalization.
   * 
   * @param ts the input data.
   * @param windowSize the sliding window size.
   * @param paaSize the PAA size.
   * @param cuts the Alphabet cuts.
   * @param nThreshold the normalization threshold value.
   * @param strategy the NR strategy.
   * 
   * @return SAX representation of the time series.
   */
  public SAXRecords ts2saxViaWindow(ArrayList<Double> ts, int windowSize, int paaSize, double[] cuts,
      NumerosityReductionStrategy strategy, double nThreshold) {

    // the resulting data structure init
    //
    SAXRecords saxFrequencyData = new SAXRecords();
    // scan across the time series extract sub sequences, and convert them to strings
    char[] previousString = null;
    int count = 0;
    for (int i = 0; i < ts.size() - (windowSize - 1); i++) {
    	// Convert ArrayList to Array
    	Double[] ts_1 = ts.toArray(new Double[ts.size()]);
    	double[] ts_2 = ArrayUtils.toPrimitive(ts_1);
    	// Get timeserie subSection
        double[] subSection = Arrays.copyOfRange(ts_2, i, i+windowSize);

      // perform PAA conversion if needed
      double[] paa = tsProcessor.paa(subSection, paaSize);

      // Convert the PAA to a string.
      char[] currentString = tsProcessor.ts2String(paa, cuts);

      if (null != previousString) {

        if (NumerosityReductionStrategy.EXACT.equals(strategy)
            && Arrays.equals(previousString, currentString)) {
          // NumerosityReduction
          continue;
        }
        else if (NumerosityReductionStrategy.MINDIST.equals(strategy)
            && checkMinDistIsZero(previousString, currentString)) {
          continue;
        }

      }

      previousString = currentString;
      count++;
      saxFrequencyData.add(currentString, i);
    }
    System.out.println("COUNT " + count);
    return saxFrequencyData;

  }
  
  /**
   * Check for mindist.
   * 
   * @param a first string.
   * @param b second string.
   * @return true if mindist between strings is zero.
   */
  public boolean checkMinDistIsZero(char[] a, char[] b) {
    for (int i = 0; i < a.length; i++) {
      if (charDistance(a[i], b[i]) > 1) {
        return false;
      }
    }
    return true;
  }

  /**
   * Convert the timeseries into SAX string representation.
   * 
   * @param ts the timeseries.
   * @param paaSize the PAA size.
   * @param cuts the alphabet cuts.
   * @param nThreshold the normalization thresholds.
   * 
   * @return The SAX representation for timeseries.
   */
  public char[] ts2string(double[] ts, int paaSize, double[] cuts, double nThreshold) {	  
      // perform PAA conversion
      double[] paa = tsProcessor.paa(ts, paaSize);
      return tsProcessor.ts2String(paa, cuts);    
  }

  /**
   * Compute the distance between the two chars based on the ASCII symbol codes.
   * 
   * @param a The first char.
   * @param b The second char.
   * @return The distance.
   */
  public int charDistance(char a, char b) {
    return Math.abs(Character.getNumericValue(a) - Character.getNumericValue(b));
  }

  /**
   * Compute the distance between the two strings, this function use the numbers associated with
   * ASCII codes, i.e. distance between a and b would be 1.
   * 
   * @param a The first string.
   * @param b The second string.
   * @return The pairwise distance.
   * @throws SAXException if length are differ.
   */
  public int strDistance(char[] a, char[] b) throws SAXException {
    if (a.length == b.length) {
      int distance = 0;
      for (int i = 0; i < a.length; i++) {
        int tDist = Math.abs(Character.getNumericValue(a[i]) - Character.getNumericValue(b[i]));
        if (tDist > 1) {
          distance += tDist;
        }
      }
      return distance;
    }
    else {
      throw new SAXException("Unable to compute SAX distance, string lengths are not equal");
    }
  }

  /**
   * This function implements SAX MINDIST function which uses alphabet based distance matrix.
   * 
   * @param a The SAX string.
   * @param b The SAX string.
   * @param distanceMatrix The distance matrix to use.
   * @return distance between strings.
   * @throws SAXException If error occurs.
   */
  public double saxMinDist(char[] a, char[] b, double[][] distanceMatrix) throws SAXException {
    if (a.length == b.length) {
      double dist = 0.0D;
      for (int i = 0; i < a.length; i++) {
        if (Character.isLetter(a[i]) && Character.isLetter(b[i])) {
          int numA = Character.getNumericValue(a[i]) - 10;
          int numB = Character.getNumericValue(b[i]) - 10;
          if (numA > 19 || numA < 0 || numB > 19 || numB < 0) {
            throw new SAXException("The character index greater than 19 or less than 0!");
          }
          double localDist = distanceMatrix[numA][numB];
          dist += localDist;
        }
        else {
          throw new SAXException("Non-literal character found!");
        }
      }
      return dist;
    }
    else {
      throw new SAXException("Data arrays lengths are not equal!");
    }
  }

  /**
   * Generic method to convert the milliseconds into the elapsed time string.
   * 
   * @param start Start timestamp.
   * @param finish End timestamp.
   * @return String representation of the elapsed time.
   */
  /*public static String timeToString(long start, long finish) {

    Duration duration = new Duration(finish - start); // in milliseconds
    PeriodFormatter formatter = new PeriodFormatterBuilder().appendDays().appendSuffix("d")
        .appendHours().appendSuffix("h").appendMinutes().appendSuffix("m").appendSeconds()
        .appendSuffix("s").appendMillis().appendSuffix("ms").toFormatter();
    String formatted = formatter.print(duration.toPeriod());

    return formatted;

  }*/
}
