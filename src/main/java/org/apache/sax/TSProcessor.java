package org.apache.sax;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;

/**
 * Implements algorithms for low-level data manipulation.
 * 
 * @author Pavel Senin
 * 
 */
public class TSProcessor {

  /** The latin alphabet, lower case letters a-z. */
  static final char[] ALPHABET = { 'a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm',
      'n', 'o', 'p', 'q', 'r', 's', 't', 'u', 'v', 'w', 'x', 'y', 'z' };


  /**
   * Constructor.
   */
  public TSProcessor() {
    super();
  }
  
  /**
   * Approximate the timeseries using PAA. If the timeseries has some NaN's they are handled as
   * follows: 1) if all values of the piece are NaNs - the piece is approximated as NaN, 2) if there
   * are some (more or equal one) values happened to be in the piece - algorithm will handle it as
   * usual - getting the mean.
   * 
   * @param ts The timeseries to approximate.
   * @param paaSize The desired length of approximated timeseries.
   * @return PAA-approximated timeseries.
   */
  public double[] paa(double[] ts, int paaSize) {
    // fix the length
    int len = ts.length;
    // check for the trivial case
    if (len == paaSize) {
      return Arrays.copyOf(ts, ts.length);
    }
    else {
      if (len % paaSize == 0) {
        return colMeans(reshape(asMatrix(ts), len / paaSize, paaSize));
      }
      else {
        double[] paa = new double[paaSize];
        for (int i = 0; i < len * paaSize; i++) {
          int idx = i / len; // the spot
          int pos = i / paaSize; // the col spot
          paa[idx] = paa[idx] + ts[pos];          
        }
        for (int i = 0; i < paaSize; i++) {
          paa[i] = paa[i] / (double) len;
        }
        return paa;
      }
    }

  }

  /**
   * Converts the timeseries into string using given cuts intervals. Useful for not-normal
   * distribution cuts.
   * 
   * @param vals The timeseries.
   * @param cuts The cut intervals.
   * @return The timeseries SAX representation.
   */
  public char[] ts2String(double[] vals, double[] cuts) {
    char[] res = new char[vals.length];
    for (int i = 0; i < vals.length; i++) {
      res[i] = num2char(vals[i], cuts);
    }
    return res;
  }

  /**
   * Get mapping of a number to char.
   * 
   * @param value the value to map.
   * @param cuts the array of intervals.
   * @return character corresponding to numeric value.
   */
  public char num2char(double value, double[] cuts) {
    int count = 0;
    while ((count < cuts.length) && (cuts[count] <= value)) {
      count++;
    }
    return ALPHABET[count];
  }

  /**
   * Converts index into char.
   * 
   * @param idx The index value.
   * @return The char by index.
   */
  public char num2char(int idx) {
    return ALPHABET[idx];
  }

  /**
   * Get mapping of number to cut index.
   * 
   * @param value the value to map.
   * @param cuts the array of intervals.
   * @return character corresponding to numeric value.
   */
  public int num2index(double value, double[] cuts) {
    int count = 0;
    while ((count < cuts.length) && (cuts[count] <= value)) {
      count++;
    }
    return count;
  }

  /**
   * Extract subseries out of series.
   * 
   * @param series The series array.
   * @param start the fragment start.
   * @param end the fragment end.
   * @return The subseries.
   * @throws IndexOutOfBoundsException If error occurs.
   */
  public double[] subseriesByCopy(double[] series, int start, int end)
      throws IndexOutOfBoundsException {
    if ((start > end) || (start < 0) || (end > series.length)) {
      throw new IndexOutOfBoundsException("Unable to extract subseries, series length: "
          + series.length + ", start: " + start + ", end: " + String.valueOf(end - start));
    }
    return Arrays.copyOfRange(series, start, end);
  }

  /**
   * Prettyfies the timeseries for screen output.
   * 
   * @param series the data.
   * @param df the number format to use.
   * 
   * @return The timeseries formatted for screen output.
   */
  public String seriesToString(double[] series, NumberFormat df) {
    StringBuffer sb = new StringBuffer();
    sb.append('[');
    for (double d : series) {
      sb.append(df.format(d)).append(',');
    }
    sb.delete(sb.length() - 2, sb.length() - 1).append("]");
    return sb.toString();
  }

  /**
   * Returns the m-by-n matrix B whose elements are taken column-wise from A. An error results if A
   * does not have m*n elements.
   * 
   * @param a the source matrix.
   * @param n number of rows in the new matrix.
   * @param m number of columns in the new matrix.
   * 
   * @return reshaped matrix.
   */
  public double[][] reshape(double[][] a, int n, int m) {
    int currentElement = 0;
    int aRows = a.length;

    double[][] res = new double[n][m];

    for (int j = 0; j < m; j++) {
      for (int i = 0; i < n; i++) {
        res[i][j] = a[currentElement % aRows][currentElement / aRows];
        currentElement++;
      }
    }
    return res;
  }

  /**
   * Converts the vector into one-row matrix.
   * 
   * @param vector The vector.
   * @return The matrix.
   */
  public double[][] asMatrix(double[] vector) {
    double[][] res = new double[1][vector.length];
    for (int i = 0; i < vector.length; i++) {
      res[0][i] = vector[i];
    }
    return res;
  }

  /**
   * Computes column means for the matrix.
   * 
   * @param a the input matrix.
   * @return result.
   */
  public double[] colMeans(double[][] a) {
    double[] res = new double[a[0].length];
    for (int j = 0; j < a[0].length; j++) {
      double sum = 0;
      int counter = 0;
      for (int i = 0; i < a.length; i++) {
        if (Double.isNaN(a[i][j]) || Double.isInfinite(a[i][j])) {
          continue;
        }
        sum += a[i][j];
        counter += 1;
      }
      if (counter == 0) {
        res[j] = Double.NaN;
      }
      else {
        res[j] = sum / ((Integer) counter).doubleValue();
      }
    }
    return res;
  }
}
