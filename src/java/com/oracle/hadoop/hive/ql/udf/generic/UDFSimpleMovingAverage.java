package com.oracle.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.udf.UDFType;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Iterator;
import java.lang.Double;

/**
* UDFSimpleMovingAverage
**/

@Description(name = "moving_avg", value = "_FUNC_(x, n) - Returns the moving mean of a set of numbers over a window of n observations")
@UDFType(deterministic = false, stateful = true)
public class UDFSimpleMovingAverage extends UDF
{
  private DoubleWritable result = new DoubleWritable();
  private static ArrayDeque<Double> window;
  int windowSize;
  
  public UDFSimpleMovingAverage() {
    result.set(0);
  }

  public DoubleWritable evaluate(DoubleWritable v, IntWritable n) {
    double sum = 0.0;
    double moving_average;
    double residual;
    if (window == null)
    {
        window = new ArrayDeque<Double>();
    }
            
    //slide the window
    if (window.size() == n.get())
        window.pop();
            
    window.addLast(new Double(v.get()));
            
    // compute the average
    for (Iterator<Double> i = window.iterator(); i.hasNext();)
        sum += i.next().doubleValue();
            
    moving_average = sum/window.size();
    result.set(moving_average);
    return result;
  }
}

