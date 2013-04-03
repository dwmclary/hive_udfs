package com.oracle.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
import java.util.Collections;
import java.util.Random;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

public class PrefixSumMovingAverage {
    static class PrefixSumEntry implements Comparable 
    {
        int period;
        double value;
        double prefixSum;
        double subsequenceTotal;
        double movingAverage;
        public int compareTo(Object other)
        {
            PrefixSumEntry o = (PrefixSumEntry)other;
            if (period < o.period)
                return -1;
            if (period > o.period)
                return 1;
            return 0;
        }
    }
    
    //class variables
    private int windowSize;
    private ArrayList<PrefixSumEntry> entries;
    
    public PrefixSumMovingAverage()
    {
        windowSize = 0;
    }
    
    public void reset()
    {
        windowSize = 0;
        entries = null;
    }
    
    public boolean isReady()
    {
        return (windowSize > 0);
    }
    
    /**
   * Sets the window for prefix sum computations
   *
   * @param window_size Size of the window for moving average
   */
  public void allocate(int window_size) {
    windowSize = window_size;
    entries = new ArrayList<PrefixSumEntry>();
  }


  @SuppressWarnings("unchecked")
  public void merge(List<DoubleWritable> other)
  {
    if (other == null)
        return;
    
    // if this is an empty buffer, just copy in other
    // but deserialize the list
    if (windowSize == 0)
    {
        windowSize = (int)other.get(0).get();
        entries = new ArrayList<PrefixSumEntry>();
        // we're serialized as period, value, period, value
        for (int i = 1; i < other.size(); i+=2)
        {
            PrefixSumEntry e = new PrefixSumEntry();
            e.period = (int)other.get(i).get();
            e.value = other.get(i+1).get();
            entries.add(e);
        }
    }
    
    // if we already have a buffer, we need to add these entries
    else
    {
        // we're serialized as period, value, period, value
        for (int i = 1; i < other.size(); i+=2)
        {
            PrefixSumEntry e = new PrefixSumEntry();
            e.period = (int)other.get(i).get();
            e.value = other.get(i+1).get();
            entries.add(e);
        }
    }
    
    // sort and recompute
    Collections.sort(entries);
    // update the table
    // prefixSums first
    double prefixSum = 0;
    for(int i = 0; i < entries.size(); i++)
    {
        PrefixSumEntry thisEntry = entries.get(i);
        prefixSum += thisEntry.value;
        thisEntry.prefixSum = prefixSum;
        entries.set(i, thisEntry);
    }
    
    // now do the subsequence totals and moving averages
    for(int i = 0; i < entries.size(); i++)
    {
        double subsequenceTotal;
        double movingAverage;
        PrefixSumEntry thisEntry = entries.get(i);
        PrefixSumEntry backEntry = null;
        if (i >= windowSize)
            backEntry = entries.get(i-windowSize);
        if (backEntry != null)
        {
            subsequenceTotal = thisEntry.prefixSum - backEntry.prefixSum;
            
        }
        else
        {
            subsequenceTotal = thisEntry.prefixSum;
        }

        movingAverage = subsequenceTotal/(double)windowSize;
	thisEntry.subsequenceTotal = subsequenceTotal;
	thisEntry.movingAverage = movingAverage;
	entries.set(i, thisEntry);
    }
  }
  
  public int tableSize()
  {
    return entries.size();
  }
  
  public PrefixSumEntry getEntry(int index)
  {
    return entries.get(index);
  }

  private boolean needsSorting(ArrayList<PrefixSumEntry> entries)
  {
      PrefixSumEntry previous = null;
      for (PrefixSumEntry current:entries)
	  {
	      if (previous != null && current.compareTo(previous) < 0)
		  return true;
	  }
      return false;
  }
  
  @SuppressWarnings("unchecked")
  public void add(int period, double v)
  {
    //Add a new entry to the list and update table
    PrefixSumEntry e = new PrefixSumEntry();
    e.period = period;
    e.value = v;
    entries.add(e);
    // do we need to ensure this is sorted?
    //if (needsSorting(entries))
    	Collections.sort(entries);
    // update the table
    // prefixSums first
    double prefixSum = 0;
    for(int i = 0; i < entries.size(); i++)
    {
        PrefixSumEntry thisEntry = entries.get(i);
        prefixSum += thisEntry.value;
        thisEntry.prefixSum = prefixSum;
        entries.set(i, thisEntry);
    }
    
    // now do the subsequence totals and moving averages
    for(int i = 0; i < entries.size(); i++)
    {
        double subsequenceTotal;
        double movingAverage;
        PrefixSumEntry thisEntry = entries.get(i);
        PrefixSumEntry backEntry = null;
        if (i >= windowSize)
            backEntry = entries.get(i-windowSize);
        if (backEntry != null)
        {
            subsequenceTotal = thisEntry.prefixSum - backEntry.prefixSum;
            
        }
        else
        {
            subsequenceTotal = thisEntry.prefixSum;
        }
        movingAverage = subsequenceTotal/(double)windowSize;
	thisEntry.subsequenceTotal = subsequenceTotal;
	thisEntry.movingAverage = movingAverage;
	entries.set(i, thisEntry);
    }
  }
  
  public ArrayList<DoubleWritable> serialize()
  {
    ArrayList<DoubleWritable> result = new ArrayList<DoubleWritable>();
    
    result.add(new DoubleWritable(windowSize));
    if (entries != null)
    {
        for (PrefixSumEntry i : entries)
        {
            result.add(new DoubleWritable(i.period));
            result.add(new DoubleWritable(i.value));
        }
    }
    return result;
  }
}
