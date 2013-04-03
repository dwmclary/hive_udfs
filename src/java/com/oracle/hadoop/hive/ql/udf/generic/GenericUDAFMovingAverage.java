package com.oracle.hadoop.hive.ql.udf.generic;

import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StandardListObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.util.StringUtils;

import org.apache.hadoop.hive.serde2.io.DoubleWritable;


/**
 * GenericUDAFMovingAverage
 *
 */
@Description(name = "moving_avg", value = "_FUNC_(p, x, n) - Returns the moving mean of a set of numbers over a window of n observations")
public class GenericUDAFMovingAverage extends AbstractGenericUDAFResolver {

    static final Log LOG = LogFactory.getLog(GenericUDAFMovingAverage.class.getName());
    
    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] parameters) throws SemanticException {
        //TODO: Type-checking
        
        //We need exactly three parameters
        if (parameters.length != 3) {
            throw new UDFArgumentTypeException(parameters.length - 1, "Moving Average requires 3 parameters");
        }
        
        //check the first parameter to make sure they type is numeric
        
        if (parameters[0].getCategory() != ObjectInspector.Category.PRIMITIVE)
        {
            throw new UDFArgumentTypeException(0, "Only primitive, numeric types can have a moving average but "+
            parameters[0].getTypeName() + "was passed.");
        }
        
        // if it's a primative, let's make sure it's numeric
        switch(((PrimitiveTypeInfo) parameters[0]).getPrimitiveCategory()) {
        //fall through all numeric primitives
        case FLOAT:
        case DOUBLE:
        case INT:
        case LONG:
        case SHORT:
            break;
        default:
            throw new UDFArgumentTypeException(0, "Only numeric type arguments (excluding bytes and timestamps) are accepted"+
                "but " + parameters[0].getTypeName() + " was passed.");
        }
        
        
        // check the second parameter
        if (parameters[1].getCategory() != ObjectInspector.Category.PRIMITIVE)
        {
            throw new UDFArgumentTypeException(0, "Only primitive, numeric types can have a moving average but "+
            parameters[1].getTypeName() + "was passed.");
        }
        
        // if it's a primative, let's make sure it's numeric
        switch(((PrimitiveTypeInfo) parameters[1]).getPrimitiveCategory()) {
        //fall through all numeric primitives
        case FLOAT:
        case DOUBLE:
        case INT:
        case LONG:
        case SHORT:
            break;
        default:
            throw new UDFArgumentTypeException(0, "Only numeric type arguments (excluding bytes and timestamps) are accepted"+
                "but " + parameters[1].getTypeName() + " was passed.");
        }
        
        // ensure that the window size is an integer
        if (parameters[2].getCategory() != ObjectInspector.Category.PRIMITIVE)
        {
            throw new UDFArgumentTypeException(1, "Only primitive integer types are accepted by moving_avg.");
        }
        
        if (((PrimitiveTypeInfo) parameters[2]).getPrimitiveCategory() !=
            PrimitiveObjectInspector.PrimitiveCategory.INT)
        {
            throw new UDFArgumentTypeException(1, "Only primitive integer types are accepted by moving_avg");
        }
        
        return new GenericUDAFMovingAverageEvaluator();
    }
    
    public static class GenericUDAFMovingAverageEvaluator extends GenericUDAFEvaluator {
        //TODO: UDAF logic
            
        // input inspectors for PARTIAL1 and COMPLETE
        private PrimitiveObjectInspector periodOI;
        private PrimitiveObjectInspector inputOI;
        private PrimitiveObjectInspector windowSizeOI;
        
        // input inspectors for PARTIAL2 and FINAL
        // list for MAs and one for residuals
        private StandardListObjectInspector loi;
        
        
        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
        
            super.init(m, parameters);
            
            // initialize input inspectors
            if (m == Mode.PARTIAL1 || m == Mode.COMPLETE)
            {
                assert(parameters.length == 3);
                periodOI = (PrimitiveObjectInspector) parameters[0];
                inputOI = (PrimitiveObjectInspector) parameters[1];
                windowSizeOI = (PrimitiveObjectInspector) parameters[2];
            }
            
            else
            {
                loi = (StandardListObjectInspector) parameters[0];
            }
            
              // init output object inspectors
            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                // The output of a partial aggregation is a list of doubles representing the
                // moving average being constructed.
                // the first element in the list will be the window size
                // 
                return ObjectInspectorFactory.getStandardListObjectInspector(
                    PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
            } 
            else {
                // The output of FINAL and COMPLETE is a full aggregation, which is a
                // list of DoubleWritable structs that represent the final histogram as
                // (x,y) pairs of bin centers and heights.
                
                ArrayList<ObjectInspector> foi = new ArrayList<ObjectInspector>();
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
		foi.add(PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
                ArrayList<String> fname = new ArrayList<String>();
                fname.add("period");
                fname.add("moving_average");
		fname.add("residual");

                return ObjectInspectorFactory.getStandardListObjectInspector(
                 ObjectInspectorFactory.getStandardStructObjectInspector(fname, foi) );
            }
        }
        
    @Override
    public Object terminatePartial(AggregationBuffer agg) throws HiveException {
      // return an ArrayList where the first parameter is the window size
      MaAgg myagg = (MaAgg) agg;
      return myagg.prefixSum.serialize();
    }

    @Override
    public Object terminate(AggregationBuffer agg) throws HiveException {
      // final return value goes here
      MaAgg myagg = (MaAgg) agg;
      
      if (myagg.prefixSum.tableSize() < 1)
      {
        return null;
      }
      
      else
      {
        ArrayList<DoubleWritable[]> result = new ArrayList<DoubleWritable[]>();
        for (int i = 0; i < myagg.prefixSum.tableSize(); i++)
        {
	    double residual = myagg.prefixSum.getEntry(i).value - myagg.prefixSum.getEntry(i).movingAverage;

            DoubleWritable[] entry = new DoubleWritable[3];
            entry[0] = new DoubleWritable(myagg.prefixSum.getEntry(i).period);
            entry[1] = new DoubleWritable(myagg.prefixSum.getEntry(i).movingAverage);
	    entry[2] = new DoubleWritable(residual);
            result.add(entry);
        }
        
        return result;
      }
      
    }

	@SuppressWarnings("unchecked")
    @Override
    public void merge(AggregationBuffer agg, Object partial) throws HiveException {
        // if we're merging two separate sets we're creating one table that's doubly long
        
        if (partial != null)
        {
            MaAgg myagg = (MaAgg) agg;
            List<DoubleWritable> partialMovingAverage = (List<DoubleWritable>) loi.getList(partial);
            myagg.prefixSum.merge(partialMovingAverage);
        }
    }

    @Override
    public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
    
      assert (parameters.length == 3);
      
      if (parameters[0] == null || parameters[1] == null || parameters[2] == null)
      {
        return;
      }
      
      MaAgg myagg = (MaAgg) agg;
      
      // Parse out the window size just once if we haven't done so before.  We need a window of at least 1,
      // otherwise there's no window.
      if (!myagg.prefixSum.isReady())
      {
        int windowSize = PrimitiveObjectInspectorUtils.getInt(parameters[2], windowSizeOI);
        if (windowSize < 1)
        {
            throw new HiveException(getClass().getSimpleName() + " needs a window size >= 1");
        }
        myagg.prefixSum.allocate(windowSize);
      }
      
      //Add the current data point and compute the average

      int p = PrimitiveObjectInspectorUtils.getInt(parameters[0], inputOI);
      double v = PrimitiveObjectInspectorUtils.getDouble(parameters[1], inputOI);
      myagg.prefixSum.add(p,v);
      
    }

    // Aggregation buffer definition and manipulation methods 
    static class MaAgg implements AggregationBuffer {
        PrefixSumMovingAverage prefixSum;
    };

    @Override
    public AggregationBuffer getNewAggregationBuffer() throws HiveException {
      MaAgg result = new MaAgg();
      reset(result);
      return result;
    }

    @Override
    public void reset(AggregationBuffer agg) throws HiveException {
        MaAgg myagg = (MaAgg) agg;
        myagg.prefixSum = new PrefixSumMovingAverage();
        myagg.prefixSum.reset();
    }
    }
    
}
    
    