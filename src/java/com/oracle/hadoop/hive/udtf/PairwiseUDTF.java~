package com.oracle.hive.udtf;

import java.util.ArrayList;
import java.util.List;
import java.util.Arrays;
 
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
 
@Description(name = "pairwise", value = "_FUNC_(doc) - emits pairwise combinations of an input array")
public class PairwiseUDTF extends GenericUDTF {

 private PrimitiveObjectInspector stringOI = null;
 
  @Override
  public StructObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {
    if (args.length != 1) {
      throw new UDFArgumentException("pairwise() takes exactly one argument");
    }
 
    if (args[0].getCategory() != ObjectInspector.Category.PRIMITIVE
        && ((PrimitiveObjectInspector) args[0]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
      throw new UDFArgumentException("pairwise() takes a string as a parameter");
    }
 
    stringOI = (PrimitiveObjectInspector) args[0];
 
    List<String> fieldNames = new ArrayList<String>(2);
    List<ObjectInspector> fieldOIs = new ArrayList<ObjectInspector>(2);
    fieldNames.add("memberA");
    fieldNames.add("memberB");
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    fieldOIs.add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
    return ObjectInspectorFactory.getStandardStructObjectInspector(fieldNames, fieldOIs);
  }
  
  @Override
  public void process(Object[] record) throws HiveException {
    final String document = (String) stringOI.getPrimitiveJavaObject(record[0]);
 
    if (document == null) {
      return;
    }
    String[] members = document.split(",");
	java.util.Arrays.sort(members);
	for (int i = 0; i < members.length - 1; i++)
		for (int j = 1; j < members.length; j++)
			if (!members[i].equals(members[j]))
				forward(new Object[] {members[i],members[j]});
  }
 
  @Override
  public void close() throws HiveException {
    // do nothing
  }
 
}