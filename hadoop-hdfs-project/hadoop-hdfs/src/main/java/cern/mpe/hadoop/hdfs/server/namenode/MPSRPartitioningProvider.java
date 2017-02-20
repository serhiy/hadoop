package cern.mpe.hadoop.hdfs.server.namenode;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MPSRPartitioningProvider {
	
	public static final int NUM_PARTITIONS = 3;
	
	private static final Map<Integer, List<String>> partitioning = new HashMap<Integer, List<String>> ();
	static {
		partitioning.put(0, new ArrayList<String>());
		partitioning.get(0).add("accMode");
		partitioning.get(0).add("fillNumber");
		partitioning.get(0).add("deviceType");
		
		partitioning.put(1, new ArrayList<String>());
		partitioning.get(1).add("deviceType");
		partitioning.get(1).add("year");
		partitioning.get(1).add("month");
		partitioning.get(1).add("day");
		
		partitioning.put(2, new ArrayList<String>());
		partitioning.get(2).add("accMode");
		partitioning.get(2).add("fillNumber");
		partitioning.get(2).add("deviceType");
		partitioning.get(2).add("location");
	}
	private static final List<String> tagOrder = new ArrayList<String>();
	static {
		tagOrder.add("accMode");
		tagOrder.add("fillNumber");
		tagOrder.add("deviceType");
		tagOrder.add("year");
		tagOrder.add("month");
		tagOrder.add("day");
		tagOrder.add("location");
	}
	
	public static boolean isMpsr(String directory) {
		return true;
	}
	
	public static String getTag(String directory) {
		return directory.split("=")[0];
	}
	
	public static List<String> getPartitioningTags(int part) {
		return new ArrayList<String>(partitioning.get(part));
	}

	public static String orderTags(String src) {
		String [] paths = src.split("\\/");
		
		StringBuffer sb = new StringBuffer("/");
		for (String tag: tagOrder) {
			boolean found = false;
			for (String path: paths) {
				if (getTag(path).equals(tag)) {
					sb.append(path).append("/");
					found = true;
					break;
				}
			}
			
			if (!found) {
				throw new IllegalArgumentException("--- MPSR ---: Tag '" + tag + "' was not recognized!");
			}
		}
		
		//sb.append(paths[paths.length - 1]);
		
		return sb.toString();
	}
}
