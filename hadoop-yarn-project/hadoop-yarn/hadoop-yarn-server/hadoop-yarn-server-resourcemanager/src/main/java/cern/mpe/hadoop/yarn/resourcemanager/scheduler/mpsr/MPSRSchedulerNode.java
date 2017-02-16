package cern.mpe.hadoop.yarn.resourcemanager.scheduler.mpsr;

import java.util.Map;

import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerNode;

public class MPSRSchedulerNode extends SchedulerNode {
	
	private Map<Integer, Float> partitioningTypeShare;

	public MPSRSchedulerNode(RMNode node, boolean usePortForNodeName) {
		super(node, usePortForNodeName);
	}

	@Override
	public void reserveResource(SchedulerApplicationAttempt attempt, Priority priority, RMContainer container) {
	}

	@Override
	public void unreserveResource(SchedulerApplicationAttempt attempt) {
	}

	/**
	 * @return the {@link Map} which represents the amount of blocks (in %) storing the data
	 * using determined partitioning type.
	 */
	public Map<Integer, Float> partitioningTypes() {
		return partitioningTypeShare;
	}
}
