package cern.mpe.hadoop.yarn.resourcemanager.scheduler.mpsr;

import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.Priority;
import org.apache.hadoop.yarn.api.records.Resource;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.resource.ResourceWeights;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.ActiveUsersManager;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Queue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplicationAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.Schedulable;

public class MPSRAppAttemp extends SchedulerApplicationAttempt implements Schedulable {

	public MPSRAppAttemp(ApplicationAttemptId applicationAttemptId, String user, Queue queue,
			ActiveUsersManager activeUsersManager, RMContext rmContext) {
		super(applicationAttemptId, user, queue, activeUsersManager, rmContext);
	}

	@Override
	public String getName() {
		return null;
	}

	@Override
	public Resource getDemand() {
		return null;
	}

	@Override
	public Resource getResourceUsage() {
		return null;
	}

	@Override
	public Resource getMinShare() {
		return null;
	}

	@Override
	public Resource getMaxShare() {
		return null;
	}

	@Override
	public ResourceWeights getWeights() {
		return null;
	}

	@Override
	public long getStartTime() {
		return 0;
	}

	@Override
	public Priority getPriority() {
		return null;
	}

	@Override
	public void updateDemand() {
	}

	@Override
	public Resource assignContainer(FSSchedulerNode node) {
		return null;
	}

	@Override
	public RMContainer preemptContainer() {
		return null;
	}

	@Override
	public Resource getFairShare() {
		return null;
	}

	@Override
	public void setFairShare(Resource fairShare) {
	}
}
