package cern.mpe.hadoop.yarn.resourcemanager.scheduler.mpsr;

import java.io.IOException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.yarn.api.records.ApplicationAttemptId;
import org.apache.hadoop.yarn.api.records.ApplicationId;
import org.apache.hadoop.yarn.api.records.ContainerId;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.QueueACL;
import org.apache.hadoop.yarn.api.records.QueueInfo;
import org.apache.hadoop.yarn.api.records.QueueUserACLInfo;
import org.apache.hadoop.yarn.api.records.ResourceRequest;
import org.apache.hadoop.yarn.server.resourcemanager.RMContext;
import org.apache.hadoop.yarn.server.resourcemanager.recovery.RMStateStore.RMState;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMApp;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmapp.RMAppRejectedEvent;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainer;
import org.apache.hadoop.yarn.server.resourcemanager.rmcontainer.RMContainerEventType;
import org.apache.hadoop.yarn.server.resourcemanager.rmnode.RMNode;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.AbstractYarnScheduler;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.Allocation;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.QueueMetrics;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerApplication;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.SchedulerUtils;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppAttemptRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.AppRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerExpiredSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.ContainerRescheduledEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeAddedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeRemovedSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeResourceUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.NodeUpdateSchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.event.SchedulerEvent;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSAppAttempt;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSLeafQueue;
import org.apache.hadoop.yarn.server.resourcemanager.scheduler.fair.FSSchedulerNode;
import org.apache.hadoop.yarn.util.resource.Resources;

public class MPSRScheduler extends AbstractYarnScheduler<MPSRAppAttemp, MPSRSchedulerNode> {

	private static final Log LOG = LogFactory.getLog(MPSRScheduler.class);
	
	private boolean usePortForNodeName;
	
	public MPSRScheduler(String name) {
		super(name);
	}

	@Override
	public void setRMContext(RMContext rmContext) {
	}

	@Override
	public void reinitialize(Configuration conf, RMContext rmContext) throws IOException {
	}

	@Override
	public QueueInfo getQueueInfo(String queueName, boolean includeChildQueues, boolean recursive) throws IOException {
		return null;
	}

	@Override
	public List<QueueUserACLInfo> getQueueUserAclInfo() {
		return null;
	}

	@Override
	public int getNumClusterNodes() {
		return 0;
	}

	@Override
	public Allocation allocate(ApplicationAttemptId appAttemptId, List<ResourceRequest> ask, List<ContainerId> release,
			List<String> blacklistAdditions, List<String> blacklistRemovals) {
		return null;
	}

	@Override
	public QueueMetrics getRootQueueMetrics() {
		return null;
	}

	@Override
	public boolean checkAccess(UserGroupInformation callerUGI, QueueACL acl, String queueName) {
		return false;
	}

	@Override
	public List<ApplicationAttemptId> getAppsInQueue(String queueName) {
		return null;
	}

	@Override
	public void handle(SchedulerEvent event) {
		/*switch (event.getType()) {
	    case NODE_ADDED:
	      if (!(event instanceof NodeAddedSchedulerEvent)) {
	        throw new RuntimeException("Unexpected event type: " + event);
	      }
	      NodeAddedSchedulerEvent nodeAddedEvent = (NodeAddedSchedulerEvent)event;
	      addNode(nodeAddedEvent.getAddedRMNode());
	      recoverContainersOnNode(nodeAddedEvent.getContainerReports(),
	          nodeAddedEvent.getAddedRMNode());
	      break;
	    case NODE_REMOVED:
	      if (!(event instanceof NodeRemovedSchedulerEvent)) {
	        throw new RuntimeException("Unexpected event type: " + event);
	      }
	      NodeRemovedSchedulerEvent nodeRemovedEvent = (NodeRemovedSchedulerEvent)event;
	      removeNode(nodeRemovedEvent.getRemovedRMNode());
	      break;
	    case NODE_UPDATE:
	      if (!(event instanceof NodeUpdateSchedulerEvent)) {
	        throw new RuntimeException("Unexpected event type: " + event);
	      }
	      NodeUpdateSchedulerEvent nodeUpdatedEvent = (NodeUpdateSchedulerEvent)event;
	      nodeUpdate(nodeUpdatedEvent.getRMNode());
	      break;
	    case APP_ADDED:
	      if (!(event instanceof AppAddedSchedulerEvent)) {
	        throw new RuntimeException("Unexpected event type: " + event);
	      }
	      AppAddedSchedulerEvent appAddedEvent = (AppAddedSchedulerEvent) event;
	      addApplication(appAddedEvent.getApplicationId(),
	        appAddedEvent.getQueue(), appAddedEvent.getUser(),
	        appAddedEvent.getIsAppRecovering());
	      break;
	    case APP_REMOVED:
	      if (!(event instanceof AppRemovedSchedulerEvent)) {
	        throw new RuntimeException("Unexpected event type: " + event);
	      }
	      AppRemovedSchedulerEvent appRemovedEvent = (AppRemovedSchedulerEvent)event;
	      removeApplication(appRemovedEvent.getApplicationID(),
	        appRemovedEvent.getFinalState());
	      break;
	    case NODE_RESOURCE_UPDATE:
	      if (!(event instanceof NodeResourceUpdateSchedulerEvent)) {
	        throw new RuntimeException("Unexpected event type: " + event);
	      }
	      NodeResourceUpdateSchedulerEvent nodeResourceUpdatedEvent = 
	          (NodeResourceUpdateSchedulerEvent)event;
	      updateNodeResource(nodeResourceUpdatedEvent.getRMNode(),
	            nodeResourceUpdatedEvent.getResourceOption());
	      break;
	    case APP_ATTEMPT_ADDED:
	      if (!(event instanceof AppAttemptAddedSchedulerEvent)) {
	        throw new RuntimeException("Unexpected event type: " + event);
	      }
	      AppAttemptAddedSchedulerEvent appAttemptAddedEvent =
	          (AppAttemptAddedSchedulerEvent) event;
	      addApplicationAttempt(appAttemptAddedEvent.getApplicationAttemptId(),
	        appAttemptAddedEvent.getTransferStateFromPreviousAttempt(),
	        appAttemptAddedEvent.getIsAttemptRecovering());
	      break;
	    case APP_ATTEMPT_REMOVED:
	      if (!(event instanceof AppAttemptRemovedSchedulerEvent)) {
	        throw new RuntimeException("Unexpected event type: " + event);
	      }
	      AppAttemptRemovedSchedulerEvent appAttemptRemovedEvent =
	          (AppAttemptRemovedSchedulerEvent) event;
	      removeApplicationAttempt(
	          appAttemptRemovedEvent.getApplicationAttemptID(),
	          appAttemptRemovedEvent.getFinalAttemptState(),
	          appAttemptRemovedEvent.getKeepContainersAcrossAppAttempts());
	      break;
	    case CONTAINER_EXPIRED:
	      if (!(event instanceof ContainerExpiredSchedulerEvent)) {
	        throw new RuntimeException("Unexpected event type: " + event);
	      }
	      ContainerExpiredSchedulerEvent containerExpiredEvent =
	          (ContainerExpiredSchedulerEvent)event;
	      ContainerId containerId = containerExpiredEvent.getContainerId();
	      completedContainer(getRMContainer(containerId),
	          SchedulerUtils.createAbnormalContainerStatus(
	              containerId,
	              SchedulerUtils.EXPIRED_CONTAINER),
	          RMContainerEventType.EXPIRE);
	      break;
	    case CONTAINER_RESCHEDULED:
	      if (!(event instanceof ContainerRescheduledEvent)) {
	        throw new RuntimeException("Unexpected event type: " + event);
	      }
	      ContainerRescheduledEvent containerRescheduledEvent =
	          (ContainerRescheduledEvent) event;
	      RMContainer container = containerRescheduledEvent.getContainer();
	      recoverResourceRequestForContainer(container);
	      break;
	    default:
	      LOG.error("Unknown event arrived at FairScheduler: " + event.toString());
	    }*/
	}
	
	private synchronized void addNode(RMNode node) {
	    MPSRSchedulerNode schedulerNode = new MPSRSchedulerNode(node, usePortForNodeName);
	    nodes.put(node.getNodeID(), schedulerNode);
	    Resources.addTo(clusterResource, schedulerNode.getTotalResource());
	    //updateRootQueueMetrics();

	    //queueMgr.getRootQueue().setSteadyFairShare(clusterResource);
	    //queueMgr.getRootQueue().recomputeSteadyShares();
	    LOG.info("Added node " + node.getNodeAddress() +
	        " cluster capacity: " + clusterResource);
	}
	
	private void updateRootQueueMetrics() {
		//rootMetrics.setAvailableResourcesToQueue(Resources.subtract(clusterResource, rootMetrics.getAllocatedResources()));
	}
	
	protected synchronized void addApplication(ApplicationId applicationId, String queueName, String user, boolean isAppRecovering) {
	    /*if (queueName == null || queueName.isEmpty()) {
	    	String message = "Reject application " + applicationId + " submitted by user " + user + " with an empty queue name.";
	    	LOG.info(message);
	    	rmContext.getDispatcher().getEventHandler().handle(new RMAppRejectedEvent(applicationId, message));
	    	return;
	    }

	    RMApp rmApp = rmContext.getRMApps().get(applicationId);
	    FSLeafQueue queue = assignToQueue(rmApp, queueName, user);
		if (queue == null) {
			return;
		}

		// Enforce ACLs
		UserGroupInformation userUgi = UserGroupInformation.createRemoteUser(user);
		if (!queue.hasAccess(QueueACL.SUBMIT_APPLICATIONS, userUgi) && !queue.hasAccess(QueueACL.ADMINISTER_QUEUE, userUgi)) {
			String msg = "User " + userUgi.getUserName() + " cannot submit applications to queue " + queue.getName();
			LOG.info(msg);
			rmContext.getDispatcher().getEventHandler().handle(new RMAppRejectedEvent(applicationId, msg));
			return;
		}
  
		SchedulerApplication<MPSRAppAttemp> application = new SchedulerApplication<MPSRAppAttemp>(queue, user);
		applications.put(applicationId, application);
		//TODO: submit app check here
		queue.getMetrics().submitApp(user);

		LOG.info("Accepted application " + applicationId + " from user: " + user + ", in queue: " + queueName + ", currently num of applications: " + applications.size());
		if (isAppRecovering) {
			if (LOG.isDebugEnabled()) {
				LOG.debug(applicationId + " is recovering. Skip notifying APP_ACCEPTED");
			}
		} else {
				rmContext.getDispatcher().getEventHandler().handle(new RMAppEvent(applicationId, RMAppEventType.APP_ACCEPTED));
		}*/
	}

	@Override
	public void recover(RMState state) throws Exception {
	}

	@Override
	protected void completedContainer(RMContainer rmContainer, ContainerStatus containerStatus,
			RMContainerEventType event) {
	}

}
