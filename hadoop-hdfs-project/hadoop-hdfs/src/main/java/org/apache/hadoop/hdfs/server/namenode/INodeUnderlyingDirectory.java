package org.apache.hadoop.hdfs.server.namenode;

import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.apache.hadoop.fs.PathIsNotDirectoryException;
import org.apache.hadoop.fs.permission.PermissionStatus;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.hdfs.protocol.QuotaExceededException;
import org.apache.hadoop.hdfs.server.namenode.Quota.Counts;
import org.apache.hadoop.hdfs.util.ReadOnlyList;

public class INodeUnderlyingDirectory extends INodeWithAdditionalFields implements INodeDirectoryAttributes {

	protected static final int DEFAULT_FILES_PER_UNDERLYING_DIRECTORY = 5;
	final static byte[] ROOT_NAME = DFSUtil.string2Bytes("");

	private List<INode> children = null;
	private INodeDirectory master;
	private int partitioning;

	/** constructor */
	public INodeUnderlyingDirectory(long id, byte[] name, PermissionStatus permissions, long mtime) {
		super(id, name, permissions, mtime, 0L);
	}

	/**
	 * Copy constructor
	 * 
	 * @param other
	 *            The INodeDirectory to be copied
	 * @param adopt
	 *            Indicate whether or not need to set the parent field of child
	 *            INodes to the new node
	 * @param featuresToCopy
	 *            any number of features to copy to the new node. The method
	 *            will do a reference copy, not a deep copy.
	 */
	public INodeUnderlyingDirectory(INodeUnderlyingDirectory other, boolean adopt, Feature... featuresToCopy) {
		super(other);
		this.children = other.children;
		if (adopt && this.children != null) {
			for (INode child : children) {
				child.setParent(this);
			}
		}
		this.features = featuresToCopy;
	}

	INodeUnderlyingDirectory(INodeWithAdditionalFields other) {
		super(other);
	}

	@Override
	public boolean metadataEquals(INodeDirectoryAttributes other) {
		throw new UnsupportedOperationException("'metadataEquals' is not yet supported operation.");
	}

	@Override
	void recordModification(int latestSnapshotId) throws QuotaExceededException {
		throw new UnsupportedOperationException("'recordModification' is not yet supported operation.");
	}

	@Override
	public Counts cleanSubtree(int snapshotId, int priorSnapshotId, BlocksMapUpdateInfo collectedBlocks,
			List<INode> removedINodes, boolean countDiffChange) throws QuotaExceededException {
		throw new UnsupportedOperationException("'cleanSubtree' is not yet supported operation.");
	}

	@Override
	public void destroyAndCollectBlocks(BlocksMapUpdateInfo collectedBlocks, List<INode> removedINodes) {
		throw new UnsupportedOperationException("'destroyAndCollectBlocks' is not yet supported operation.");
	}

	@Override
	public Counts computeQuotaUsage(Counts counts, boolean useCache, int lastSnapshotId) {
		throw new UnsupportedOperationException("'computeQuotaUsage' is not yet supported operation.");
	}

	@Override
	public byte getStoragePolicyID() {
		throw new UnsupportedOperationException("'getStoragePolicyID' is not yet supported operation.");
	}

	@Override
	public byte getLocalStoragePolicyID() {
		throw new UnsupportedOperationException("'getLocalStoragePolicyID' is not yet supported operation.");
	}

	/** Cast INode to INodeUnderlyingDirectory. */
	public static INodeUnderlyingDirectory valueOf(INode inode, Object path)
			throws FileNotFoundException, PathIsNotDirectoryException {
		if (inode == null) {
			throw new FileNotFoundException("Directory does not exist: " + DFSUtil.path2String(path));
		}
		if (!(inode instanceof INodeUnderlyingDirectory)) {
			throw new PathIsNotDirectoryException(DFSUtil.path2String(path));
		}
		return (INodeUnderlyingDirectory) inode;
	}

	public INodeDirectory getMaster() {
		return master;
	}

	public void setMaster(INodeDirectory master) {
		this.master = master;
	}

	int searchChildren(byte[] name) {
		return children == null ? -1 : Collections.binarySearch(children, name);
	}

	private ReadOnlyList<INode> getCurrentChildrenList() {
		return children == null ? ReadOnlyList.Util.<INode> emptyList() : ReadOnlyList.Util.asReadOnlyList(children);
	}
	
	@Override
	public boolean isUnderlyingDirectory() {
		return true;
	}

	/**
	 * Given a child's name, return the index of the next child
	 *
	 * @param name
	 *            a child's name
	 * @return the index of the next child
	 */
	static int nextChild(ReadOnlyList<INode> children, byte[] name) {
		if (name.length == 0) { // empty name
			return 0;
		}
		int nextPos = ReadOnlyList.Util.binarySearch(children, name) + 1;
		if (nextPos >= 0) {
			return nextPos;
		}
		return -nextPos;
	}

	public boolean addChild(INode node, final boolean setModTime, final int latestSnapshotId)
			throws QuotaExceededException {
		final int low = searchChildren(node.getLocalNameBytes());
		if (low >= 0) {
			return false;
		}

		addChild(node, low);
		if (setModTime) {
			// update modification time of the parent directory
			updateModificationTime(node.getModificationTime(), latestSnapshotId);
		}
		return true;
	}

	public boolean addChild(INode node) {
		final int low = searchChildren(node.getLocalNameBytes());
		if (low >= 0) {
			return false;
		}
		addChild(node, low);
		return true;
	}

	private void addChild(final INode node, final int insertionPoint) {
		if (children == null) {
			children = new ArrayList<INode>();
		}
		node.setParent(this);
		children.add(-insertionPoint - 1, node);

		if (node.getGroupName() == null) {
			node.setGroup(getGroupName());
		}
	}

	@Override
	public ContentSummaryComputationContext computeContentSummary(ContentSummaryComputationContext summary) {
		return computeDirectoryContentSummary(summary);
	}

	// DONE
	ContentSummaryComputationContext computeDirectoryContentSummary(ContentSummaryComputationContext summary) {
		ReadOnlyList<INode> childrenList = getChildrenList();
		// Explicit traversing is done to enable repositioning after
		// relinquishing
		// and reacquiring locks.
		for (int i = 0; i < childrenList.size(); i++) {
			INode child = childrenList.get(i);
			byte[] childName = child.getLocalNameBytes();

			long lastYieldCount = summary.getYieldCount();
			child.computeContentSummary(summary);

			// Check whether the computation was paused in the subtree.
			// The counts may be off, but traversing the rest of children
			// should be made safe.
			if (lastYieldCount == summary.getYieldCount()) {
				continue;
			}
			// The locks were released and reacquired. Check parent first.
			if (!isRoot() && getParent() == null) {
				// Stop further counting and return whatever we have so far.
				break;
			}
			// Obtain the children list again since it may have been modified.
			childrenList = getChildrenList();
			// Reposition in case the children list is changed. Decrement by 1
			// since it will be incremented when loops.
			i = nextChild(childrenList, childName) - 1;
		}

		// Increment the directory count for this directory.
		summary.getCounts().add(Content.UNDERLYING_DIRECTORY, 1);
		// Relinquish and reacquire locks if necessary.
		summary.yield();
		return summary;
	}

	public ReadOnlyList<INode> getChildrenList() {
		return getCurrentChildrenList();
	}

	public void clearChildren() {
		this.children = null;
	}

	@Override
	public void clear() {
		super.clear();
		clearChildren();
	}

	public final int getChildrenNum() {
		return getChildrenList().size();
	}
	
	public INode getChild(byte[] name, int snapshotId) {
      ReadOnlyList<INode> c = getCurrentChildrenList();
      final int i = ReadOnlyList.Util.binarySearch(c, name);
      return i < 0 ? null : c.get(i);
    }
	
	public INodeUnderlyingDirectory getUParent() {
		if (parent!=null && !(parent instanceof INodeUnderlyingDirectory)) {
			throw new IllegalStateException("--- MPSR ---: getUParent() : Parent is not an instance of INodeUnderlyingDirectory[parentId = " + parent.getId() + "]!");
		}
		return (INodeUnderlyingDirectory) parent;
	}

	public int getPartitioning() {
		return partitioning;
	}

	public void setPartitioning(int partitioning) {
		this.partitioning = partitioning;
	}
}
