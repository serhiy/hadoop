package org.apache.hadoop.hdfs.server.namenode;

import static org.junit.Assert.fail;

import java.io.IOException;

import org.apache.commons.math3.stat.clustering.Cluster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DFSConfigKeys;
import org.apache.hadoop.hdfs.DFSTestUtil;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Before;
import org.junit.Test;

public class FSDirectoryMPSRTest {
	private static final long SEED = 0;
	private static final short REPLICATION = 3;
	
	private final Path dir = new Path("/");
	private final Path sub1 = new Path(dir, "accMode=RAMP");
	private final Path sub2 = new Path(sub1, "fillNum=100");
	private final Path sub3 = new Path(sub2, "deviceType=BLM");
	private final Path file1 = new Path(sub3, "file1");
	private final Path file2 = new Path(sub3, "file2");
	
	private Configuration configuration;
	private MiniDFSCluster cluster;
	private FSDirectory fsDirectory;
	private FSNamesystem fsNamesystem;
	private DistributedFileSystem fileSystem;
	
	@Before
	public void setup() throws IOException {
		configuration = new Configuration();
		configuration.setInt(DFSConfigKeys.DFS_NAMENODE_MAX_XATTRS_PER_INODE_KEY, 2);
	    cluster = new MiniDFSCluster.Builder(configuration)
	      .numDataNodes(REPLICATION)
	      .build();
	    cluster.waitActive();
	    
	    fsNamesystem = cluster.getNamesystem();
	    fsDirectory = fsNamesystem.getFSDirectory();
	    
	    fileSystem = cluster.getFileSystem();
	    fileSystem.mkdirs(sub3);
	    DFSTestUtil.createFile(fileSystem, file1, 1024, REPLICATION, SEED);
	    DFSTestUtil.createFile(fileSystem, file2, 1024, REPLICATION, SEED);
	}
	
	@Test
	public void test() {
		fsNamesystem.printFS();
	}

}
