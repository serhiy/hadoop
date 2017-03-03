/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hdfs.protocol;

import java.net.URI;

import org.apache.hadoop.classification.InterfaceAudience;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.FileEncryptionInfo;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;

/** Interface that represents the over the wire information for a file.
 */
@InterfaceAudience.Private
@InterfaceStability.Evolving
public class HdfsFileStatus {

  private final byte[][] path;  // local name of the inode that's encoded in java UTF8
  private final byte[][] symlink; // symlink target encoded in java UTF8 or null
  private final long []length;
  private final boolean []isdir;
  private final short []block_replication;
  private final long []blocksize;
  private final long []modification_time;
  private final long []access_time;
  private final FsPermission []permission;
  private final String []owner;
  private final String []group;
  private final long []fileId;

  private final FileEncryptionInfo []feInfo;
  
  // Used by dir, not including dot and dotdot. Always zero for a regular file.
  private final int []childrenNum;
  private final byte []storagePolicy;
  
  public static final byte[] EMPTY_NAME = new byte[0];
  
  public HdfsFileStatus(int numPartitions) {
	  this.length = new long[numPartitions];
	    this.isdir = new boolean [numPartitions];
	    this.block_replication = new short[numPartitions];
	    this.blocksize = new long[numPartitions];
	    this.modification_time = new long[numPartitions];
	    this.access_time = new long[numPartitions];
	    this.permission = new FsPermission [numPartitions];
	    this.owner = new String[numPartitions];
	    this.group = new String[numPartitions];
	    this.symlink = new byte[numPartitions][];
	    this.path = new byte[numPartitions][];
	    this.fileId = new long[numPartitions];
	    this.childrenNum = new int[numPartitions];
	    this.feInfo = new FileEncryptionInfo[numPartitions];
	    this.storagePolicy = new byte[numPartitions];
  }

  /**
   * Constructor
   * @param length the number of bytes the file has
   * @param isdir if the path is a directory
   * @param block_replication the replication factor
   * @param blocksize the block size
   * @param modification_time modification time
   * @param access_time access time
   * @param permission permission
   * @param owner the owner of the path
   * @param group the group of the path
   * @param path the local name in java UTF8 encoding the same as that in-memory
   * @param fileId the file id
   * @param feInfo the file's encryption info
   */
  public HdfsFileStatus(long length, boolean isdir, int block_replication,
      long blocksize, long modification_time, long access_time,
      FsPermission permission, String owner, String group, byte[] symlink,
      byte[] path, long fileId, int childrenNum, FileEncryptionInfo feInfo,
      byte storagePolicy) {
    this.length = new long[] { length };
    this.isdir = new boolean [] { isdir };
    this.block_replication = new short[] { (short)block_replication };
    this.blocksize = new long[] { blocksize };
    this.modification_time = new long[] { modification_time };
    this.access_time = new long[] { access_time };
    this.permission = new FsPermission [] { (permission == null) ? 
        ((isdir || symlink!=null) ? 
            FsPermission.getDefault() : 
            FsPermission.getFileDefault()) :
        permission } ;
    this.owner = new String[] { (owner == null) ? "" : owner };
    this.group = new String[] { (group == null) ? "" : group };
    this.symlink = new byte[][] { symlink };
    this.path = new byte[][] { path };
    this.fileId = new long[] { fileId };
    this.childrenNum = new int[] { childrenNum };
    this.feInfo = new FileEncryptionInfo[] { feInfo };
    this.storagePolicy = new byte[] { storagePolicy };
  }

  /**
   * Get the length of this file, in bytes.
   * @return the length of this file, in bytes.
   */
  public final long getLen() {
    return length[0];
  }

  /**
   * Is this a directory?
   * @return true if this is a directory
   */
  public final boolean isDir() {
    return isdir[0];
  }

  /**
   * Is this a symbolic link?
   * @return true if this is a symbolic link
   */
  public boolean isSymlink() {
    return symlink[0] != null;
  }
  
  /**
   * Get the block size of the file.
   * @return the number of bytes
   */
  public final long getBlockSize() {
    return blocksize[0];
  }

  /**
   * Get the replication factor of a file.
   * @return the replication factor of a file.
   */
  public final short getReplication() {
    return block_replication[0];
  }

  /**
   * Get the modification time of the file.
   * @return the modification time of file in milliseconds since January 1, 1970 UTC.
   */
  public final long getModificationTime() {
    return modification_time[0];
  }

  /**
   * Get the access time of the file.
   * @return the access time of file in milliseconds since January 1, 1970 UTC.
   */
  public final long getAccessTime() {
    return access_time[0];
  }

  /**
   * Get FsPermission associated with the file.
   * @return permssion
   */
  public final FsPermission getPermission() {
    return permission[0];
  }
  
  /**
   * Get the owner of the file.
   * @return owner of the file
   */
  public final String getOwner() {
    return owner[0];
  }
  
  /**
   * Get the group associated with the file.
   * @return group for the file. 
   */
  public final String getGroup() {
    return group[0];
  }
  
  /**
   * Check if the local name is empty
   * @return true if the name is empty
   */
  public final boolean isEmptyLocalName() {
    return path[0].length == 0;
  }

  /**
   * Get the string representation of the local name
   * @return the local name in string
   */
  public final String getLocalName() {
    return DFSUtil.bytes2String(path[0]);
  }
  
  /**
   * Get the Java UTF8 representation of the local name
   * @return the local name in java UTF8
   */
  public final byte[] getLocalNameInBytes() {
    return path[0];
  }

  /**
   * Get the string representation of the full path name
   * @param parent the parent path
   * @return the full path in string
   */
  public final String getFullName(final String parent) {
    if (isEmptyLocalName()) {
      return parent;
    }
    
    StringBuilder fullName = new StringBuilder(parent);
    if (!parent.endsWith(Path.SEPARATOR)) {
      fullName.append(Path.SEPARATOR);
    }
    fullName.append(getLocalName());
    return fullName.toString();
  }

  /**
   * Get the full path
   * @param parent the parent path
   * @return the full path
   */
  public final Path getFullPath(final Path parent) {
    if (isEmptyLocalName()) {
      return parent;
    }
    
    return new Path(parent, getLocalName());
  }

  /**
   * Get the string representation of the symlink.
   * @return the symlink as a string.
   */
  public final String getSymlink() {
    return DFSUtil.bytes2String(symlink[0]);
  }
  
  public final byte[] getSymlinkInBytes() {
    return symlink[0];
  }
  
  public final long getFileId() {
    return fileId[0];
  }
  
  public final FileEncryptionInfo getFileEncryptionInfo() {
    return feInfo[0];
  }

  public final int getChildrenNum() {
    return childrenNum[0];
  }

  /** @return the storage policy id */
  public final byte getStoragePolicy() {
    return storagePolicy[0];
  }
  
  
  
  
  
  
  /**
   * Get the length of this file, in bytes.
   * @return the length of this file, in bytes.
   */
  public final long getLen(int partitioning) {
    return length[partitioning];
  }

  /**
   * Is this a directory?
   * @return true if this is a directory
   */
  public final boolean isDir(int partitioning) {
    return isdir[partitioning];
  }

  /**
   * Is this a symbolic link?
   * @return true if this is a symbolic link
   */
  public boolean isSymlink(int partitioning) {
    return symlink[partitioning] != null;
  }
  
  /**
   * Get the block size of the file.
   * @return the number of bytes
   */
  public final long getBlockSize(int partitioning) {
    return blocksize[partitioning];
  }

  /**
   * Get the replication factor of a file.
   * @return the replication factor of a file.
   */
  public final short getReplication(int partitioning) {
    return block_replication[partitioning];
  }

  /**
   * Get the modification time of the file.
   * @return the modification time of file in milliseconds since January 1, 1970 UTC.
   */
  public final long getModificationTime(int partitioning) {
    return modification_time[partitioning];
  }

  /**
   * Get the access time of the file.
   * @return the access time of file in milliseconds since January 1, 1970 UTC.
   */
  public final long getAccessTime(int partitioning) {
    return access_time[partitioning];
  }

  /**
   * Get FsPermission associated with the file.
   * @return permssion
   */
  public final FsPermission getPermission(int partitioning) {
    return permission[partitioning];
  }
  
  /**
   * Get the owner of the file.
   * @return owner of the file
   */
  public final String getOwner(int partitioning) {
    return owner[partitioning];
  }
  
  /**
   * Get the group associated with the file.
   * @return group for the file. 
   */
  public final String getGroup(int partitioning) {
    return group[partitioning];
  }
  
  /**
   * Check if the local name is empty
   * @return true if the name is empty
   */
  public final boolean isEmptyLocalName(int partitioning) {
    return path[partitioning].length == 0;
  }

  /**
   * Get the string representation of the local name
   * @return the local name in string
   */
  public final String getLocalName(int partitioning) {
    return DFSUtil.bytes2String(path[partitioning]);
  }
  
  /**
   * Get the Java UTF8 representation of the local name
   * @return the local name in java UTF8
   */
  public final byte[] getLocalNameInBytes(int partitioning) {
    return path[partitioning];
  }

  /**
   * Get the string representation of the symlink.
   * @return the symlink as a string.
   */
  public final String getSymlink(int partitioning) {
    return DFSUtil.bytes2String(symlink[partitioning]);
  }
  
  public final byte[] getSymlinkInBytes(int partitioning) {
    return symlink[partitioning];
  }
  
  public final long getFileId(int partitioning) {
    return fileId[partitioning];
  }
  
  public final FileEncryptionInfo getFileEncryptionInfo(int partitioning) {
    return feInfo[partitioning];
  }

  public final int getChildrenNum(int partitioning) {
    return childrenNum[partitioning];
  }

  /** @return the storage policy id */
  public final byte getStoragePolicy(int partitioning) {
    return storagePolicy[partitioning];
  }
  
  
  

  public final FileStatus makeQualified(URI defaultUri, Path path) {
    return new FileStatus(getLen(), isDir(), getReplication(),
        getBlockSize(), getModificationTime(),
        getAccessTime(),
        getPermission(), getOwner(), getGroup(),
        isSymlink() ? new Path(getSymlink()) : null,
        (getFullPath(path)).makeQualified(
            defaultUri, null)); // fully-qualify path
  }
  
  public void addFileStatus(long length, boolean isdir, int block_replication,
	      long blocksize, long modification_time, long access_time,
	      FsPermission permission, String owner, String group, byte[] symlink,
	      byte[] path, long fileId, int childrenNum, FileEncryptionInfo feInfo,
	      byte storagePolicy, int partitioning) {
	    this.length[partitioning] = length;
	    this.isdir[partitioning] = isdir;
	    this.block_replication[partitioning] = (short)block_replication;
	    this.blocksize[partitioning] = blocksize;
	    this.modification_time[partitioning] = modification_time;
	    this.access_time[partitioning] = access_time;
	    this.permission[partitioning] = (permission == null) ?  ((isdir || symlink!=null) ? FsPermission.getDefault() : FsPermission.getFileDefault()) : permission;
	    this.owner[partitioning] = (owner == null) ? "" : owner;
	    this.group[partitioning] = (group == null) ? "" : group;
	    this.symlink[partitioning] = symlink;
	    this.path[partitioning] = path;
	    this.fileId[partitioning] = fileId;
	    this.childrenNum[partitioning] = childrenNum;
	    this.feInfo[partitioning] = feInfo;
	    this.storagePolicy[partitioning] = storagePolicy;
  }
}
