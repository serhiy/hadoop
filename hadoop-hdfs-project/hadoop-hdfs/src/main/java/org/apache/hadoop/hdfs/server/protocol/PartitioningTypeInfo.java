package org.apache.hadoop.hdfs.server.protocol;

// serhiy
public class PartitioningTypeInfo {
	int pType;
	
	public PartitioningTypeInfo() {
		pType = -1;
	}
	
	public PartitioningTypeInfo(int pType) {
		this.pType = pType;
	}

	public int getpType() {
		return pType;
	}

	public void setpType(int pType) {
		this.pType = pType;
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + pType;
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		PartitioningTypeInfo other = (PartitioningTypeInfo) obj;
		if (pType != other.pType)
			return false;
		return true;
	}
}
