/*
 * Copyright 2002-2014 iGeek, Inc.
 * All Rights Reserved
 * @Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.@
 */
 
package com.igeekinc.indelible.indeliblefs.iscsi.local;

public class LogBlockIdentifier
{
	private LogManager parent;
	private int logNum;			// Which log this block belongs to
	private long blockNum;		// The block number within the log
	private int referenceCount;	// The number of references to this block
	private long writeID;		// The id of the write
	
	protected LogBlockIdentifier(LogManager parent, int logNum, long blockNum, long writeID)
	{
		this.parent = parent;
		this.logNum = logNum;
		this.blockNum = blockNum;
		this.writeID = writeID;
		referenceCount = 0;
	}

	public int getLogNum()
	{
		return logNum;
	}

	public long getBlockNum()
	{
		return blockNum;
	}
	
	public long getWriteID()
	{
		return writeID;
	}
	
	public String toString()
	{
		return "logNum = "+logNum+" blockNum = "+blockNum;
	}
	
	public int getReferenceCount()
	{
		return referenceCount;
	}
	
	public void incrementReferenceCount()
	{
		referenceCount++;
	}
	
	public void decrementReferenceCount()
	{
		referenceCount--;
		if (referenceCount <= 0)
			parent.releaseBlock(this);
	}
}
