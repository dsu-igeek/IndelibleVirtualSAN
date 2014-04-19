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

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Date;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.exceptions.ForkNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.iscsi.IndelibleFSStorageModule.StorageState;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.util.logging.ErrorLogMessage;

public class IndelibleBlockDevice implements Cloneable
{
	StableDevice stableDevice;
	Segment [] segments;
	long size;							// Size in bytes
	long writeID;						// ID of last write to this device
	int blockSize, segmentSize;
	LogManager logManager;
	IndelibleBlockDeviceManager manager;
	long lastSnapshotWriteID = -1L;
	Date lastSnapshotTime = new Date();
	Date lastConsistencyCheckTime = new Date();
	StorageState state;
	
	public IndelibleBlockDevice(StableDevice stableDevice, LogManager logManager, StableSegmentPool segmentPool, IndelibleBlockDeviceManager manager)
			throws IOException, ForkNotFoundException
	{
		this.state = StorageState.kStable;
		this.logManager = logManager;		
		this.stableDevice = stableDevice;
		blockSize = 4096;
		segmentSize = stableDevice.getSegmentSize();
		segments = new Segment[(int) (stableDevice.getSize()/segmentSize)];
		for (int curSegmentNum = 0; curSegmentNum < segments.length; curSegmentNum++)
		{
			segments[curSegmentNum] = new Segment((long)curSegmentNum * (long)segmentSize, segmentSize, stableDevice.identifierForSegment(curSegmentNum), logManager, stableDevice);
		}
		size = stableDevice.getSize();
		this.manager = manager;
	}

	public synchronized void read(final long offset, final ByteBuffer readBuffer) throws IOException
	{
		if (offset > size)
			throw new IllegalArgumentException(offset+" > "+size);
		int bytesToRead = readBuffer.limit();
		if (offset + bytesToRead > size)
			bytesToRead = (int)(size - offset);
		long deviceOffset = offset;
		int bufferOffset = 0;
		int curSegmentNum;
		
		if (deviceOffset % segmentSize != 0)
		{
			curSegmentNum = (int)(deviceOffset / segmentSize);
			int segmentOffset = (int) (deviceOffset % segmentSize);
			int curBytesToRead = segmentSize - segmentOffset;
			if (curBytesToRead > bytesToRead)
				curBytesToRead = bytesToRead;
			Segment curSegment = segments[curSegmentNum];
			ByteBuffer curSegmentBuffer = readBuffer.slice();
			curSegmentBuffer.limit(curBytesToRead);
			curSegment.read(segmentOffset, curSegmentBuffer, false);
			readBuffer.position(curBytesToRead);
			deviceOffset += curBytesToRead;
			bytesToRead -= curBytesToRead;
			bufferOffset += bytesToRead;
		}
		while (bytesToRead > 0)
		{
			curSegmentNum = (int)(deviceOffset / segmentSize);
			// Assume that we are now segment aligned so no need for a starting offset into the segment
			int curBytesToRead = segmentSize;
			if (curBytesToRead > bytesToRead)
				curBytesToRead = bytesToRead;
			Segment curSegment = segments[curSegmentNum];
			ByteBuffer curSegmentBuffer = readBuffer.slice();
			curSegmentBuffer.limit(curBytesToRead);
			curSegment.read(0, curSegmentBuffer, false);
			readBuffer.position(readBuffer.position() + curBytesToRead);
			deviceOffset += curBytesToRead;
			bytesToRead -= curBytesToRead;
			bufferOffset += bytesToRead;
			
		}
	}
	
	public void write(final long offset, final ByteBuffer writeBuffer) throws IOException
	{
		synchronized(this)
		{
			if (offset > size)
				throw new IllegalArgumentException(offset+" > "+size);
			int bytesToWrite = writeBuffer.limit();
			if (offset + bytesToWrite > size)
				bytesToWrite = (int)(size - offset);
			long deviceOffset = offset;
			int bufferOffset = 0;

			int curSegmentNum;

			//System.out.println("Writing "+bytesToWrite+" at "+offset);
			if (deviceOffset % segmentSize != 0)
			{
				curSegmentNum = (int)(deviceOffset / segmentSize);
				int segmentOffset = (int) (deviceOffset % segmentSize);
				int curBytesToWrite = segmentSize - segmentOffset;
				if (curBytesToWrite > bytesToWrite)
					curBytesToWrite = bytesToWrite;
				Segment curSegment = segments[curSegmentNum];
				ByteBuffer curSegmentBuffer = writeBuffer.duplicate();
				curSegmentBuffer.limit(curBytesToWrite);
				curSegment.write(segmentOffset, curSegmentBuffer);
				deviceOffset += curBytesToWrite;
				bytesToWrite -= curBytesToWrite;
				bufferOffset += curBytesToWrite;
			}
			while (bytesToWrite > 0)
			{
				curSegmentNum = (int)(deviceOffset / segmentSize);
				// Assume that we are now segment aligned so no need for a starting offset into the segment
				int curBytesToWrite = segmentSize;
				if (curBytesToWrite > bytesToWrite)
					curBytesToWrite = bytesToWrite;
				Segment curSegment = segments[curSegmentNum];
				ByteBuffer curSegmentBuffer = writeBuffer.duplicate();
				curSegmentBuffer.limit(curBytesToWrite + bufferOffset);
				curSegmentBuffer.position(bufferOffset);
				curSegment.write(0, curSegmentBuffer);
				deviceOffset += curBytesToWrite;
				bytesToWrite -= curBytesToWrite;
				bufferOffset += bytesToWrite;
			}
		}
		state = StorageState.kWriteBack;
		manager.deviceUpdated();
	}

	public long getSize() throws IOException 
	{
		return stableDevice.getSize();
	}

	public IndelibleFSObjectID getVolumeID() 
	{
		return stableDevice.getVolumeID();
	}
	
	public StableDevice getStableDevice()
	{
		return stableDevice;
	}

	@Override
	protected synchronized IndelibleBlockDevice clone() throws CloneNotSupportedException 
	{
		IndelibleBlockDevice returnClone = (IndelibleBlockDevice) super.clone();
		returnClone.segments = new Segment[segments.length];
		for (int curSegmentNum = 0; curSegmentNum < segments.length; curSegmentNum++)
		{
			returnClone.segments[curSegmentNum] = (Segment) segments[curSegmentNum].clone();
		}
		return returnClone;
	}

	/**
	 * Returns a cloned snapshot of the segments
	 * @return
	 */
	public synchronized Segment[] getSegments() 
	{
		Segment [] returnSegments = new Segment[segments.length];
		for (int curSegmentNum = 0; curSegmentNum < segments.length; curSegmentNum++)
		{
			try
			{
				returnSegments[curSegmentNum] = segments[curSegmentNum].clone();
			} catch (CloneNotSupportedException e)
			{
				// TODO Auto-generated catch block
				Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
			}
		}
		return segments;
	}

	public synchronized void discardUpdatesBefore(long highestWriteID) 
	{
		for (int curSegmentNum = 0; curSegmentNum < segments.length; curSegmentNum++)
		{
			Segment curSegment = segments[curSegmentNum];
			curSegment.discardUpdatesBefore(highestWriteID);
		}
	}

	public long getLastSnapshotWriteID()
	{
		return lastSnapshotWriteID;
	}

	public void setLastSnapshotWriteID(long lastSnapshotWriteID)
	{
		this.lastSnapshotWriteID = lastSnapshotWriteID;
	}

	public Date getLastSnapshotTime()
	{
		return lastSnapshotTime;
	}

	public void setLastSnapshotTime(Date lastSnapshotTime)
	{
		this.lastSnapshotTime = lastSnapshotTime;
	}

	public Date getLastConsistencyCheckTime()
	{
		return lastConsistencyCheckTime;
	}

	public void setLastConsistencyCheckTime(Date lastConsistencyCheckTime)
	{
		this.lastConsistencyCheckTime = lastConsistencyCheckTime;
	}

	public StorageState getState()
	{
		return state;
	}

	public synchronized void updateToBackingFile() throws IOException, PermissionDeniedException
	{
		stableDevice.updateToBackingFile();
		state = StorageState.kStable;
	}
}
