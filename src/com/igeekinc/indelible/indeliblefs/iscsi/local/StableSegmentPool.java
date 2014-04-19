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
import java.util.ArrayList;
import java.util.HashMap;

import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;


class SegmentInfo
{
	private StableDevice	device;
	private int				segmentNum;
	public SegmentInfo(StableDevice device, int segmentNum)
	{
		this.device = device;
		this.segmentNum = segmentNum;
	}
	public StableDevice getDevice()
	{
		return device;
	}
	public int getSegmentNum()
	{
		return segmentNum;
	}
}
public class StableSegmentPool
{
	private HashMap<CASIdentifier, ArrayList<SegmentInfo>> segmentPool = new HashMap<CASIdentifier, ArrayList<SegmentInfo>>();
	
	public StableSegmentPool()
	{
		
	}
	
	public void registerSegment(CASIdentifier segmentID, StableDevice device, int segmentNum)
	{
		synchronized(segmentPool)
		{
			SegmentInfo insertInfo = new SegmentInfo(device, segmentNum);
			ArrayList<SegmentInfo>infoList = segmentPool.get(segmentID);
			if (infoList == null)
				infoList = new ArrayList<SegmentInfo>();
			if (!infoList.contains(insertInfo))
				infoList.add(insertInfo);
			segmentPool.put(segmentID, infoList);
		}
	}
	
	public void unregisterSegment(CASIdentifier segmentID, StableDevice device, int segmentNum)
	{
		ArrayList<SegmentInfo> retrieveInfo;
		synchronized(segmentPool)
		{
			retrieveInfo = segmentPool.get(segmentID);
			for (SegmentInfo curInfo:retrieveInfo)
			{
				if (curInfo.getDevice().equals(device) && curInfo.getSegmentNum() == segmentNum)
					retrieveInfo.remove(curInfo);
			}
			if (retrieveInfo.size() == 0)
				segmentPool.remove(segmentID);
		}
	}
	
	public boolean getSegmentData(CASIdentifier segmentID, ByteBuffer buffer) throws IOException
	{
		SegmentInfo retrieveInfo = null;
		synchronized(segmentPool)
		{
			ArrayList<SegmentInfo> retrieveInfoList = segmentPool.get(segmentID);
			if (retrieveInfoList.size() > 0)
			{
				retrieveInfo = retrieveInfoList.get(0);
				retrieveInfo.getDevice().readLockSegment(retrieveInfo.getSegmentNum());
			}
			else
			{
				return false;
			}
		}
		try
		{
			retrieveInfo.getDevice().read(buffer, retrieveInfo.getSegmentNum());
			return true;
		}
		finally
		{
			retrieveInfo.getDevice().releaseSegment(retrieveInfo.getSegmentNum());
		}
	}
	
	public boolean containsSegment(CASIdentifier segmentID)
	{
		synchronized(segmentPool)
		{
			return segmentPool.containsKey(segmentID);
		}
	}
}
