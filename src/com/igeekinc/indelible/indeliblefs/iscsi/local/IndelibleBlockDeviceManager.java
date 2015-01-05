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

import java.io.File;
import java.io.FilenameFilter;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.rmi.RemoteException;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSVolumeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleServerConnectionIF;
import com.igeekinc.indelible.indeliblefs.exceptions.ForkNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.ObjectNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.exceptions.VolumeNotFoundException;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSForkRemote;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFileNodeRemote;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.indelible.oid.ObjectID;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.util.FilePath;
import com.igeekinc.util.logging.ErrorLogMessage;

public class IndelibleBlockDeviceManager implements Runnable
{
	public static final String kStableDeviceFileSuffix = ".stable";
	File stableDeviceDirectory;
	long maxDeviceSpace;
	File logDirectory;
	long maxLogSpace;
	StableSegmentPool segmentPool;
	LogManager logManager;
	IndelibleServerConnectionIF serverConnection;
	
	HashMap<IndelibleFSObjectID, IndelibleBlockDevice>blockDevices = new HashMap<IndelibleFSObjectID, IndelibleBlockDevice>();
	Logger logger = Logger.getLogger(getClass());
	private FilePath stableDeviceDirectoryPath;
	
	private Thread commitThread;
	private boolean keepRunning = true, isRunning = false;
	
	public IndelibleBlockDeviceManager(File stableDeviceDirectory, long maxDeviceSpace, File logDirectory, long maxLogSpace, 
			IndelibleServerConnectionIF serverConnection) throws IOException 
	{
		this.stableDeviceDirectory = stableDeviceDirectory;
		this.maxDeviceSpace = maxDeviceSpace;
		this.logDirectory = logDirectory;
		this.maxLogSpace = maxLogSpace;
		this.serverConnection = serverConnection;
		
		if (!stableDeviceDirectory.exists())
		{
			throw new IllegalArgumentException(stableDeviceDirectory.getAbsolutePath()+" does not exist");
		}
		if (!stableDeviceDirectory.isDirectory())
		{
			throw new IllegalArgumentException(stableDeviceDirectory.getAbsolutePath()+" is not a directory");
		}
		
		if (!logDirectory.exists())
		{
			throw new IllegalArgumentException(logDirectory.getAbsolutePath()+" does not exist");
		}
		if (!logDirectory.isDirectory())
		{
			throw new IllegalArgumentException(logDirectory.getAbsolutePath()+" is not a directory");
		}
		logManager = new LogManager(logDirectory, maxLogSpace);
		segmentPool = new StableSegmentPool();
		initStableDevices();
		
		Thread commitThread = new Thread(this, "Block Device Mgr Commit Thread");
		commitThread.setDaemon(true);
		commitThread.start();
	}

	public void initStableDevices()
	{
		String [] stableDeviceNames = stableDeviceDirectory.list(new FilenameFilter() {
			
			@Override
			public boolean accept(File dir, String name) {
				if (name.endsWith(kStableDeviceFileSuffix))
					return true;
				return false;
			}
		});
		stableDeviceDirectoryPath = FilePath.getFilePath(stableDeviceDirectory);
		for (String curStableName:stableDeviceNames)
		{
			String parseStr = curStableName.substring(0, curStableName.length() - kStableDeviceFileSuffix.length());
			if (parseStr.indexOf('-') > 0)
			{
				String volumeIDStr = parseStr.substring(0, parseStr.indexOf('-'));
				String fileID = parseStr.substring(parseStr.indexOf('-') + 1);
				try
				{
					ObjectID volumeID = ObjectIDFactory.reconstituteFromString(volumeIDStr);
					if (volumeID instanceof IndelibleFSObjectID)
					{
						IndelibleFSVolumeIF indelibleVolume = serverConnection.retrieveVolume((IndelibleFSObjectID) volumeID);
						ObjectID id = ObjectIDFactory.reconstituteFromString(fileID);
						if (id instanceof IndelibleFSObjectID)
						{
							IndelibleFSObjectID objectID = (IndelibleFSObjectID)id;
							try 
							{
								IndelibleFileNodeIF backingObject = indelibleVolume.getObjectByID(objectID);
								FilePath stablePath = stableDeviceDirectoryPath.getChild(curStableName);
								StableDevice curStableDevice = new StableDevice(stablePath, backingObject, segmentPool);
								IndelibleBlockDevice curDevice = new IndelibleBlockDevice(curStableDevice, logManager, segmentPool, this);
								blockDevices.put(objectID, curDevice);
							} catch (Exception e)
							{
								logger.error("Got exception loading stable file "+curStableName, e);
							}
						}
						else
						{
							logger.error(fileID+" is an invalid object ID, skipping");
						}
					}
				}
				catch(IllegalArgumentException e)
				{

					logger.error(volumeIDStr+" is an invalid object ID, skipping");
				} catch (VolumeNotFoundException e) 
				{
					logger.error("Could not retrieve volume "+volumeIDStr);
				} catch (IOException e) {
					logger.error("Got remote exception", e);
				}
			}
			else
			{
				logger.error("Bad file name "+curStableName+", skipping");
			}
		}
	}
	
	public synchronized IndelibleBlockDevice getBlockDevice(IndelibleFSVolumeIF indelibleVolume, FilePath deviceImagePath) 
			throws ObjectNotFoundException, PermissionDeniedException, RemoteException, IOException, ForkNotFoundException
	{
		IndelibleFileNodeIF backingObject = indelibleVolume.getObjectByPath(deviceImagePath);
		IndelibleFSObjectID objectID = (IndelibleFSObjectID) backingObject.getObjectID();
		IndelibleBlockDevice returnDevice = blockDevices.get(objectID);
		if (returnDevice == null)
		{
			// Doesn't already exist, set it up
			ObjectID volumeID = indelibleVolume.getObjectID();
			String stableName = getStableFileName(objectID, volumeID);
			FilePath stablePath = stableDeviceDirectoryPath.getChild(stableName);
			File checkFile = new File(stablePath.toString());
			StableDevice stableDevice;
			if (checkFile.exists())
				stableDevice = new StableDevice(stablePath, backingObject, segmentPool);
			else
				stableDevice = new StableDevice(stablePath, backingObject, 1024*1024, segmentPool);
			returnDevice = new IndelibleBlockDevice(stableDevice, logManager, segmentPool, this);
			//logManager.setUpdatesForBlockDevice(returnDevice);
			blockDevices.put(objectID, returnDevice);
		}
		return returnDevice;
	}

	private String getStableFileName(IndelibleFSObjectID objectID,
			ObjectID volumeID) {
		return volumeID.toString()+"-"+objectID.toString()+kStableDeviceFileSuffix;
	}
	
	public long snapshotDevice(IndelibleBlockDevice deviceToSnapshot) throws RemoteException, IOException, PermissionDeniedException
	{
		long highestWriteID = -1L;
		try 
		{
			IndelibleBlockDevice snapshot = deviceToSnapshot.clone();
			StableDevice snapshotStable = snapshot.getStableDevice();
			serverConnection.startTransaction();
			boolean committed = false;
			try
			{
				IndelibleFileNodeIF snapshotFile = snapshotStable.backingObject;
				try {
					IndelibleFSForkIF imageFork = snapshotFile.getFork("data", false);
					Segment [] segments = snapshot.getSegments();
					for (int curSegmentNum = 0; curSegmentNum < segments.length; curSegmentNum++)
					{
						Segment curSegment = segments[curSegmentNum];
						LogBlockIdentifier [] updates = curSegment.getUpdates();
						boolean hasUpdates = false;
						for (int updateNum = 0; updateNum < updates.length; updateNum++)
						{
							if (updates[updateNum] != null)
							{
								if (updates[updateNum].getWriteID() > highestWriteID)
									highestWriteID = updates[updateNum].getWriteID();
								hasUpdates = true;
							}
						}
						if (hasUpdates)
						{
							byte [] segmentData = new byte[curSegment.getLength()];
							curSegment.read(0, ByteBuffer.wrap(segmentData), true);
							long segmentOffset = curSegment.getOffset();
							imageFork.writeDataDescriptor(segmentOffset, new CASIDMemoryDataDescriptor(segmentData));
							//System.out.println("Updating segment at offset "+segmentOffset);
						}
					}
				} catch (ForkNotFoundException e) {
					throw new InternalError("No data fork for "+deviceToSnapshot.getStableDevice().getStablePath());
				} catch (PermissionDeniedException e)
				{
					throw new InternalError("No permission denied for "+deviceToSnapshot.getStableDevice().getStablePath());
				}
				serverConnection.commit();
				committed = true;
			}
			finally
			{
				if (!committed)
				{
					serverConnection.rollback();
					highestWriteID = -1L;
				}
			}
			if (highestWriteID > -1L)
			{
				//System.out.println("Finished updating to "+highestWriteID);
				deviceToSnapshot.updateToBackingFile();
				deviceToSnapshot.discardUpdatesBefore(highestWriteID);
				deviceToSnapshot.setLastSnapshotWriteID(highestWriteID);
				deviceToSnapshot.setLastSnapshotTime(new Date());
			}
		} catch (CloneNotSupportedException e) {
			throw new InternalError("Got CloneNotSupportedException");
		}
		return highestWriteID;
	}

	protected void checkConsistency(IndelibleBlockDevice checkDevice)
			throws IOException, RemoteException
	{
		logger.warn("Starting consistency check on "+checkDevice.stableDevice.getVolumeID()+"/"+checkDevice.stableDevice.getStablePath());
		IndelibleBlockDevice snapshot;
		try
		{
			snapshot = checkDevice.clone();
		} catch (CloneNotSupportedException e1)
		{
			// TODO Auto-generated catch block
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e1);
			return;
		}
		long highestWriteID = snapshot.getLastSnapshotWriteID();
		StableDevice snapshotStable = snapshot.getStableDevice();
		IndelibleFileNodeIF snapshotFile = snapshotStable.backingObject;
		// No updates, let's compare!
		try
		{
			IndelibleFSForkIF imageFork = snapshotFile.getFork("data", false);
			Segment [] segments = snapshot.getSegments();
			for (int curSegmentNum = 0; curSegmentNum < segments.length; curSegmentNum++)
			{
				Segment curSegment = segments[curSegmentNum];
				LogBlockIdentifier [] updates = curSegment.getUpdates();
				boolean hasUpdates = false;
				byte [] ourSegmentData = new byte[curSegment.getLength()];
				curSegment.read(0, ByteBuffer.wrap(ourSegmentData), false);
				for (int updateNum = 0; updateNum < updates.length; updateNum++)
				{
					if (updates[updateNum] != null)
					{
						if (updates[updateNum].getWriteID() > highestWriteID)
						{
							if (highestWriteID > updates[updateNum].getWriteID())
								logger.error("Missed segment update at "+curSegmentNum);
						}
						hasUpdates = true;
					}
				}
				if (!hasUpdates)
				{
					long segmentOffset = curSegment.getOffset();
					CASIDDataDescriptor fsSegmentDataDescriptor = imageFork.getDataDescriptor(segmentOffset, curSegment.getLength());
					byte [] fsSegmentData = fsSegmentDataDescriptor.getData();
					if (!Arrays.equals(ourSegmentData, fsSegmentData))
					{
						logger.error("Mismatched data at "+curSegmentNum);
						// Should we push our data?
					}
				}
				else
				{
					logger.error("Updates occurred at "+curSegmentNum+" skipping check");
				}
			}
			checkDevice.setLastSnapshotTime(new Date());
		}
		catch(ForkNotFoundException e)
		{
			
		} catch (PermissionDeniedException e)
		{
			// TODO Auto-generated catch block
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		}
		logger.warn("Finished consistency check on "+checkDevice.stableDevice.getVolumeID()+"/"+checkDevice.stableDevice.getStablePath());

	}
	
	public synchronized void deviceUpdated()
	{
		this.notifyAll();
	}
	
	public void run()
	{
		isRunning = true;
		try
		{
			while(keepRunning)
			{
				try
				{
					synchronized(this)
					{
						this.wait(60000);	// Wake up once a minute and scan no matter what
					}
					for (IndelibleBlockDevice curDevice:blockDevices.values().toArray(new IndelibleBlockDevice[0]))
					{
						long lastWriteID = snapshotDevice(curDevice);
						if (lastWriteID < 0)	// No updates
						{
							if (System.currentTimeMillis() - curDevice.getLastConsistencyCheckTime().getTime() > 10000)
							{
								checkConsistency(curDevice);
							}
						}
					}
				}
				catch (Throwable t)
				{
					logger.error(new ErrorLogMessage("Caught unexpected exception"), t);
				}
			}
		}
		finally
		{
			isRunning = false;
		}
	}
	
	public void shutdown()
	{
		keepRunning = false;
		while(isRunning)
		{
			try
			{
				Thread.sleep(100);
			}
			catch (InterruptedException e)
			{
				
			}
		}
	}
}
