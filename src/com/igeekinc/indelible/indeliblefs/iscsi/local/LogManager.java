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
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Set;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.util.logging.ErrorLogMessage;

public class LogManager
{
	private static final String kISCSILogFileSuffix = ".iscsilog";
	private File logDirectory;
	private HashMap<Integer, LogFile> logFiles;
	private LogFile appendLog;
	private long maxBytes;
	private int appendLogNum;
	private long writeID;
	private int kBlocksPerLogFile = 1024*1024;
	private Logger logger = Logger.getLogger(getClass());
	
	public LogManager(File logDirectory, long maxBytes) throws IOException
	{
		this.logDirectory = logDirectory;
		this.maxBytes = maxBytes;
		logFiles = new HashMap<Integer, LogFile>();
		String [] logFileNames = logDirectory.list(new FilenameFilter() {
			
			@Override
			public boolean accept(File arg0, String arg1) {
				if (arg1.endsWith(kISCSILogFileSuffix))
					return true;
				return false;
			}
		});
		for (String curLogFileName:logFileNames)
		{
			File curLogFile = new File(logDirectory, curLogFileName);
			try {
				LogFile logFile = new LogFile(curLogFile);
				int logFileNum = Integer.parseInt(curLogFileName.substring(0, curLogFileName.length() - kISCSILogFileSuffix.length()));
				logFiles.put(logFileNum, logFile);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		if (logFiles.size() > 0)
		{
			Integer [] logFileNums = logFiles.keySet().toArray(new Integer[logFiles.size()]);

			Arrays.sort(logFileNums);
			appendLogNum = logFileNums[logFileNums.length - 1];
			appendLog = logFiles.get(appendLogNum);
		}
		else
		{
			appendLogNum = 0;
			addLogFile();
		}
	}
	
	synchronized void addLogFile() throws IOException
	{
		appendLogNum++;
		String logFileName = appendLogNum + kISCSILogFileSuffix;
		File newLogFile = new File(logDirectory, logFileName);
		appendLog = new LogFile(newLogFile);
		logFiles.put(appendLogNum, appendLog);
	}
	
	public synchronized LogBlockIdentifier appendBlock(ByteBuffer blockToAppend, IndelibleFSObjectID segmentFileID, long segmentFileOffset) throws IOException
	{
		if (appendLog.getNumBlocks() > kBlocksPerLogFile)
			addLogFile();
		long appendOffset = appendLog.appendBlock(blockToAppend, segmentFileID, segmentFileOffset);
		LogBlockIdentifier returnBlock = new LogBlockIdentifier(this, appendLogNum, appendOffset, writeID++);
		returnBlock.incrementReferenceCount();
		return returnBlock;
	}
	
	public int getBlockSize()
	{
		return LogFile.kBlockSize;
	}

	public void read(LogBlockIdentifier logBlockIdentifier, ByteBuffer updateBuffer) throws IOException
	{
		LogFile readLog = logFiles.get(logBlockIdentifier.getLogNum());
		if (readLog == null)
			throw new IOException("Logfile "+logBlockIdentifier.getLogNum()+" not found");
		readLog.read(logBlockIdentifier.getBlockNum(), updateBuffer);
	}

	public void releaseBlock(LogBlockIdentifier blockToRelease)
	{
		LogFile releaseLog;
		
		synchronized(this)
		{
			releaseLog= logFiles.get(blockToRelease.getLogNum());
		}
		if (releaseLog != null)
		{
			if (releaseLog.release(blockToRelease.getBlockNum()))
			{
				releaseLogFile(blockToRelease.getLogNum());
			}
		}
		else
		{
			logger.error(new ErrorLogMessage("Could not find logfile {0} to release", new Serializable [] {blockToRelease.getBlockNum()}));
		}
	}
	
	protected void releaseLogFile(int logFileNum)
	{
		LogFile releaseLog;
		synchronized(this)
		{
			releaseLog = logFiles.get(logFileNum);
			if (releaseLog.getNumBlocksInUse() != 0)
			{
				logger.error(new ErrorLogMessage("Trying to release log file that has in-use blocks"));
				return;
			}
			logFiles.remove(logFileNum);
			if (appendLog == releaseLog)
			{
				try
				{
					addLogFile();
				} catch (IOException e)
				{
					logger.fatal(new ErrorLogMessage("Could not create new log file"));
					throw new InternalError("Could not create new log file");
				}	// Should always have a log file to write to
			}
		}
		releaseLog.remove();
	}
	
	public synchronized void setUpdatesForBlockDevice(IndelibleBlockDevice updateDevice) throws IOException
	{
		IndelibleFSObjectID backingID = updateDevice.getStableDevice().getBackingObjectID();
		Set<Integer>logFileNumKeySet = logFiles.keySet();
		Integer [] logFileNums = new Integer[logFileNumKeySet.size()];
		logFileNums = logFileNumKeySet.toArray(logFileNums);
		Arrays.sort(logFileNums);
		for (Integer curLogFileNum:logFileNums)
		{
			LogFile checkLog = logFiles.get(curLogFileNum);
			int numBlocks = checkLog.getNumBlocks();
			for (int curBlockNum = 0; curBlockNum < numBlocks; curBlockNum++)
			{
				BlockInfo curBlockInfo = checkLog.getBlockInfoForBlock(curBlockNum);
				if (curBlockInfo.getSegmentFileID().equals(backingID))
				{
					//updateDevice.
				}
			}
		}
	}
}
