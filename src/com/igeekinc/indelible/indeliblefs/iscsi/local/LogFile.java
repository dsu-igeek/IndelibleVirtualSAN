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
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.util.BitTwiddle;
import com.igeekinc.util.logging.ErrorLogMessage;

public class LogFile 
{
	private File file;
	private RandomAccessFile backingFile;
	private FileChannel backingChannel;
	private int numBlocks;
	byte [] header;
	public static final int kHeaderSize = 512;
	
	public static final int kBlockSize = 4096;
	private int kBlockHeaderSize = 512;
	private int kBlockTotalSize = kBlockSize+kBlockHeaderSize;
	private long appendOffset;
	private Logger logger = Logger.getLogger(getClass());
	private HashSet<Long>inUse = new HashSet<Long>();
	
	public LogFile(File file) throws IOException
	{
		boolean exists = file.exists();
		this.file = file;
		backingFile = new RandomAccessFile(file, "rw");
		header = new byte[kHeaderSize];
		if (exists && backingFile.length() > kHeaderSize)
		{
			backingFile.seek(0);
			backingFile.read(header);
		}
		else
		{
			backingFile.seek(0);
			backingFile.write(header);
		}
		appendOffset = backingFile.length();
		backingChannel = backingFile.getChannel();
		numBlocks = (int)((appendOffset - kHeaderSize)/kBlockHeaderSize + kBlockSize);
	}
	
	byte [] writeHeader = new byte[kBlockHeaderSize];
	public synchronized long appendBlock(ByteBuffer bufferToAppend, IndelibleFSObjectID segmentFileID, long segmentFileOffset) throws IOException
	{
		if (bufferToAppend.limit() != kBlockSize)
			throw new IllegalArgumentException(bufferToAppend.limit() +"!= "+kBlockSize);
		segmentFileID.getBytes(writeHeader, 0);
		BitTwiddle.longToByteArray(segmentFileOffset, writeHeader, IndelibleFSObjectID.kTotalBytes, BitTwiddle.kLittleEndian);
		long returnBlockNum = (appendOffset - kHeaderSize)/(kBlockHeaderSize + kBlockSize);
		backingChannel.write(ByteBuffer.wrap(writeHeader), appendOffset);
		appendOffset += kBlockHeaderSize;
		backingChannel.write(bufferToAppend, appendOffset);
		logger.debug("Appended to logfile "+file.getName()+", segment file offset = "+segmentFileOffset+" header at "+(appendOffset - kBlockHeaderSize)+", data at "+appendOffset);
		appendOffset += kBlockSize;
		numBlocks ++;
		inUse.add(returnBlockNum);
		return returnBlockNum;
	}
	
	public void read(long blockNum, ByteBuffer updateBuffer) throws IOException 
	{
		if (updateBuffer.limit() != kBlockSize)
			throw new IllegalArgumentException(updateBuffer.limit() +"!= "+kBlockSize);
		long offset = getOffset(blockNum);
		offset += kBlockHeaderSize;	// Skip over the block header
		logger.debug("Reading from logfile "+file.getName()+", offset = "+offset);
		backingChannel.read(updateBuffer, offset);
	}
	
	public int getNumBlocks()
	{
		return numBlocks;
	}
	
	public synchronized BlockInfo getBlockInfoForBlock(long retrieveBlockNum) throws IOException
	{
		long offset = getOffset(retrieveBlockNum);
		byte [] header = new byte[kHeaderSize];
		backingChannel.read(ByteBuffer.wrap(header), offset);
		IndelibleFSObjectID segmentFileID = (IndelibleFSObjectID) ObjectIDFactory.reconstituteFromBytes(header, 0, IndelibleFSObjectID.kTotalBytes);
		long segmentFileOffset = BitTwiddle.byteArrayToLong(header, IndelibleFSObjectID.kTotalBytes, BitTwiddle.kLittleEndian);
		BlockInfo returnInfo = new BlockInfo(segmentFileID, segmentFileOffset);
		return returnInfo;
	}

	protected long getOffset(long retrieveBlockNum)
	{
		return retrieveBlockNum * (kBlockHeaderSize + kBlockSize) + kHeaderSize;
	}
	
	/**
	 * Releases a block in use in the log file.  Returns true if no blocks are in use
	 * @param blockNumToRelease
	 * @return
	 */
	public synchronized boolean release(long blockNumToRelease)
	{
		inUse.remove(blockNumToRelease);
		return inUse.size() == 0;
	}
	
	public synchronized int getNumBlocksInUse()
	{
		return inUse.size();
	}
	
	public synchronized void remove()
	{
		if (inUse.size() > 0)
			throw new IllegalArgumentException("Cannot remove log file with in-use blocks");
		try
		{
			backingChannel.close();
		} catch (IOException e)
		{
			Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
		}
		file.delete();
	}
	
	public synchronized void close() throws IOException
	{
		backingChannel.close();
	}
}
