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

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;

/**
 * A Segment is allocated for each segment in the backing IndelibleFS
 * file.  Each segment is equally sized, usually 1 megabyte.
 * 
 * A segment consists of the IndelibleFS CASIdentifier it maps to and 0 or more
 * LogBlockIdentifiers that identify updates that have not yet been committed to the
 * IndelibleFS file.
 * 
 * @author David L. Smith-Uchida
 *
 */
public class Segment implements Cloneable
{
	private long offset;
	private int length;
	private CASIdentifier casID;
	private LogBlockIdentifier [] updates;
	private LogManager logManager;
	private StableDevice stableDevice;
	private int blockSize;
	private int externalReferences;
	private boolean external;	// Indicates if this segment is in the StableDevice or should be looked up in the general pool
	private long lastWriteTime;
	private Logger logger = Logger.getLogger(getClass());
	
	public Segment(long offset, int length, CASIdentifier casID, LogManager logManager, StableDevice stableDevice)
	{
		if (offset < 0)
			throw new IllegalArgumentException("Offset cannot be negative");
		this.offset = offset;
		this.length = length;
		this.casID = casID;
		this.logManager = logManager;
		this.stableDevice = stableDevice;
		if (length % logManager.getBlockSize() != 0)
			throw new IllegalArgumentException("length must be a multiple of the logManger block size ("+logManager.getBlockSize()+")");
		updates = new LogBlockIdentifier[length / logManager.getBlockSize()];
		blockSize = logManager.getBlockSize();
		externalReferences = 0;
	}
	
	/**
	 * Reads data from this segment.  Takes care of reading from the main file or the log file
	 * @param readOffset - offset in the segment to start at
	 * @param readLength - number of bytes in the segment to read
	 * @param buffer - the buffer to fill in
	 * @return - reads all the bytes unless an exception happens
	 */
	public synchronized void read(final int readOffset, final ByteBuffer buffer, boolean commitToStable)
	throws IOException
	{
		int readLength = buffer.remaining();
		ByteBuffer readBuffer = buffer.duplicate();
		
		if (readOffset + readLength > length)
			throw new IllegalArgumentException("readOffset = "+readOffset+" + readLength = "+readLength+" > "+length);
		
		int curReadOffset = readOffset;
		int startBlockOffset = curReadOffset % blockSize;
		int blockNum = curReadOffset / blockSize;
		if (startBlockOffset != 0)
		{
			ByteBuffer curReadBuffer = readBuffer.slice();
			int bytesToRead = blockSize - startBlockOffset;
			if (bytesToRead > readLength)
				bytesToRead = readLength;
			curReadBuffer.limit(bytesToRead);
			readBuffer.position(bytesToRead);
			
			ByteBuffer blockBuffer = ByteBuffer.allocate(blockSize);

			
			//System.out.println("Misaligned read - pre.  readOffset = "+readOffset+", bytesToRead = "+bytesToRead+", blockNum = "+blockNum);
			if (commitToStable)
				readAndCommitBlock(blockNum, blockBuffer);
			else
				readBlock(blockNum, blockBuffer);
			blockBuffer.position(startBlockOffset);
			blockBuffer.limit(startBlockOffset + bytesToRead);
			curReadBuffer.put(blockBuffer);
			
			curReadOffset += bytesToRead;
			readLength -= bytesToRead;
			blockNum++;
		}
		
		if (readLength > 0)
		{
			if (curReadOffset % blockSize != 0)
				throw new InternalError("Miscalculated! curReadOffset = "+curReadOffset);

			int midReadLength = (readLength/blockSize) * blockSize;

			int midBlocksToRead = midReadLength / blockSize;
			int stopBlock = blockNum + midBlocksToRead;
			while (blockNum < stopBlock)
			{
				ByteBuffer curReadBuffer = readBuffer.slice();
				curReadBuffer.limit(blockSize);
				readBuffer.position(readBuffer.position() + blockSize);
				//System.out.println("Aligned read, blockNum = "+blockNum);
				if (commitToStable)
					readAndCommitBlock(blockNum, curReadBuffer);
				else
					readBlock(blockNum, curReadBuffer);
				readLength -= blockSize;
				blockNum++;
			}

			if (readLength > 0)
			{
				ByteBuffer curReadBuffer = readBuffer.slice();
				if (readLength > blockSize)
					logger.error("Too many offset bytes remaining = "+readLength);
				ByteBuffer blockBuffer = ByteBuffer.allocate(blockSize);
				int readBlock = blockNum;
				//System.out.println("Misaligned read - post.  bytesToRead = "+readLength+", blockNum = "+readBlock);				
				if (commitToStable)
					readAndCommitBlock(readBlock, blockBuffer);
				else
					readBlock(readBlock, blockBuffer);
				blockBuffer.position(0);
				blockBuffer.limit(readLength);
				curReadBuffer.put(blockBuffer);
			}
		}
	}

	private void readBlock(int blockNum, ByteBuffer curReadBuffer)
			throws IOException 
	{
		if (updates[blockNum] != null)
		{
			logger.debug("blockNum "+blockNum+" (offset = "+blockNum * blockSize+") has been updated, reading from log file");
			logManager.read(updates[blockNum], curReadBuffer);
		}
		else
		{
			stableDevice.read(curReadBuffer, (long)blockNum * (long)blockSize + offset);
		}
	}
	
	private void readAndCommitBlock(int blockNum, ByteBuffer curReadBuffer)
			throws IOException 
	{
		if (updates[blockNum] != null)
		{
			logger.debug("blockNum "+blockNum+" (offset = "+blockNum * blockSize+") has been updated, reading from log file");
			logManager.read(updates[blockNum], curReadBuffer);
			stableDevice.write(curReadBuffer, (long)blockNum * (long)blockSize + offset);
		}
		else
		{
			stableDevice.read(curReadBuffer, (long)blockNum * (long)blockSize + offset);
		}
	}
	
	/**
	 * Writes data to this segment.  Takes care of writing to the log file and updating the updates array
	 * as necessary and maintaining the reference counts on the LogBlockIdentifiers
	 * @param writeOffset
	 * @param buffer
	 * @return The writeID of the last write performed for this write
	 * @throws IOException 
	 */

	public synchronized long write(int writeOffset, final ByteBuffer buffer) throws IOException
	{
		long writeID = 0;
		int writeLength = buffer.remaining();
		ByteBuffer writeBuffer = buffer.duplicate();
		
		if (writeOffset + writeLength > length)
			throw new IllegalArgumentException("writeOffset = "+writeOffset+" + writeLength = "+writeLength+" > "+length);
		
		int curWriteOffset = writeOffset;
		int startBlockOffset = curWriteOffset % blockSize;
		int blockNum = curWriteOffset / blockSize;
		if (startBlockOffset != 0)
		{
			ByteBuffer curWriteBuffer = writeBuffer.slice();
			int bytesToWrite = blockSize - startBlockOffset;
			if (bytesToWrite > writeLength)
				bytesToWrite = writeLength;
			curWriteBuffer.limit(bytesToWrite);
			writeBuffer.position(bytesToWrite);
			
			ByteBuffer blockBuffer = ByteBuffer.allocate(blockSize);
			
			// Get the previous block to update
			readBlock(blockNum, blockBuffer);
			blockBuffer.position(startBlockOffset);
			blockBuffer.limit(startBlockOffset + bytesToWrite);
			blockBuffer.put(curWriteBuffer);
			
			blockBuffer.position(0);
			blockBuffer.limit(blockSize);
			writeID = writeBlock(blockNum, blockBuffer);
			
			curWriteOffset += bytesToWrite;
			writeLength -= bytesToWrite;
			blockNum++;
		}
		
		if (writeLength > 0)
		{
			if (curWriteOffset % blockSize != 0)
				throw new InternalError("Miscalculated! curWriteOffset = "+curWriteOffset);

			int midWriteLength = (writeLength/blockSize) * blockSize;

			int midBlocksToWrite = midWriteLength / blockSize;
			int stopBlock = blockNum + midBlocksToWrite;
			while (blockNum < stopBlock)
			{
				ByteBuffer curWriteBuffer = writeBuffer.slice();
				curWriteBuffer.limit(blockSize);
				writeBuffer.position(writeBuffer.position() + blockSize);
				//System.out.println("Aligned read, blockNum = "+blockNum);
				writeID = writeBlock(blockNum, curWriteBuffer);
				writeLength -= blockSize;
				blockNum++;
			}

			if (writeLength > 0)
			{
				ByteBuffer curWriteBuffer = writeBuffer.slice();
				if (writeLength > blockSize)
					logger.error("Too many offset bytes remaining = "+writeLength);
				ByteBuffer blockBuffer = ByteBuffer.allocate(blockSize);
				
				// Get the previous block to update
				readBlock(blockNum, blockBuffer);
				blockBuffer.position(0);
				blockBuffer.limit(writeLength);
				blockBuffer.put(curWriteBuffer);
				
				blockBuffer.position(0);
				blockBuffer.limit(blockSize);
				writeID = writeBlock(blockNum, blockBuffer);
			}
		}
		return writeID;
	}

	private long writeBlock(int blockNum, ByteBuffer curWriteBuffer)
			throws IOException {
		if (updates[blockNum] != null)
			updates[blockNum].decrementReferenceCount();
		updates[blockNum] = logManager.appendBlock(curWriteBuffer, stableDevice.backingObjectID, (long)blockNum * (long)blockSize);
		return updates[blockNum].getWriteID();
	}

	@Override
	/**
	 * Clones the current segment.  Usually used as part of taking a snapshot of the device.
	 */
	protected synchronized Segment clone() throws CloneNotSupportedException
	{
		Segment segmentClone = new Segment(offset, length, casID, logManager, stableDevice);
		System.arraycopy(updates, 0, segmentClone.updates, 0, updates.length);
		return segmentClone;
	}
	
	public long getOffset()
	{
		return offset;
	}
	
	public int getLength()
	{
		return length;
	}
	
	public int getExternalReferences()
	{
		return externalReferences;
	}
	
	public int incrementExternalReferences()
	{
		externalReferences++;
		return externalReferences;
	}
	
	public int decrementExternalReferences()
	{
		externalReferences--;
		return externalReferences;
	}
	
	public LogBlockIdentifier [] getUpdates()
	{
		return updates;
	}
	
	public synchronized void discardUpdatesBefore(long highestCommittedWriteID)
	{
		for (int curBlockNum = 0; curBlockNum < updates.length; curBlockNum++)
		{
			if (updates[curBlockNum] != null && updates[curBlockNum].getWriteID() <= highestCommittedWriteID)
			{
				updates[curBlockNum].decrementReferenceCount();
				updates[curBlockNum] = null;
			}
		}
	}
}
