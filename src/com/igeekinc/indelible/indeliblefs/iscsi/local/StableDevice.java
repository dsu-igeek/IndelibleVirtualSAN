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
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.exceptions.ForkNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIdentifier;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.util.BitTwiddle;
import com.igeekinc.util.FilePath;
import com.igeekinc.util.SHA1HashID;
import com.igeekinc.util.logging.ErrorLogMessage;
import com.igeekinc.util.rwlock.ReadWriteLock;
import com.igeekinc.util.rwlock.ReadWriteLockImplementation;

/**
 * The StableDevice handles all of the data for a device that has been committed
 * to the IndelibleFS and, therefore, is "stable".  The StableDevice is only
 * changed when data is committed from the log files to the IndelibleFS backing
 * file.
 * @author David L. Smith-Uchida
 *
 */

class SegmentStatus
{
	private CASIdentifier id;
	private boolean loaded, loadFromReference;
	public static final int kSegmentStatusSize = CASIdentifier.kCASIdentifierSize + 4;	// 32 bytes total
	private ReadWriteLock lock;
	public SegmentStatus(CASIdentifier id, boolean loaded)
	{
		super();
		this.id = id;
		this.loaded = loaded;
		lock = new ReadWriteLockImplementation();
	}
	
	public SegmentStatus(byte [] source, int offset)
	{
		id = new CASIdentifier(source, offset);
		offset += CASIdentifier.kCASIdentifierSize;
		int segmentFlags = BitTwiddle.byteArrayToInt(source, offset, BitTwiddle.kLittleEndian);
		if (segmentFlags == 1)
			loaded = true;
		lock = new ReadWriteLockImplementation();
	}
	public CASIdentifier getId()
	{
		return id;
	}
	
	public void setID(CASIdentifier id)
	{
		lockForWriting();
		this.id = id;
		
	}
	
	public boolean lockForReading()
	{
		return lockForReading(-1);
	}
	
	public boolean lockForReading(int waitTime)
	{
		return lock.forReading(waitTime);
	}
	
	public boolean lockForWriting()
	{
		return lockForWriting(-1);
	}
	
	public boolean lockForWriting(int waitTime)
	{
		return lock.forWriting(waitTime);
	}
	
	public void release()
	{
		lock.release();
	}
	
	public boolean isLoaded()
	{
		return loaded;
	}
	
	public void setLoaded(boolean loaded)
	{
		this.loaded = loaded;
	}
	
	public boolean isLoadFromReference()
	{
		return loadFromReference;
	}
	public void setLoadFromReference(boolean loadFromReference)
	{
		this.loadFromReference = loadFromReference;
	}
	public void getBytes(byte [] buffer, int offset)
	{
		lock.forReading();
		id.getBytes(buffer, offset);
		offset += CASIdentifier.kCASIdentifierSize;
		int segmentFlags = 0;
		if (loaded)
			segmentFlags = 1;
		BitTwiddle.intToByteArray(segmentFlags, buffer, offset, BitTwiddle.kLittleEndian);
		lock.release();
	}
}

public class StableDevice
{
	long size;
	int segmentSize;
	RandomAccessFile stableRAFile;
	FileChannel stableChannel;
	StableSegmentPool segmentPool;
	FilePath stablePath;
	
	SegmentStatus [] segmentStatus;
	IndelibleFileNodeIF backingObject;
	IndelibleFSObjectID volumeID;
	IndelibleFSObjectID backingObjectID;
	
	Logger logger = Logger.getLogger(getClass());
	
	long headerSequence = 0L;
	int uncommittedWrites = 0;
	boolean writePrimary = true;
	boolean writeFaultedData = true;
	
	public static final long kStartingMagicNumber = 0x62646e49474d496cL;
	public static final int kStartingHeaderMagicNumberOffset = 0;
	public static final int kSegmentSizeOffset = kStartingHeaderMagicNumberOffset + 8;
	public static final int kNumSegmentsOffset = kSegmentSizeOffset + 8;
	public static final int kIndelibleObjectIDOffset = kNumSegmentsOffset + 8;
	public static final int kHeaderSequenceOffset = kIndelibleObjectIDOffset + IndelibleFSObjectID.kTotalBytes;
	public static final int kFlagsOffset = kHeaderSequenceOffset + 8; 
	public static final int kStartingPaddingStart = kFlagsOffset + 4;
	public static final int kHeaderSize = 512;
	
	public static final long kEndingMagicNumber = 0x53474d4943314148L;
	public static final int kEndingHeaderMagicNumberOffset = 0;	// Offset from end of segements status
	public static final int kHashOffset = kEndingHeaderMagicNumberOffset + 8;
	public static final int kEndingPaddingStart = kEndingHeaderMagicNumberOffset + SHA1HashID.kSHA1ByteLength;
	private static final int kIsStable = 1;
	private IndelibleFSForkIF dataFork;
	
	public StableDevice(FilePath stablePath, IndelibleFileNodeIF backingObject, int segmentSize, StableSegmentPool segmentPool) throws IOException, ForkNotFoundException, PermissionDeniedException
	{
		this.backingObject = backingObject;
		this.segmentSize = segmentSize;
		this.segmentPool = segmentPool;
		this.stablePath = stablePath;
		File stableFile = new File(stablePath.toString());
		
		backingObjectID = (IndelibleFSObjectID) backingObject.getObjectID();
		volumeID = backingObject.getVolume().getVolumeID();
		dataFork = backingObject.getFork("data", false);
		this.size = dataFork.length();
		
		if (stableFile.exists())
		{
			throw new IllegalArgumentException(stableFile.getAbsolutePath()+" already exists");
		}

		stableRAFile = new RandomAccessFile(stableFile, "rw");
		int computedNumSegments = (int) (size/segmentSize);
		segmentStatus = new SegmentStatus[computedNumSegments];
		stableRAFile.setLength(size + getDataStartOffset());
		CASIdentifier [] ids = dataFork.getSegmentIDs();
		if (ids.length != segmentStatus.length)
			throw new InternalError("data fork has "+ids.length+" segments, we computed "+segmentStatus.length);
		for (int curIDNum = 0; curIDNum < ids.length; curIDNum++)
		{
			segmentStatus[curIDNum] = new SegmentStatus(ids[curIDNum], false);
		}
		
		stableChannel = stableRAFile.getChannel();
		// Write both copies of the header
		writeHeader(true, true);
		writeHeader(false, true);
	}

	public StableDevice(FilePath stablePath, IndelibleFileNodeIF backingObject, StableSegmentPool segmentPool) throws IOException, ForkNotFoundException, PermissionDeniedException
	{
		this.backingObject = backingObject;
		this.segmentPool = segmentPool;
		this.stablePath = stablePath;
		File stableFile = new File(stablePath.toString());
		
		backingObjectID = (IndelibleFSObjectID) backingObject.getObjectID();
		volumeID = backingObject.getVolume().getVolumeID();
		dataFork = backingObject.getFork("data", false);
		this.size = dataFork.length();
		stableRAFile = new RandomAccessFile(stableFile, "rw");
		stableChannel = stableRAFile.getChannel();
		readHeaders();
	}
	
	public int getSegmentSize() 
	{
		return segmentSize;
	}

	/**
	 * Header layout:
	 * 0 - kStartingMagicNumber (0x62646e49474d496cL - 8 bytes)
	 * 8 - Segment size (8 bytes)
	 * 16 - Number of segments (8 bytes)
	 * 24 - Backing file object ID (32 bytes)
	 * 56 - Header sequence # (8 bytes)
	 * 64 - Header flags (8 bytes)
	 * 72 - Reserved/zeros (440 bytes)
	 * 512 - Segment status (32 bytes * number of segments)
	 * 512 + (32 * number of segments) - kEndingMagicNumber (0x53474d4943314148L - 8 bytes)
	 * 512 + (32 * number of segments) + 8 - SHA-1 hash of header (20 bytes)
	 * @param primary
	 * @param stable
	 * @throws IOException
	 */
	private void writeHeader(boolean primary, boolean stable) throws IOException
	{
		int headerSize = getHeaderSize(segmentStatus.length);
		int paddedHeaderSize = getPaddedHeaderSize(segmentStatus.length);
		byte [] headerBytes = new byte[paddedHeaderSize];
		logger.error(new ErrorLogMessage("headerSize = "+headerSize));
		logger.error(new ErrorLogMessage("paddedHeaderSize = "+paddedHeaderSize));
		BitTwiddle.longToByteArray(kStartingMagicNumber, headerBytes, kStartingHeaderMagicNumberOffset, BitTwiddle.kLittleEndian);
		BitTwiddle.longToByteArray(segmentSize, headerBytes, kSegmentSizeOffset, BitTwiddle.kLittleEndian);
		BitTwiddle.longToByteArray(segmentStatus.length, headerBytes, kNumSegmentsOffset, BitTwiddle.kLittleEndian);
		backingObjectID.getBytes(headerBytes, kIndelibleObjectIDOffset);
		BitTwiddle.longToByteArray(headerSequence, headerBytes, kHeaderSequenceOffset, BitTwiddle.kLittleEndian);
		headerSequence++;
		int flags = 0;
		if (stable)
			flags |= kIsStable;
		BitTwiddle.intToByteArray(flags, headerBytes, kFlagsOffset, BitTwiddle.kLittleEndian);
		
		int offset = kHeaderSize;
		for (int curSegmentStatusNum = 0; curSegmentStatusNum < segmentStatus.length; curSegmentStatusNum++)
		{
			segmentStatus[curSegmentStatusNum].getBytes(headerBytes, offset);
			offset += SegmentStatus.kSegmentStatusSize;
		}
		logger.error(new ErrorLogMessage("Writing magic number at offset "+offset));
		BitTwiddle.longToByteArray(kEndingMagicNumber, headerBytes, offset, BitTwiddle.kLittleEndian);
		offset += 8;
		
		SHA1HashID headerHash = new SHA1HashID();
		int hashLength = headerSize - SHA1HashID.kSHA1ByteLength;
		logger.error("Hashing "+hashLength+" bytes");
		headerHash.update(headerBytes, 0, hashLength);	// Hash everything except the hash itself
		headerHash.finalizeHash();

		headerHash.getBytes(headerBytes, offset);
		String hashString = "";
		for (int hashByteOffset = offset; hashByteOffset < offset + SHA1HashID.kSHA1ByteLength; hashByteOffset ++)
		{
			hashString += Integer.toHexString(((int)headerBytes[hashByteOffset]) & 0xff)+":";
		}
		logger.error("Hash = " + hashString+" at offset "+offset);
		long writeOffset = 0;
		if (!primary)
			writeOffset += paddedHeaderSize;
		stableChannel.write(ByteBuffer.wrap(headerBytes), writeOffset);
	}

	private void readHeaders() throws IOException
	{
		byte [] header1Bytes = new byte[kHeaderSize];
		byte [] header2Bytes = new byte[kHeaderSize];
		
		long readOffset = 0L;
		stableChannel.position(readOffset);
		stableChannel.read(ByteBuffer.wrap(header1Bytes));
		
		boolean primaryConsistent = false, secondaryConsistent = false, usePrimary = false, useSecondary = false;
		long checkBeginningMagic1 = BitTwiddle.byteArrayToLong(header1Bytes, kStartingHeaderMagicNumberOffset, BitTwiddle.kLittleEndian);
		byte [] header1PlusSegmentsStatus = null;
		if (checkBeginningMagic1 == kStartingMagicNumber)
		{
			long header1SegmentSize = BitTwiddle.byteArrayToLong(header1Bytes, kSegmentSizeOffset, BitTwiddle.kLittleEndian);
			long numSegments = BitTwiddle.byteArrayToLong(header1Bytes, kNumSegmentsOffset, BitTwiddle.kLittleEndian);
			
			int segmentStatusSize = (int)numSegments * SegmentStatus.kSegmentStatusSize;
			int headerSize = getHeaderSize((int)numSegments);
			header1PlusSegmentsStatus = new byte[headerSize];
			stableChannel.position(0);
			stableChannel.read(ByteBuffer.wrap(header1PlusSegmentsStatus));
			int segmentEnd = kHeaderSize + segmentStatusSize;
			int endingMagicNumberOffset = segmentEnd + kEndingHeaderMagicNumberOffset;
			logger.error(new ErrorLogMessage("Ending magic number offset = "+endingMagicNumberOffset));
			long checkEndingMagic1 = BitTwiddle.byteArrayToLong(header1PlusSegmentsStatus, endingMagicNumberOffset, BitTwiddle.kLittleEndian);
			if (checkEndingMagic1 == kEndingMagicNumber)
			{
				SHA1HashID headerCheckHash = new SHA1HashID();
				int bytesToHash = headerSize - SHA1HashID.kSHA1ByteLength;
				logger.error("Hashing "+bytesToHash+" bytes");
				headerCheckHash.update(header1PlusSegmentsStatus, 0, bytesToHash);	// Everything up to the hash itself
				headerCheckHash.finalizeHash();

				byte [] headerCheckBytes = headerCheckHash.getBytes();
				byte [] headerHashBytes = new byte[SHA1HashID.kSHA1ByteLength];

				int headerHashOffset = segmentEnd + kHashOffset;
				System.arraycopy(header1PlusSegmentsStatus, headerHashOffset, headerHashBytes, 0, SHA1HashID.kSHA1ByteLength);
				String hashString = "";
				for (byte curByte:headerHashBytes)
				{
					hashString += Integer.toHexString(((int)curByte) & 0xff)+":";
				}
				logger.error("Hash from file = " + hashString+" at offset "+headerHashOffset);
				hashString = "";
				for (byte curByte:headerCheckBytes)
				{
					hashString += Integer.toHexString(((int)curByte) & 0xff)+":";
				}
				logger.error("Check hash = " + hashString);
				
				if (Arrays.equals(headerCheckBytes, headerHashBytes))
					primaryConsistent = true;
			}
		}
		long checkMagic2 = BitTwiddle.byteArrayToLong(header2Bytes, kStartingHeaderMagicNumberOffset, BitTwiddle.kLittleEndian);
		if (checkMagic2 == kStartingMagicNumber)
			secondaryConsistent = true;
		
		if (!primaryConsistent && !secondaryConsistent)
			throw new IOException("Device file is corrupt");
		
		if (!primaryConsistent || !secondaryConsistent)
		{
			if (primaryConsistent)
				usePrimary = true;
			if (secondaryConsistent)
				useSecondary = true;
		}
		else
		{
			usePrimary = true;
		}
		byte [] headerToUse=null;
		if (usePrimary)
			headerToUse = header1PlusSegmentsStatus;
		if (useSecondary)
			headerToUse = header2Bytes;
		
		segmentSize = (int)BitTwiddle.byteArrayToLong(headerToUse, kSegmentSizeOffset, BitTwiddle.kLittleEndian);
		int numSegments = (int)BitTwiddle.byteArrayToLong(headerToUse, kNumSegmentsOffset, BitTwiddle.kLittleEndian);
		segmentStatus = new SegmentStatus[numSegments];
		int offset = kHeaderSize;
		for (int curSegmentNum = 0; curSegmentNum < numSegments; curSegmentNum++)
		{
			segmentStatus[curSegmentNum] = new SegmentStatus(headerToUse, offset);
			offset += SegmentStatus.kSegmentStatusSize;
		}
	}

	public int getDataStartOffset()
	{
		return getPaddedHeaderSize(segmentStatus.length) * 2;
	}

	
	public int getPaddedHeaderSize(int numSegments) 
	{
		int paddedHeaderSize = getHeaderSize(numSegments);
		if (paddedHeaderSize % segmentSize != 0)
			paddedHeaderSize = ((paddedHeaderSize/segmentSize) + 1) * segmentSize;
		return paddedHeaderSize;
	}

	public int getHeaderSize(int numSegments) 
	{
		int headerSize = kHeaderSize;
		headerSize += numSegments * SegmentStatus.kSegmentStatusSize;
		headerSize += 8;	// Ending magic number
		headerSize += SHA1HashID.kSHA1ByteLength;
		return headerSize;
	}
	
	
	public void read(ByteBuffer updateBuffer, long offset) throws IOException
	{

		int segmentNum = (int) (offset/segmentSize);
		int segmentOffset = (int) (offset - getOffsetForSegment(segmentNum));
		int bytesToRead = updateBuffer.limit();
		int bufferOffset = 0;
		
		while(bytesToRead > 0)
		{
			SegmentStatus curStatus = segmentStatus[segmentNum];
			curStatus.lockForReading();
			try
			{
				long readOffset = getOffsetForSegmentInBackingFile(segmentNum) + segmentOffset;
				ByteBuffer readBuffer = updateBuffer.duplicate();
				readBuffer.position(bufferOffset);
				int maxRead = segmentSize - segmentOffset;
				if (readBuffer.limit() > maxRead+bufferOffset)
					readBuffer.limit(maxRead + bufferOffset);
				if (curStatus.isLoaded())
				{
					if (stableChannel.read(readBuffer, readOffset) != readBuffer.limit())
						throw new IOException("Premature EOF at offset "+readOffset+" reading "+readBuffer.limit());
				}
				else
				{
					faultSegment(segmentNum, readBuffer, segmentOffset);
				}
				bytesToRead -= readBuffer.limit();
				segmentOffset = 0;	// After first read, segmentOffset is always 0
				segmentNum++;
			}
			finally
			{
				curStatus.release();
			}
		}
	}
	
	public void write(ByteBuffer writeBuffer, long offset) throws IOException
	{
		int segmentNum = (int) (offset/segmentSize);
		int segmentOffset = (int) (offset - getOffsetForSegment(segmentNum));
		int bytesToWrite = writeBuffer.limit();
		int bufferOffset = 0;
		
		while(bytesToWrite > 0)
		{
			SegmentStatus curStatus = segmentStatus[segmentNum];
			curStatus.lockForWriting();
			try
			{
				long writeOffset = getOffsetForSegmentInBackingFile(segmentNum) + segmentOffset;
				ByteBuffer curWriteBuffer = writeBuffer.duplicate();
				curWriteBuffer.position(bufferOffset);
				int maxRead = segmentSize - segmentOffset;
				if (curWriteBuffer.limit() > maxRead+bufferOffset)
					curWriteBuffer.limit(maxRead + bufferOffset);
				if (curStatus.isLoaded())
				{
					if (stableChannel.write(curWriteBuffer, writeOffset) != curWriteBuffer.limit())
						throw new IOException("Premature EOF at offset "+writeOffset+" reading "+curWriteBuffer.limit());
				}
				else
				{
					faultSegment(segmentNum, curWriteBuffer, segmentOffset);
				}
				bytesToWrite -= curWriteBuffer.limit();
				segmentOffset = 0;	// After first read, segmentOffset is always 0
				segmentNum++;
			}
			finally
			{
				curStatus.release();
			}
		}
	}
	
	public boolean read(ByteBuffer readBuffer, int segmentNum) throws IOException
	{
		SegmentStatus curStatus = segmentStatus[segmentNum];
		if (curStatus.isLoaded())
		{
			long readOffset = getOffsetForSegmentInBackingFile(segmentNum);
			if (stableChannel.read(readBuffer, readOffset) != readBuffer.limit())
				throw new IOException("Premature EOF at offset "+readOffset+" reading "+readBuffer.limit());
			return true;
		}
		else
		{
			return false;
		}
	}

	public IndelibleFSObjectID getBackingObjectID()
	{
		return backingObjectID;
	}

	private long getOffsetForSegment(int segmentNum) 
	{
		return ((long)segmentNum * (long)segmentSize);
	}
	
	private long getOffsetForSegmentInBackingFile(int segmentNum) 
	{
		return ((long)segmentNum * (long)segmentSize)  + (long)getDataStartOffset();
	}
	
	public synchronized void faultSegment(int segmentNum, ByteBuffer readBuffer, int offsetInSegment) throws IOException
	{
		SegmentStatus faultSegmentStatus = segmentStatus[segmentNum];
		byte [] readData = null;
		boolean dataNotRead = true;
		if (segmentPool.containsSegment(faultSegmentStatus.getId()))
		{
			readData = new byte[segmentSize];
			ByteBuffer faultBuffer = ByteBuffer.wrap(readData);
			if (segmentPool.getSegmentData(faultSegmentStatus.getId(), faultBuffer))
				dataNotRead = false;
		}

		if (dataNotRead)
		{
			CASIDDataDescriptor readDescriptor = dataFork.getDataDescriptor(getOffsetForSegment(segmentNum), segmentSize);
			CASIdentifier readIdentifier = readDescriptor.getCASIdentifier();
			if (!faultSegmentStatus.getId().equals(readIdentifier))
			{
				Logger.getLogger(getClass()).error(new ErrorLogMessage("ids don't match"));
				faultSegmentStatus = new SegmentStatus(readIdentifier, false);
				segmentStatus[segmentNum] = faultSegmentStatus;
			}
			readData = readDescriptor.getData();
		}
		readBuffer.put(readData, offsetInSegment, readBuffer.limit());
		if (writeFaultedData)
		{
			ByteBuffer writeBuffer = ByteBuffer.wrap(readData);
			stableChannel.write(writeBuffer, getOffsetForSegmentInBackingFile(segmentNum));
			faultSegmentStatus.setLoaded(true);
			segmentPool.registerSegment(faultSegmentStatus.getId(), this, segmentNum);
			uncommittedWrites++;
			if (uncommittedWrites > 1024)
			{
				writeHeader(writePrimary, true);
				uncommittedWrites = 0;
			}
		}
	}
	
	/**
	 * Load all of the segments from the server.  Usually run in the background after the
	 * file is first initialized
	 * @throws IOException
	 */
	public void loadAll() throws IOException
	{
		ByteBuffer readBuffer = ByteBuffer.wrap(new byte[segmentSize]);
		for (int curSegmentNum = 0; curSegmentNum < segmentStatus.length; curSegmentNum++)
		{
			if (!segmentStatus[curSegmentNum].isLoaded())
				faultSegment(curSegmentNum, readBuffer, 0);
		}
	}
	
	public void readLockSegment(int segmentNum)
	{
		segmentStatus[segmentNum].lockForReading();
	}
	
	public void releaseSegment(int segmentNum)
	{
		segmentStatus[segmentNum].release();
	}

	public long getSize() throws IOException 
	{
		return size;
	}
	
	public IndelibleFSObjectID getVolumeID()
	{
		return volumeID;
	}

	public FilePath getStablePath() 
	{
		return stablePath;
	}

	public CASIdentifier identifierForSegment(int curSegmentNum) 
	{
		return segmentStatus[curSegmentNum].getId();
	}

	public synchronized void updateToBackingFile() throws IOException, PermissionDeniedException
	{
		try {
			dataFork = backingObject.getFork("data", false);
			CASIdentifier [] ids = dataFork.getSegmentIDs();
			if (ids.length != segmentStatus.length)
				throw new InternalError("data fork has "+ids.length+" segments, we computed "+segmentStatus.length);
			for (int curIDNum = 0; curIDNum < ids.length; curIDNum++)
			{
				// If the ID's don't match then we need to update the segment.  Mark it as missing and move on
				if (!segmentStatus[curIDNum].getId().equals(ids[curIDNum]))
					segmentStatus[curIDNum] = new SegmentStatus(ids[curIDNum], false);
			}
		} catch (ForkNotFoundException e) {
			throw new InternalError("Could not find data fork for "+getStablePath());
		} 

	}
}
