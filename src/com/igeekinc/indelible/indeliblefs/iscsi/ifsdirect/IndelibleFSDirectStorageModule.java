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
 
package com.igeekinc.indelible.indeliblefs.iscsi.ifsdirect;

import java.io.IOException;
import java.rmi.RemoteException;

import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSVolumeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.exceptions.ForkNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.ObjectNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.iscsi.IndelibleFSStorageModule;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDMemoryDataDescriptor;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.util.FilePath;

public class IndelibleFSDirectStorageModule extends IndelibleFSStorageModule
{
    IndelibleFSObjectID volumeID;
    FilePath storagePath;
    IndelibleFileNodeIF fileNode;
    IndelibleFSForkIF dataFork;
    
    public static IndelibleFSDirectStorageModule open(IndelibleFSVolumeIF volume, FilePath storagePath) 
            throws ObjectNotFoundException, PermissionDeniedException, RemoteException, IOException, ForkNotFoundException
    {
        if (!storagePath.isAbsolute())
            storagePath = FilePath.getFilePath("/").getChild(storagePath);
        IndelibleFileNodeIF fileNode = volume.getObjectByPath(storagePath);
        return new IndelibleFSDirectStorageModule(storagePath, fileNode);
    }
    int kFileBlockSize = 1024*1024;
    public IndelibleFSDirectStorageModule(FilePath storagePath, IndelibleFileNodeIF fileNode) throws ForkNotFoundException, IOException, PermissionDeniedException
    {
        super(fileNode.getFork("data", false).length()/VIRTUAL_BLOCK_SIZE);
        this.fileNode = fileNode;
        this.storagePath = storagePath;
        volumeID = fileNode.getVolume().getVolumeID();
        dataFork = fileNode.getFork("data", false);
    }

    @Override
    public synchronized void read(byte[] bytes, int bytesOffset, int length,
            long storageIndex) throws IOException
    {
    	try
    	{
    		long startTime = System.currentTimeMillis();
    		int bytesToRead = length;
    		long offset = storageIndex;
    		int bufOffset = bytesOffset;
    		while (bytesToRead > 0)
    		{
    			int bytesToReadNow = bytesToRead;
    			if (bytesToReadNow > bytesToRead)
    				bytesToReadNow = bytesToRead;
    			CASIDDataDescriptor dataDescriptor = dataFork.getDataDescriptor(offset, bytesToReadNow);
    			dataDescriptor.getData(bytes, bufOffset, 0, bytesToReadNow, true);
    			dataDescriptor.close();
    			bytesToRead -= bytesToReadNow;
    			offset += bytesToReadNow;
    			bufOffset += bytesToReadNow;
    		}
    		long elapsed = System.currentTimeMillis() - startTime;
    		System.out.println("Reading "+length+" from "+storageIndex+" in "+elapsed+" mss");

    	}
    	catch (IOException e1)
    	{
    		e1.printStackTrace();
    		throw e1;
    	}
    	catch (Throwable t)
    	{
    		t.printStackTrace();
    	}
    }

    @Override
    public synchronized void write(byte[] bytes, int bytesOffset, int length,
            long storageIndex) throws IOException
    {
        // We're going to do writes on 1M boundaries
        // If everything is aligned, just do it
        if (storageIndex % kFileBlockSize == 0 && length % kFileBlockSize == 0)
        {
            CASIDDataDescriptor source = new CASIDMemoryDataDescriptor(bytes, bytesOffset, length);
            dataFork.writeDataDescriptor(storageIndex, source);
        }
        else
        {
            long fileOffset = (storageIndex /(long)kFileBlockSize) * (long)kFileBlockSize;
            int bytesToWrite = length;
            if (fileOffset != storageIndex)
            {
                // Starting off misaligned so we need to read/modify/write
                byte [] rwBuffer = new byte[(int)kFileBlockSize];
                CASIDDataDescriptor oldDataDescriptor = dataFork.getDataDescriptor(fileOffset, kFileBlockSize);
                oldDataDescriptor.getData(rwBuffer, 0, 0, kFileBlockSize, true);
                int rwBufferOffset = (int)(storageIndex - fileOffset);
                int bytesToCopyNow = kFileBlockSize - rwBufferOffset;
                if (bytesToCopyNow > length)
                    bytesToCopyNow = length;
                System.arraycopy(bytes, bytesOffset, rwBuffer, rwBufferOffset, bytesToCopyNow);
                CASIDDataDescriptor newDataDescriptor = new CASIDMemoryDataDescriptor(rwBuffer);
                dataFork.writeDataDescriptor(fileOffset, newDataDescriptor);
                fileOffset += kFileBlockSize;
                bytesOffset += bytesToCopyNow;
                bytesToWrite -= bytesToCopyNow;
            }
            if (fileOffset % kFileBlockSize != 0)
                throw new InternalError("Math mistake in write");
            while (bytesToWrite >= kFileBlockSize)
            {
                // 
                CASIDDataDescriptor writeDescriptor = new CASIDMemoryDataDescriptor(bytes, bytesOffset, kFileBlockSize);
                dataFork.writeDataDescriptor(fileOffset, writeDescriptor);
                fileOffset += kFileBlockSize;
                bytesOffset += kFileBlockSize;
                bytesToWrite -= kFileBlockSize;
            }
            if (bytesToWrite > kFileBlockSize - 1)
                throw new InternalError("Math mistake in write");
            if (bytesToWrite > 0)
            {
                // The end is misaligned so we need to read/modify/write
                byte [] rwBuffer = new byte[(int)kFileBlockSize];
                CASIDDataDescriptor oldDataDescriptor = dataFork.getDataDescriptor(fileOffset, kFileBlockSize);
                oldDataDescriptor.getData(rwBuffer, 0, 0, kFileBlockSize, true);
                System.arraycopy(bytes, bytesOffset, rwBuffer, 0, bytesToWrite);
                CASIDDataDescriptor newDataDescriptor = new CASIDMemoryDataDescriptor(rwBuffer);
                dataFork.writeDataDescriptor(fileOffset, newDataDescriptor);
                fileOffset += kFileBlockSize;
                bytesOffset += bytesToWrite;
                bytesToWrite -= bytesToWrite;
                bytesOffset += bytesToWrite;
            }
            
            if (bytesToWrite != 0)
                throw new InternalError("Math mistake in write");
        }
    }

    public IndelibleFSObjectID getVolumeID()
    {
        return volumeID;
    }

    public FilePath getStoragePath()
    {
        return storagePath;
    }

	@Override
	public StorageState getState()
	{
		return StorageState.kStable;	// We're always stable because we write directly to IFS
	}

	@Override
	public long getBytesToWriteBack()
	{
		return 0;
	}
}
