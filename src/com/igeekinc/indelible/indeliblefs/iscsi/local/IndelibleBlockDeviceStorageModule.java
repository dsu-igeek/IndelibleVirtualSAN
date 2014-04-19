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

import org.jscsi.target.storage.AbstractStorageModule;

import com.igeekinc.indelible.indeliblefs.iscsi.IndelibleFSStorageModule;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.util.FilePath;

public class IndelibleBlockDeviceStorageModule extends IndelibleFSStorageModule 
{
	IndelibleBlockDevice device;
	protected FilePath storagePath;
	public static final int kBlockSize = 512;
	protected IndelibleBlockDeviceStorageModule(IndelibleBlockDevice device, FilePath storagePath) throws IOException 
	{
		super(device.getSize()/kBlockSize);
		this.device = device;
		this.storagePath = storagePath;
	}

	@Override
	public void read(byte[] bytes, int bytesOffset, int length, long storageIndex) throws IOException 
	{
		long startTime = System.currentTimeMillis();
		try
		{
			ByteBuffer readBuffer = ByteBuffer.wrap(bytes);
			readBuffer.position(bytesOffset);
			readBuffer.limit(bytesOffset + length);
			device.read(storageIndex, readBuffer);

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
		finally
		{
			/*
			long finishTime = System.currentTimeMillis();
			System.out.println("Read "+length+" at "+storageIndex+" in "+(finishTime-startTime)+" ms");
			*/
		}
	}

	@Override
	public void write(byte[] bytes, int bytesOffset, int length, long storageIndex) throws IOException 
	{
		ByteBuffer writeBuffer = ByteBuffer.wrap(bytes);
		writeBuffer.position(bytesOffset);
		writeBuffer.limit(bytesOffset + length);
		device.write(storageIndex, writeBuffer);
	}

	public IndelibleFSObjectID getVolumeID() 
	{
		return device.getVolumeID();
	}

	public FilePath getStoragePath() 
	{
		return storagePath;
	}

	@Override
	public int getBlockSizeInBytes() 
	{
		return kBlockSize;
	}

	@Override
	public StorageState getState()
	{
		return device.getState();
	}

	@Override
	public long getBytesToWriteBack()
	{
		// TODO Auto-generated method stub
		return 0;
	}

}
