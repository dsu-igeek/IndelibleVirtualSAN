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

import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.util.FilePath;

public class IndelibleFSLocalStorageModule extends AbstractStorageModule
{
	IndelibleBlockDevice blockDevice;
    FilePath indeliblePath;
    
    public static IndelibleFSLocalStorageModule open(IndelibleBlockDevice blockDevice, FilePath indeliblePath) throws IOException
    {
    	return new IndelibleFSLocalStorageModule(blockDevice, indeliblePath);
    }
    
    int kFileBlockSize = 1024*1024;
    public IndelibleFSLocalStorageModule(IndelibleBlockDevice blockDevice, FilePath indeliblePath) throws IOException
    {
    	super(blockDevice.getSize()/VIRTUAL_BLOCK_SIZE);
    	this.blockDevice = blockDevice;
    	this.indeliblePath = indeliblePath;
    }

    @Override
    public synchronized void read(byte[] bytes, int bytesOffset, int length,
            long storageIndex) throws IOException
    {
    	try
    	{
    		ByteBuffer readBuffer = ByteBuffer.wrap(bytes, bytesOffset, length);
    		blockDevice.read(storageIndex, readBuffer);
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
    public synchronized void write(byte[] bytes, int bytesOffset, int length, long storageIndex) throws IOException
    {
    	try
    	{
    		ByteBuffer writeBuffer = ByteBuffer.wrap(bytes, bytesOffset, length);
    		blockDevice.write(storageIndex, writeBuffer);
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

    public IndelibleFSObjectID getVolumeID()
    {
        return blockDevice.getVolumeID();
    }

    public FilePath getStoragePath()
    {
        return indeliblePath;
    }
}
