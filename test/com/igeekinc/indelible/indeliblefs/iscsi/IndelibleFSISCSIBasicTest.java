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
 
package com.igeekinc.indelible.indeliblefs.iscsi;

import java.io.File;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Arrays;

import junit.framework.TestCase;

import org.apache.log4j.BasicConfigurator;
import org.apache.log4j.Logger;
import org.newsclub.net.unix.AFUNIXSocketAddress;

import com.igeekinc.indelible.indeliblefs.CreateFileInfo;
import com.igeekinc.indelible.indeliblefs.IndelibleDirectoryNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSServer;
import com.igeekinc.indelible.indeliblefs.IndelibleFSVolumeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleServerConnectionIF;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverReceiver;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSession;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSource;
import com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSClient;
import com.igeekinc.indelible.indeliblefs.firehose.IndelibleFSFirehoseClient;
import com.igeekinc.indelible.indeliblefs.iscsi.local.IndelibleBlockDevice;
import com.igeekinc.indelible.indeliblefs.iscsi.local.IndelibleBlockDeviceManager;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSForkRemoteInputStream;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSForkRemoteOutputStream;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthentication;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationClient;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationServer;
import com.igeekinc.indelible.indeliblefs.uniblock.CASIDDataDescriptor;
import com.igeekinc.indelible.oid.GeneratorID;
import com.igeekinc.indelible.oid.GeneratorIDFactory;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.indelible.server.IndelibleServerPreferences;
import com.igeekinc.testutils.TestFilesTool;
import com.igeekinc.util.BitTwiddle;
import com.igeekinc.util.FilePath;
import com.igeekinc.util.MonitoredProperties;
import com.igeekinc.util.SHA1HashID;

public class IndelibleFSISCSIBasicTest extends TestCase 
{
    private static final int kPageSize = 4096;
	private static final int kMinBlockSize = 512;
	private static final int kReadSize = 131072;
	private static final long kDeviceSize = 100L*1024L*1024L;
    private static final long kMaxDeviceSpace = 5L*kDeviceSize;
    private static final long kMaxLogSpace = 5L*kDeviceSize;
	private IndelibleFSServer fsServer;
    private IndelibleFSVolumeIF testVolume;
    private IndelibleDirectoryNodeIF root;
    private IndelibleServerConnectionIF connection;
    private DataMoverSession moverSession;
    private static boolean dataMoverInitialized = false;
    Logger logger = Logger.getLogger(getClass());
    
    public void setUp()
    throws Exception
    {
        BasicConfigurator.configure();
        IndelibleServerPreferences.initPreferences(null);

        MonitoredProperties serverProperties = IndelibleServerPreferences.getProperties();
        File preferencesDir = new File(serverProperties.getProperty(IndelibleServerPreferences.kPreferencesDirPropertyName));
        File securityClientKeystoreFile = new File(preferencesDir, IndelibleFSClient.kIndelibleEntityAuthenticationClientConfigFileName);
        EntityAuthenticationClient.initializeEntityAuthenticationClient(securityClientKeystoreFile, null, serverProperties);
        EntityAuthenticationClient.startSearchForServers();
        EntityAuthenticationServer [] securityServers = new EntityAuthenticationServer[0];
        while(securityServers.length == 0)
        {
            securityServers = EntityAuthenticationClient.listEntityAuthenticationServers();
            if (securityServers.length == 0)
                Thread.sleep(1000);
        }
        
        EntityAuthenticationServer securityServer = securityServers[0];
        
        EntityAuthenticationClient.getEntityAuthenticationClient().trustServer(securityServer);
        IndelibleFSClient.start(null, serverProperties);
        IndelibleFSServer[] servers = new IndelibleFSServer[0];
        
        while(servers.length == 0)
        {
            servers = IndelibleFSClient.listServers();
            if (servers.length == 0)
                Thread.sleep(1000);
        }
        fsServer = servers[0];
        
        if (!dataMoverInitialized)
        {
            GeneratorIDFactory genIDFactory = new GeneratorIDFactory();
            GeneratorID testBaseID = genIDFactory.createGeneratorID();
            ObjectIDFactory oidFactory = new ObjectIDFactory(testBaseID);
            String moverPortStr = serverProperties.getProperty(IndelibleServerPreferences.kMoverPortPropertyName);
            int moverPort = Integer.parseInt(moverPortStr);
            
            String localPortDirStr = serverProperties.getProperty(IndelibleServerPreferences.kLocalPortDirectory);
            File localPortDir = new File(localPortDirStr);
            if (localPortDir.exists() && !localPortDir.isDirectory())
            {
            	localPortDir.delete();
            }
            if (!localPortDir.exists())
            {
            	localPortDir.mkdirs();
            }
            File localPortSocketFile = new File(localPortDir, "dataMover");
            // Should have the CAS server and the entity authentication server and client configured by this point
            DataMoverReceiver.init(oidFactory);
            DataMoverSource.init(oidFactory, new InetSocketAddress(moverPort),
            		new AFUNIXSocketAddress(localPortSocketFile, moverPort));   // TODO - move this someplace logical
            dataMoverInitialized = true;
        }
        connection = fsServer.open();
        connection.startTransaction();
        testVolume = connection.createVolume(null);
        connection.commit();
        EntityAuthentication serverID = connection.getClientEntityAuthentication();
        moverSession = DataMoverSource.getDataMoverSource().createDataMoverSession(securityServer.getEntityID());
        moverSession.addAuthorizedClient(serverID);
        root = testVolume.getRoot();
    }

    @Override
    protected void tearDown() throws Exception
    {
        connection.close();
        super.tearDown();
    }
    
    public void testReadWriteDevice()
    throws Exception
    {
    	String name = "backingFile.img";
    	CreateFileInfo testInfo = root.createChildFile(name, true);
    	IndelibleFileNodeIF curTestFile = testInfo.getCreatedNode();
    	root = testInfo.getDirectoryNode();
    	IndelibleFSForkIF testDataFork = curTestFile.getFork("data", true);
    	IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);
    	SHA1HashID hashID = TestFilesTool.writeTestDataToOutputStream(forkOutputStream, kDeviceSize);
    	forkOutputStream.close();
    	IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
    	assertTrue(TestFilesTool.verifyInputStream(forkInputStream, kDeviceSize, hashID));
    	forkInputStream.close();

    	File iscsiTestDir=new File("/tmp/iscsitest");
    	if (iscsiTestDir.exists())
    		TestFilesTool.deleteTree(iscsiTestDir);
    	iscsiTestDir.mkdir();
    	File stableDeviceDirectory = new File(iscsiTestDir, "stableDeviceDir");
    	if (stableDeviceDirectory.exists())
    		TestFilesTool.deleteTree(stableDeviceDirectory);
    	stableDeviceDirectory.mkdir();
    	File logDirectory = new File(iscsiTestDir, "logDir");
    	if (logDirectory.exists())
    		TestFilesTool.deleteTree(logDirectory);
    	logDirectory.mkdir();
    	IndelibleBlockDeviceManager deviceManager = new IndelibleBlockDeviceManager(stableDeviceDirectory, kMaxDeviceSpace, logDirectory, kMaxLogSpace, connection);

    	try
    	{
    		IndelibleBlockDevice testDevice = deviceManager.getBlockDevice(testVolume, FilePath.getFilePath("/"+name));

    		SHA1HashID checkHash = new SHA1HashID();
    		byte [] readBytes = new byte[kReadSize];
    		for (long offset = 0; offset < kDeviceSize; offset+= 131072)
    		{
    			testDevice.read(offset, ByteBuffer.wrap(readBytes));
    			checkHash.update(readBytes);
    		}

    		checkHash.finalizeHash();
    		boolean bytesComparedOK = true;
    		if (!hashID.equals(checkHash))
    		{
    			// OK, compare block by block and see where we're going wrong
    			byte [] deviceBytes = new byte[kPageSize];
    			byte [] fileBytes;
    			for (long offset = 0; offset < kDeviceSize; offset+= kPageSize)
    			{
    				testDevice.read(offset, ByteBuffer.wrap(deviceBytes));
    				CASIDDataDescriptor dataDescriptor = testDataFork.getDataDescriptor(offset, kPageSize);
    				fileBytes = dataDescriptor.getData();
    				if (fileBytes.length != deviceBytes.length)
    				{
    					logger.error("Got back "+fileBytes.length+" bytes from the file and "+deviceBytes.length+" from the device at offset "+offset);
    					bytesComparedOK = false;
    				}
    				else
    				{
    					if (!Arrays.equals(deviceBytes, fileBytes))
    					{
    						logger.error("deviceBytes != fileBytes at offset "+offset);
    						bytesComparedOK = false;
    					}
    				}
    			}
    			assertTrue(bytesComparedOK);
    			assertTrue(hashID.equals(checkHash));
    		}


    		byte [] writeBytes = new byte[kPageSize];
    		for (long offset = 0; offset < kDeviceSize; offset+= kPageSize)
    		{
    			int blockNum = (int)(offset/kPageSize);
    			for (int bufOffset = 0; bufOffset < kPageSize; bufOffset += 8)
    			{
    				BitTwiddle.intToByteArray(blockNum, writeBytes, bufOffset, BitTwiddle.kLittleEndian);
    				BitTwiddle.intToByteArray(bufOffset, writeBytes, bufOffset + 4, BitTwiddle.kLittleEndian);
    			}
    			testDevice.write(offset, ByteBuffer.wrap(writeBytes));
    		}

    		byte [] checkBytes = new byte[kPageSize];
    		byte [] readBytes2 = new byte[kPageSize];
    		for (long offset = 0; offset < kDeviceSize; offset+= kPageSize)
    		{
    			int blockNum = (int)(offset/kPageSize);
    			for (int bufOffset = 0; bufOffset < kPageSize; bufOffset += 8)
    			{
    				BitTwiddle.intToByteArray(blockNum, checkBytes, bufOffset, BitTwiddle.kLittleEndian);
    				BitTwiddle.intToByteArray(bufOffset, checkBytes, bufOffset + 4, BitTwiddle.kLittleEndian);
    			}
    			testDevice.read(offset, ByteBuffer.wrap(readBytes2));
    			if (!Arrays.equals(checkBytes, readBytes2))
    			{
    				logger.error("checkBytes != readBytes2 at offset "+offset);
    				bytesComparedOK = false;
    			}
    		}
    		assertTrue(bytesComparedOK);
    	}
    	finally
    	{
    		deviceManager.shutdown();
    	}
    }
    
    public void testNonAlignedReadWrite()
    throws Exception
    {
    	String name = "backingFile.img";
    	CreateFileInfo testInfo = root.createChildFile(name, true);
    	IndelibleFileNodeIF curTestFile = testInfo.getCreatedNode();
    	root = testInfo.getDirectoryNode();
    	IndelibleFSForkIF testDataFork = curTestFile.getFork("data", true);
    	IndelibleFSForkRemoteOutputStream forkOutputStream = new IndelibleFSForkRemoteOutputStream(testDataFork, false, moverSession);

    	int generation = 1;
    	byte [] bufferBlock = new byte[kReadSize];
    	for (long offset = 0; offset < kDeviceSize; offset += kReadSize)
    	{
    		for (long bufOffset = 0; bufOffset < kReadSize; offset += kMinBlockSize )
    		{
    			generateBlockForBlockNumAndGeneration(bufferBlock, (int)bufOffset, offset + bufOffset, generation);
    		}
    		forkOutputStream.write(bufferBlock);
    	}
    	forkOutputStream.close();
    	IndelibleFSForkRemoteInputStream forkInputStream = new IndelibleFSForkRemoteInputStream(testDataFork);
    	for (long offset = 0; offset < kDeviceSize; offset += kMinBlockSize)
    	{
    		forkInputStream.read(bufferBlock);
    		assertTrue(checkBlockForBlockNumAndGeneration(bufferBlock, 0, offset, generation));
    	}
    	forkInputStream.close();

    	File iscsiTestDir=new File("/tmp/iscsitest");
    	if (iscsiTestDir.exists())
    		TestFilesTool.deleteTree(iscsiTestDir);
    	iscsiTestDir.mkdir();
    	File stableDeviceDirectory = new File(iscsiTestDir, "stableDeviceDir");
    	if (stableDeviceDirectory.exists())
    		TestFilesTool.deleteTree(stableDeviceDirectory);
    	stableDeviceDirectory.mkdir();
    	File logDirectory = new File(iscsiTestDir, "logDir");
    	if (logDirectory.exists())
    		TestFilesTool.deleteTree(logDirectory);
    	logDirectory.mkdir();
    	IndelibleBlockDeviceManager deviceManager = new IndelibleBlockDeviceManager(stableDeviceDirectory, kMaxDeviceSpace, logDirectory, kMaxLogSpace, connection);

    	try
    	{
    		IndelibleBlockDevice testDevice = deviceManager.getBlockDevice(testVolume, FilePath.getFilePath("/"+name));

    		byte [] readBytes = new byte[kPageSize];
    		for (long offset = 0; offset < kDeviceSize; offset+= kPageSize)
    		{
    			testDevice.read(offset, ByteBuffer.wrap(readBytes));
    			for (int bufferOffset = 0; bufferOffset < kPageSize; bufferOffset += kMinBlockSize)
    				assertTrue(checkBlockForBlockNumAndGeneration(readBytes, bufferOffset, offset + bufferOffset, generation));
    		}

    		// Now, try the standard linux offset of 63  * 512 bytes
    		for (long offset = 63 * 512; offset < kDeviceSize; offset+= kPageSize)
    		{
    			testDevice.read(offset, ByteBuffer.wrap(readBytes));
    			int bytesToCheck = kPageSize;
    			if (offset + kPageSize > kDeviceSize)
    				bytesToCheck = (int)(kDeviceSize - offset);
    			for (int bufferOffset = 0; bufferOffset < bytesToCheck; bufferOffset += kMinBlockSize)
    				assertTrue(checkBlockForBlockNumAndGeneration(readBytes, bufferOffset, offset + bufferOffset, generation));
    		}

    		// Now, rewrite at the 512 byte level

    		generation++;
    		byte [] writeBytes = new byte[kMinBlockSize];
    		for (long offset = 0; offset < kDeviceSize; offset+= kMinBlockSize)
    		{
    			generateBlockForBlockNumAndGeneration(writeBytes, 0, offset, generation);
    			testDevice.write(offset, ByteBuffer.wrap(writeBytes));
    		}

    		for (long offset = 0; offset < kDeviceSize; offset+= kMinBlockSize)
    		{
    			testDevice.read(offset, ByteBuffer.wrap(readBytes));
    			assertTrue(checkBlockForBlockNumAndGeneration(readBytes, 0, offset, generation));
    		}

    		// Rewrite with 4K blocks offset by 63 x 512

    		generation++;
    		writeBytes = new byte[kPageSize];
    		readBytes = new byte[kPageSize];
    		for (long offset = 63 * 512; offset < kDeviceSize; offset+= kPageSize)
    		{
    			int bytesToWrite = kPageSize;
    			if (offset + kPageSize > kDeviceSize)
    				bytesToWrite = (int)(kDeviceSize - offset);
    			for (int bufferOffset = 0; bufferOffset < bytesToWrite; bufferOffset += kMinBlockSize)
    				generateBlockForBlockNumAndGeneration(writeBytes, bufferOffset, offset + bufferOffset, generation);
    			testDevice.write(offset, ByteBuffer.wrap(writeBytes));

    			testDevice.read(offset, ByteBuffer.wrap(readBytes));
    			int bytesToCheck = kPageSize;
    			if (offset + kPageSize > kDeviceSize)
    				bytesToCheck = (int)(kDeviceSize - offset);
    			for (int bufferOffset = 0; bufferOffset < bytesToCheck; bufferOffset += kMinBlockSize)
    				assertTrue(checkBlockForBlockNumAndGeneration(readBytes, bufferOffset, offset + bufferOffset, generation));
    		}


    		for (long offset = 63 * 512; offset < kDeviceSize; offset+= kPageSize)
    		{
    			testDevice.read(offset, ByteBuffer.wrap(readBytes));
    			int bytesToCheck = kPageSize;
    			if (offset + kPageSize > kDeviceSize)
    				bytesToCheck = (int)(kDeviceSize - offset);
    			for (int bufferOffset = 0; bufferOffset < bytesToCheck; bufferOffset += kMinBlockSize)
    				assertTrue(checkBlockForBlockNumAndGeneration(readBytes, bufferOffset, offset + bufferOffset, generation));
    		}
    	}
    	finally
    	{
    		deviceManager.shutdown();
    	}
    }

	public void generateBlockForBlockNumAndGeneration(byte[] bufferBlock, int bufferOffset, long offset, int generation) 
	{
		int blockNum = (int)(offset / kMinBlockSize);
		int writeNum = (generation << 20) + blockNum;
		String writeStr = BitTwiddle.toHexString(writeNum, 8);
		byte [] writeBytes = writeStr.getBytes(Charset.forName("UTF-8"));
		for (int blockOffset = bufferOffset; blockOffset < bufferOffset + kMinBlockSize; blockOffset += 8)
		{
			System.arraycopy(writeBytes, 0, bufferBlock, blockOffset, 8);
		}
	}
	
	public boolean checkBlockForBlockNumAndGeneration(byte[] bufferBlock, int bufferOffset, long offset, int generation)
	{
		int blockNum = (int)(offset / kMinBlockSize);
		int checkNum = (generation << 20) + blockNum;
		String checkStr = BitTwiddle.toHexString(checkNum, 8);
		byte [] checkBytes = checkStr.getBytes(Charset.forName("UTF-8"));
		for (int blockOffset = bufferOffset; blockOffset < bufferOffset + kMinBlockSize; blockOffset += 8)
		{
			if (checkBytes[0] != bufferBlock[blockOffset] || checkBytes[1] != bufferBlock[blockOffset + 1] || checkBytes[2] != bufferBlock[blockOffset + 2] || checkBytes[3] != bufferBlock[blockOffset + 3]
					|| checkBytes[4] != bufferBlock[blockOffset + 4] || checkBytes[5] != bufferBlock[blockOffset + 5] || checkBytes[6] != bufferBlock[blockOffset + 6] || checkBytes[7] != bufferBlock[blockOffset + 7])
			{
				String badStr = new String(bufferBlock, blockOffset, 8);
				System.out.println("Discrepancy at offset "+offset+", bufferOffset = "+bufferOffset+" expected "+checkStr+" got "+badStr);
				String bufferStr = new String(bufferBlock);
				System.out.println("Complete bad buffer = "+bufferStr);
				return false;
			}
		}
		return true;
	}
}
