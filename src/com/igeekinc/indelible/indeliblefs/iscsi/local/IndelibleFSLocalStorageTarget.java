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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.rmi.RemoteException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.ContextHandlerCollection;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.jscsi.target.TargetServer;
import org.jscsi.target.connection.TargetSession;
import org.jscsi.target.settings.TargetConfigurationXMLParser;
import org.jscsi.target.settings.TargetInfo;

import com.igeekinc.indelible.indeliblefs.IndelibleFSVolumeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleServerConnectionIF;
import com.igeekinc.indelible.indeliblefs.exceptions.ForkNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.ObjectNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.exceptions.VolumeNotFoundException;
import com.igeekinc.indelible.indeliblefs.iscsi.IndelibleFSTarget;
import com.igeekinc.indelible.indeliblefs.iscsi.IndelibleFSTargetInfo;
import com.igeekinc.indelible.indeliblefs.iscsi.IndelibleTargetPerfServlet;
import com.igeekinc.indelible.indeliblefs.iscsi.IndelibleiSCSIMgmtServlet;
import com.igeekinc.indelible.indeliblefs.iscsi.assets.AssetsServlet;
import com.igeekinc.indelible.indeliblefs.security.AuthenticationFailureException;
import com.igeekinc.indelible.indeliblefs.webaccess.IndelibleFSUtilsServlet;
import com.igeekinc.indelible.server.IndelibleServerPreferences;
import com.igeekinc.util.FilePath;
import com.igeekinc.util.MonitoredProperties;
import com.igeekinc.util.SystemInfo;
import com.igeekinc.util.logging.ErrorLogMessage;

/**
 * The Indelible VSAN storage target
 */
public class IndelibleFSLocalStorageTarget extends IndelibleFSTarget
{
    public static IndelibleFSLocalStorageTarget indelibleFSTarget;
    
    private IndelibleBlockDeviceManager blockDeviceManager;
    private MonitoredProperties targetProperties;
    /**
     * Starts the jSCSI target.
     * 
     * @param args
     *            all command line arguments are ignored
     */
    public static void main(String[] args) 
    {
        try
        {
            indelibleFSTarget = new IndelibleFSLocalStorageTarget();
            indelibleFSTarget.run(args);
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
    
    public IndelibleFSLocalStorageTarget() throws UnrecoverableKeyException, InvalidKeyException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IllegalStateException, NoSuchProviderException, SignatureException, IOException, AuthenticationFailureException, InterruptedException
    {
        super();
        targetProperties = setupProperties();
    }
    
    public void runApp() throws UnrecoverableKeyException, InvalidKeyException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IllegalStateException, NoSuchProviderException, SignatureException, IOException, AuthenticationFailureException, InterruptedException
    {
    	Logger.getRootLogger().setLevel(Level.WARN);

    	try {
			File stableDeviceDirectory = new File(targetProperties.getProperty(IndelibleVSANPreferences.kCacheDirPropertyName));
			if (!stableDeviceDirectory.exists())
				stableDeviceDirectory.mkdir();
			File logDirectory = new File(targetProperties.getProperty(IndelibleVSANPreferences.kLogFileDirectoryPropertyName));
			if (!logDirectory.exists())
				logDirectory.mkdir();
			IndelibleServerConnectionIF bdmConnection = fsServer.open();	// Use a separate connection so we don't have any contention for transactions
			blockDeviceManager = new IndelibleBlockDeviceManager(stableDeviceDirectory, 1024L*1024L*1024L*1024L, logDirectory, 50L*1024L*1024L*1024L, bdmConnection);
		} catch (IOException e2) {
			logger.fatal("Caught IOException starting IndelibleBlockDeviceManager", e2);
			return;
		}
        System.out.println("jSCSI Target");

        targetServer = new TargetServer();
        // read target settings from configuration file
        // exit if there is a problem
        if (!readConfig()) {
            LOGGER.fatal("Error while trying to read settings from "
                    + TargetConfigurationXMLParser.CONFIGURATION_FILE_NAME
                    + ".\nShutting down.");
            return;
        }
        System.out.println("   port:           " + targetServer.getConfig().getPort());
        
        for (TargetInfo curTargetInfo:targetServer.getConfig().getTargetInfo())
        {
            IndelibleFSTargetInfo curIFSTargetInfo = (IndelibleFSTargetInfo)curTargetInfo;
            IndelibleFSVolumeIF exportVolume = null;
            // open the storage medium
            try {
                try
                {
                    exportVolume = connection.retrieveVolume(curIFSTargetInfo.getStorageVolumeID());
                } catch (VolumeNotFoundException e1)
                {
                    System.err.println("Could not find volume "+curIFSTargetInfo.getStorageVolumeID());
                    System.exit(1);
                } catch (RemoteException e)
                {
                    Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
                }


                /*storageModule = SynchronizedRandomAccessStorageModule.open(config
                    .getStorageFilePath());*/
                String exportFilePathStr = curIFSTargetInfo.getFilePath();
                FilePath exportFilePath = FilePath.getFilePath(exportFilePathStr);
                String targetName = curIFSTargetInfo.getTargetName();
                String targetAlias = curIFSTargetInfo.getTargetAlias();
                IndelibleBlockDeviceStorageModule indelibleBDStorageModule = exportIndelibleFSFile(
                        exportVolume, exportFilePath, targetName, targetAlias);
            
                // print configuration and medium details
                System.out.println("   target name:    " + targetName);

                System.out.println("   storage file:   " + exportFilePath);
                System.out.println("   file size:      "
                        + indelibleBDStorageModule.getHumanFriendlyMediumSize());
                
            } catch (FileNotFoundException e) {
                LOGGER.fatal(e.toString());
            } catch (ObjectNotFoundException e)
            {
                LOGGER.fatal(e.toString());
            } catch (PermissionDeniedException e)
            {
                LOGGER.fatal(e.toString());
            } catch (RemoteException e)
            {
                LOGGER.fatal(e.toString());
                return;
            } catch (ForkNotFoundException e)
            {
                LOGGER.fatal(e.toString());
            } catch (IOException e)
            {
                LOGGER.fatal(e.toString());
                return;
            }
        }

        Server server = new Server(8090);
        
        ContextHandlerCollection contexts = new ContextHandlerCollection();
        server.setHandler(contexts);
         
        ServletContextHandler context = new ServletContextHandler(contexts, "/");
        context.addServlet(IndelibleFSUtilsServlet.class, "/ifsutils/*");
        IndelibleiSCSIMgmtServlet iscsiMgmtServlet = new IndelibleiSCSIMgmtServlet();
        iscsiMgmtServlet.setTarget(this);
        ServletHolder isciMgmtServletHolder = new ServletHolder(iscsiMgmtServlet);
        context.addServlet(isciMgmtServletHolder, "/iscsi/*");
        
        IndelibleTargetPerfServlet perfServlet = new IndelibleTargetPerfServlet();
        perfServlet.setTarget(this);
        ServletHolder perfServletHolder = new ServletHolder(perfServlet);
        context.addServlet(perfServletHolder, "/perf");
        
        context.addServlet(AssetsServlet.class, "/assets/*");
        try
        {
            server.start();
        } catch (Exception e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        }
        
        targetServer.mainLoop();
    }

    public IndelibleBlockDeviceStorageModule exportIndelibleFSFile(
            IndelibleFSVolumeIF exportVolume, FilePath exportFilePath,
            String targetName, String targetAlias)
            throws ObjectNotFoundException, PermissionDeniedException,
            RemoteException, IOException, ForkNotFoundException
    {
    	IndelibleBlockDeviceStorageModule indelibleFSStorageModule = new IndelibleBlockDeviceStorageModule(
    			blockDeviceManager.getBlockDevice(exportVolume, exportFilePath), exportFilePath);
        targetServer.addStorageModule(targetName, targetAlias, indelibleFSStorageModule);
        IndelibleFSTargetInfo targetInfo = new IndelibleFSTargetInfo(targetName, targetAlias, 
                exportVolume.getObjectID().toString(), exportFilePath.toString());
        return indelibleFSStorageModule;
    }

}
