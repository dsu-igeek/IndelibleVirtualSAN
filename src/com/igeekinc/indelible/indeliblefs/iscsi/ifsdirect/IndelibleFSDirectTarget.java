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
import com.igeekinc.indelible.indeliblefs.exceptions.ForkNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.ObjectNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.exceptions.VolumeNotFoundException;
import com.igeekinc.indelible.indeliblefs.iscsi.IndelibleFSTarget;
import com.igeekinc.indelible.indeliblefs.iscsi.IndelibleFSTargetInfo;
import com.igeekinc.indelible.indeliblefs.iscsi.IndelibleiSCSIMgmtServlet;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSVolumeRemote;
import com.igeekinc.indelible.indeliblefs.security.AuthenticationFailureException;
import com.igeekinc.indelible.indeliblefs.webaccess.IndelibleFSUtilsServlet;
import com.igeekinc.util.FilePath;
import com.igeekinc.util.logging.ErrorLogMessage;


/**
 * The central class of the jSCSI Target, which keeps track of all active
 * {@link TargetSession}s, stores target-wide parameters and variables, and
 * which contains the {@link #main(String[])} method for starting the program.
 * 
 * @author Andreas Ergenzinger
 */
public class IndelibleFSDirectTarget extends IndelibleFSTarget
{
    public static IndelibleFSDirectTarget indelibleFSTarget;
    
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
            indelibleFSTarget = new IndelibleFSDirectTarget();
            indelibleFSTarget.run(args);
        }
        catch (Throwable t)
        {
            t.printStackTrace();
            System.exit(1);
        }
        System.exit(0);
    }
    
    public IndelibleFSDirectTarget() throws UnrecoverableKeyException, InvalidKeyException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IllegalStateException, NoSuchProviderException, SignatureException, IOException, AuthenticationFailureException, InterruptedException
    {
        super();
    }
    
    public void runApp() throws UnrecoverableKeyException, InvalidKeyException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IllegalStateException, NoSuchProviderException, SignatureException, IOException, AuthenticationFailureException, InterruptedException, PermissionDeniedException
    {
    	Logger.getRootLogger().setLevel(Level.WARN);

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
                IndelibleFSDirectStorageModule indelibleFSStorageModule = exportIndelibleFSFile(
                        exportVolume, exportFilePath, targetName, targetAlias);
            
                // print configuration and medium details
                System.out.println("   target name:    " + targetName);

                System.out.println("   storage file:   " + exportFilePath);
                System.out.println("   file size:      "
                        + indelibleFSStorageModule.getHumanFriendlyMediumSize());
                
            } catch (FileNotFoundException e) {
                LOGGER.fatal(e.toString());
                return;
            } catch (ObjectNotFoundException e)
            {
                LOGGER.fatal(e.toString());
                return;
            } catch (PermissionDeniedException e)
            {
                LOGGER.fatal(e.toString());
                return;
            } catch (RemoteException e)
            {
                LOGGER.fatal(e.toString());
                return;
            } catch (ForkNotFoundException e)
            {
                LOGGER.fatal(e.toString());
                return;
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
        ServletHolder servletHolder = new ServletHolder(iscsiMgmtServlet);
        context.addServlet(servletHolder, "/iscsi/*");
        try
        {
            server.start();
        } catch (Exception e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        }
        
        targetServer.mainLoop();
    }

    @Override
    public IndelibleFSDirectStorageModule exportIndelibleFSFile(
            IndelibleFSVolumeIF exportVolume, FilePath exportFilePath,
            String targetName, String targetAlias)
            throws ObjectNotFoundException, PermissionDeniedException,
            RemoteException, IOException, ForkNotFoundException
    {
        IndelibleFSDirectStorageModule indelibleFSStorageModule = IndelibleFSDirectStorageModule.open(exportVolume, exportFilePath);
        targetServer.addStorageModule(targetName, targetAlias, indelibleFSStorageModule);
        IndelibleFSTargetInfo targetInfo = new IndelibleFSTargetInfo(targetName, targetAlias, 
                exportVolume.getObjectID().toString(), exportFilePath.toString());
        return indelibleFSStorageModule;
    }
}
