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
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.charset.Charset;
import java.rmi.RemoteException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import javax.xml.parsers.ParserConfigurationException;

import org.apache.log4j.DailyRollingFileAppender;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.PropertyConfigurator;
import org.jscsi.target.Target;
import org.jscsi.target.TargetServer;
import org.jscsi.target.settings.TargetConfiguration;
import org.jscsi.target.storage.AbstractStorageModule;
import org.xml.sax.SAXException;

import com.igeekinc.indelible.indeliblefs.CreateDirectoryInfo;
import com.igeekinc.indelible.indeliblefs.CreateFileInfo;
import com.igeekinc.indelible.indeliblefs.IndelibleDirectoryNodeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFSVolumeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.exceptions.FileExistsException;
import com.igeekinc.indelible.indeliblefs.exceptions.ForkNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.ObjectNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.exceptions.VolumeNotFoundException;
import com.igeekinc.indelible.indeliblefs.iscsi.ifsdirect.IndelibleFSDirectTarget;
import com.igeekinc.indelible.indeliblefs.iscsi.local.IndelibleVSANPreferences;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSForkRemoteOutputStream;
import com.igeekinc.indelible.indeliblefs.security.AuthenticationFailureException;
import com.igeekinc.indelible.indeliblefs.utilities.IndelibleFSUtilBase;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.util.FilePath;
import com.igeekinc.util.MonitoredProperties;
import com.igeekinc.util.SystemInfo;
import com.igeekinc.util.logging.ErrorLogMessage;

public abstract class IndelibleFSTarget extends IndelibleFSUtilBase 
{

	protected static final Logger LOGGER = Logger.getLogger(IndelibleFSDirectTarget.class);
	/**
	 * The name of the <i>log4j</i> properties file.
	 * 
	 * @see #readLog4jConfigurationFile()
	 */
	private static final String LOG4J_PROPERTIES_XML = "log4j.xml";
	/**
	 * The relative path to <code>src/main/resources/</code>. The
	 * {@link #LOG4J_PROPERTIES_XML} file may be located there.
	 * 
	 * @see #readLog4jConfigurationFile()
	 */
	private static final String RESOURCES_DIRECTORY = "src/main/resources/";
	protected TargetServer targetServer;
	IndelibleFSVolumeIF configurationVolume = null;
	IndelibleDirectoryNodeIF configDir = null;
	public static final String kIndelibleFSTargetConfigurationVolumeMDName = "com.igeekinc.indelible.iscsi.configuration";
	public static final String kIndelibleFSTargetMACAddrPropertyName = "com.igeekinc.indelible.iscsi.macaddr";
	public static final String kVerboseLogFileLevelPropertyName = "com.igeekinc.indelible.indeliblefs.iscsi.verboseLogLevel";

	protected DailyRollingFileAppender rollingLog;
	protected MonitoredProperties targetProperties;
    public IndelibleFSTarget() throws UnrecoverableKeyException, InvalidKeyException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IllegalStateException, NoSuchProviderException, SignatureException, IOException, AuthenticationFailureException, InterruptedException
    {
        super();
        targetProperties = setupProperties();
    }
    
	public MonitoredProperties setupProperties() throws IOException
	{
		if (IndelibleVSANPreferences.getProperties() == null)
			IndelibleVSANPreferences.initPreferences();

        MonitoredProperties clientProperties = IndelibleVSANPreferences.getProperties();
		return clientProperties;
	}
	/**
	 * Reads target settings from configuration file and stores them in the
	 * {@link #config} object. Returns <code>false</code> if the operation could
	 * not be completed successfully, else it returns <code>true</code>.
	 * 
	 * @return <code>true</code> if the target settings were read from the
	 *         configuration file, <code>false</code> otherwise. {@see
	 *         TargetConfiguration}
	 */
	public boolean readConfig() 
	{
	
	    try
	    {
	        IndelibleFSObjectID [] volumeIDs = connection.listVolumes();
	        for (IndelibleFSObjectID curVolumeID:volumeIDs)
	        {
	            IndelibleFSVolumeIF retrieveVolume = connection.retrieveVolume(curVolumeID);
	            try
	            {
	                Map<String, Object> metaDataResource = retrieveVolume.getMetaDataResource(kIndelibleFSTargetConfigurationVolumeMDName);
	                if (metaDataResource != null)
	                {
	                    // Later we may actually put something in the metadata resource, but for now its existence is our queue
	                    configurationVolume = retrieveVolume;
	                    break;
	                }
	            } catch (PermissionDeniedException e)
	            {
	                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
	            } catch (IOException e)
	            {
	                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
	            }
	        }
	        /**
	         * No configuration volume!
	         */
	        if (configurationVolume == null)
	        {
	            connection.startTransaction();
	    		Properties volumeProperties = new Properties();
	            volumeProperties.put(IndelibleFSVolumeIF.kVolumeNamePropertyName, "Indelible Virtual SAN Configuration");
	            configurationVolume = connection.createVolume(volumeProperties);
	            HashMap<String, Object>metaDataResource = new HashMap<String, Object>();
	            metaDataResource.put("Dunsel", "placeholder");
	            configurationVolume.setMetaDataResource(kIndelibleFSTargetConfigurationVolumeMDName, metaDataResource);
	            connection.commit();
	        }
	        IndelibleDirectoryNodeIF root = configurationVolume.getRoot();
	        
	        try
			{
				configDir = (IndelibleDirectoryNodeIF) root.getChildNode("IndelibleFSTargetConfiguration");
			} catch (ObjectNotFoundException e)
			{
				// OK, we'll just make it then!
	            connection.startTransaction();
	            CreateDirectoryInfo configDirInfo = root.createChildDirectory("IndelibleFSTargetConfiguration");
	            connection.commit();
	            configDir = configDirInfo.getCreatedNode();
	        }
	        
	    } catch (RemoteException e1)
	    {
	        Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e1);
	        return false;
	    } catch (VolumeNotFoundException e)
	    {
	        Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
	        return false;
	    } catch (PermissionDeniedException e)
	    {
	        Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
	        return false;
	    } catch (IOException e)
	    {
	        Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
	        return false;
	    } catch (FileExistsException e)
	    {
	        Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
	        return false;
	    }
	    if (configDir != null)
	    {
	        try {
	            String configFileName = getConfigFileName();
	            IndelibleFileNodeIF configFileNode = null;
				try
				{
					configFileNode = configDir.getChildNode(configFileName);
	                targetServer.setConfig(new IndelibleFSTargetConfigurationXMLParser().parseSettings(configFileNode));
	                return true;
				} catch (ObjectNotFoundException e)
				{
					// No config file, not a big deal
				}
	        } catch (SAXException e) {
	            LOGGER.fatal(e);
	        } catch (ParserConfigurationException e) {
	            LOGGER.fatal(e);
	        } catch (IOException e) {
	            LOGGER.fatal(e);
	        } catch (ForkNotFoundException e)
	        {
	        	LOGGER.fatal(e);
	        } catch (PermissionDeniedException e)
			{
	        	LOGGER.fatal(e);
			}
	    }
	
	    // Fall through to here - just make a default configuration with no targets
	    TargetConfiguration targetConfiguration;
	    try
	    {
	        targetConfiguration = new TargetConfiguration();
	        //targetConfiguration.setPort(targetProperties.);
	        targetServer.setConfig(targetConfiguration);
	        return true;
	    } catch (IOException e)
	    {
	        Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
	    }
	    return false;
	    
	}

	public String getConfigFileName() {
	    String ethernetID = SystemInfo.getSystemInfo().getEthernetID().toString();
	    String configFileName = "iSCSITargetConfig-"+ethernetID+".xml";
	    return configFileName;
	}

	public void writeConfig() throws RemoteException, IOException,
			PermissionDeniedException, FileExistsException,
			ForkNotFoundException {
			    if (configDir != null)
			    {
			        connection.startTransaction();
			
			        try
			        {
			            String configFileName = getConfigFileName();
			            IndelibleFileNodeIF configFileNode = null;
			            try
			            {
			            	configFileNode = configDir.getChildNode(configFileName);
			            }
			            catch (ObjectNotFoundException e)
			            {
			                CreateFileInfo configFileCreateInfo = configDir.createChildFile(configFileName, true);
			                configFileNode = configFileCreateInfo.getCreatedNode();
			            }
			            IndelibleFSForkIF writeFork = configFileNode.getFork("data", true);
			            writeFork.truncate(0);
			            IndelibleFSForkRemoteOutputStream writeStream = new IndelibleFSForkRemoteOutputStream(writeFork, false, moverSession);
			            IndelibleFSTargetConfigurationXMLSerializer serializer = new IndelibleFSTargetConfigurationXMLSerializer();
			            writeStream.write("<?xml version=\"1.0\" encoding=\"UTF-8\" ?>\n".getBytes(Charset.forName("UTF-8")));
			            
			            TargetConfiguration writeConfiguration = new TargetConfiguration();
			            writeConfiguration.setAllowSloppyNegotiation(targetServer.getConfig().getAllowSloppyNegotiation());
			            writeConfiguration.setPort(targetServer.getConfig().getPort());
			            String [] targetNames = targetServer.getTargetNames();
			            for(String curTargetName:targetNames)
			            {
			                Target target = targetServer.getTarget(curTargetName);
			                writeConfiguration.addTargetInfo(new IndelibleFSTargetInfo(target.getTargetName(), target.getTargetAlias(), 
			                        ((IndelibleFSStorageModule)target.getStorageModule()).getVolumeID().toString(),
			                        ((IndelibleFSStorageModule)target.getStorageModule()).getStoragePath().toString()));
			            }
			            serializer.serializeToOutputStream(writeStream, writeConfiguration);
			
			            writeStream.close();
			            connection.commit();
			        }
			        catch (Throwable t)
			        {
			            t.printStackTrace();
			            connection.rollback();
			        }
			    }
			}

	public TargetServer getTargetServer() {
	    return targetServer;
	}

    public abstract AbstractStorageModule exportIndelibleFSFile(
            IndelibleFSVolumeIF exportVolume, FilePath exportFilePath,
            String targetName, String targetAlias)
            throws ObjectNotFoundException, PermissionDeniedException,
            RemoteException, IOException, ForkNotFoundException;

	public boolean unexport(String unExportTargetName) 
	{
	    return targetServer.removeStorageModule(unExportTargetName);
	}
	
	@Override
    public void setupLogging(MonitoredProperties properties)
    {
    	Logger.getRootLogger().setLevel(Level.ERROR);
        Properties loggingProperties = new Properties();
        loggingProperties.putAll(properties);
        File additionalLoggingConfigFile = new File(properties.getProperty(IndelibleVSANPreferences.kPreferencesDirPropertyName),
        "indelibleVSANOptions.properties"); //$NON-NLS-1$
        Exception savedException = null;
        try
        {
            if (additionalLoggingConfigFile.exists())
            {
                Properties additionalLoggingProperties = new Properties();
                FileInputStream additionalLoggingInStream = new FileInputStream(additionalLoggingConfigFile);
                additionalLoggingProperties.load(additionalLoggingInStream);
                loggingProperties.putAll(additionalLoggingProperties);
            }	
        }
        catch (Exception e)
        {
            savedException = e;
        }
        Logger.getRootLogger().removeAllAppenders();	// Clean up anything lying around
        PropertyConfigurator.configure(loggingProperties);
        rollingLog = new DailyRollingFileAppender();
    
        File logDir = new File(getLogFileDir()); //$NON-NLS-1$
        logDir.mkdirs();
        File logFile = new File(logDir, getServerLogFileName()); //$NON-NLS-1$
        System.out.println("Server log file = "+logFile.getAbsolutePath());
       /* String logFileEncoding = VendorProperties.getLogFileEncoding();
    	if (logFile.exists() && logFileEncoding.toLowerCase().equals("utf-16") && logFile.length() >= 2)
    	{
    		SimpleDateFormat checkFormatter = new SimpleDateFormat("yyyy-MM-dd");
    		if (checkFormatter.format(new Date()).equals(checkFormatter.format(new Date(logFile.lastModified()))))	// We'll be writing to the same file
    		{
    			// Check the BOM
    			try {
    				InputStream checkStream = new FileInputStream(logFile);
    				int bom0 = checkStream.read();
    				int bom1 = checkStream.read();
    				if (bom0 == 0xfe && bom1 == 0xff)
    					logFileEncoding = "utf-16be";
    				else
    					logFileEncoding = "utf-16le";
    			} catch (FileNotFoundException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			} catch (IOException e) {
    				// TODO Auto-generated catch block
    				e.printStackTrace();
    			}
    		}
    	}
        rollingLog.setEncoding(logFileEncoding);*/
        rollingLog.setFile(logFile.getAbsolutePath());
        rollingLog.setDatePattern("'.'yyyy-MM-dd"); //$NON-NLS-1$
        setLogFileLevelFromPrefs();
    
        rollingLog.activateOptions();
        //rollingLog.setLayout(new XMLLayout());
        rollingLog.setLayout(new PatternLayout("%d %-5p [%t]: %m%n")); //$NON-NLS-1$
        Logger.getRootLogger().addAppender(rollingLog);
        
        // Disable the timing logger for now
    	Logger timingLogger = Logger.getLogger("org.perf4j.TimingLogger");
    	timingLogger.setLevel(Level.FATAL);
    	timingLogger.setAdditivity(false);
    }
    
	public void setLogFileLevelFromPrefs()
    {
		Level level = Level.toLevel(targetProperties.getProperty(kVerboseLogFileLevelPropertyName, "INFO"), Level.INFO);
		rollingLog.setThreshold(level); //$NON-NLS-1$
		Logger.getRootLogger().setLevel(level);
		logger.debug("Set debug level to "+level);
    }

    public String getLogFileDir()
    {
        return targetProperties.getProperty(IndelibleVSANPreferences.kLogFileDirectoryPropertyName);
    }
    
    public String getServerLogFileName()
    {
        return "indelibleVirtualSAN.log";
    }
}
