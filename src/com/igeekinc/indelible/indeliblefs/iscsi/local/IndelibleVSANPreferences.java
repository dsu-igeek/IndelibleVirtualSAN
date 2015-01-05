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
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

import com.igeekinc.indelible.PreferencesManager;
import com.igeekinc.indelible.server.IndelibleServerPreferences;
import com.igeekinc.util.CheckCorrectDispatchThread;
import com.igeekinc.util.MonitoredProperties;
import com.igeekinc.util.SystemInfo;

public class IndelibleVSANPreferences extends PreferencesManager
{
	public static final String kPropertiesFileName = "indelible.vsan.properties"; //$NON-NLS-1$
	public static final String kPreferencesDirName = "com.igeekinc.indelible";	//$NON-NLS-1$
	public static final String kDiskImageLogFileDirectoryPropertyName = "indelible.vsan.disklogdir"; //$NON-NLS-1$
	public static final String kLogFileDirectoryPropertyName = "ndelible.vsan.LogFileDirectory";	//$NON-NLS-1$
	public static final String kCacheDirPropertyName = "indelible.vsan.cachedir";	//$NON-NLS-1$
	public static final String kTargetPortNumberPropertyName = "indelible.vsan.targetPortNumber";	//$NON-NLS-1$
	public static final String kLogDirName = "indelibleFSLogs";	//$NON-NLS-1$
	public static final String kDiskImageDirName = "indelible-vsan-disk-logs";	//$NON-NLS-1$
    public static final String kCacheDirName = "indelible-vsan-cache";	//$NON-NLS-1$
    public static void initPreferences() throws IOException
    {
    	new IndelibleVSANPreferences(null);	// This will automatically hook itself to the singleton
    }
    
	public IndelibleVSANPreferences(CheckCorrectDispatchThread dispatcher)
			throws IOException
	{
		super(dispatcher);
	}

	public File getPreferencesFileInternal()
	{
		File preferencesDir = getPreferencesDirInternal();
		return new File(preferencesDir, kPropertiesFileName);
	}

	public File getPreferencesDirInternal()
	{
		return new File(SystemInfo.getSystemInfo().getGlobalPreferencesDirectory(), kPreferencesDirName);
	}

	@Override
	protected void initPreferencesInternal(CheckCorrectDispatchThread dispatcher)
			throws IOException
	{
		File preferencesDir = getPreferencesDir();
		Properties defaults = new Properties();
		defaults.setProperty(kLogFileDirectoryPropertyName, new File(SystemInfo.getSystemInfo().getLogDirectory(), kLogDirName).getAbsolutePath()); //$NON-NLS-1$
		properties = new MonitoredProperties(defaults, dispatcher);
		setIfNotSet(kPreferencesDirPropertyName, preferencesDir.getAbsolutePath()); //$NON-NLS-1$
		setIfNotSet(kDiskImageLogFileDirectoryPropertyName, new File(SystemInfo.getSystemInfo().getCacheDirectory(), kDiskImageDirName).getAbsolutePath());
		setIfNotSet(kCacheDirPropertyName, new File(SystemInfo.getSystemInfo().getCacheDirectory(), kCacheDirName).getAbsolutePath());
		setIfNotSet(kTargetPortNumberPropertyName, "3270");
		File propertiesFile = getPreferencesFile(); //$NON-NLS-1$
		if (propertiesFile.exists())
		{
			FileInputStream propertiesInputStream = new FileInputStream(propertiesFile);
			properties.load(propertiesInputStream);
			propertiesInputStream.close();
		}
	}
	
	public int getTargetPortNumber()
	{
		String targetPortNumStr = (String) properties.get(kTargetPortNumberPropertyName);
		int returnPort = Integer.parseInt(targetPortNumStr);
		return returnPort;
	}
}
