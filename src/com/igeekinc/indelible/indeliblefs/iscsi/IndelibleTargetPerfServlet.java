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

import java.io.IOException;
import java.text.DecimalFormat;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.xerces.dom.DocumentImpl;
import org.jscsi.target.Target;
import org.jscsi.target.storage.AbstractStorageModule;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.igeekinc.indelible.indeliblefs.webaccess.XMLOutputServlet;
import com.igeekinc.indelible.indeliblefs.webaccess.XMLUtils;

public class IndelibleTargetPerfServlet extends XMLOutputServlet
{
	private static final long serialVersionUID = -5774382813276223009L;
	IndelibleFSTarget target;
	
	public IndelibleTargetPerfServlet()
	{
		try
		{
			Document stylesheet =  XMLUtils.getDocument(IndelibleTargetPerfServlet.class.getResourceAsStream("IndelibleTargetPerfStylesheet.xsl"));
			setStylesheet(stylesheet);
		}
		catch (Exception e)
		{
			e.printStackTrace();
		}
	}
	@Override
	protected Document doRequest(HttpServletRequest req,
			HttpServletResponse resp) throws ServletException, IOException
	{
		Document buildDoc = new DocumentImpl();
		Element rootElem = buildDoc.createElement("performance");
		buildDoc.appendChild(rootElem);
		String [] targetNames = target.getTargetServer().getTargetNames();
		for (String curTargetName:targetNames)
		{
			Element targetElem = buildDoc.createElement("target");
			Target curTarget = target.getTargetServer().getTarget(curTargetName);
			long readBytesPerSecond = curTarget.getBytesReadPerSecond();
			long writeBytesPerSecond = curTarget.getBytesWrittenPerScond();
			
			XMLUtils.appendSingleValElement(buildDoc, targetElem, "targetName", curTargetName);
			AbstractStorageModule storageModule = curTarget.getStorageModule();
			IndelibleFSStorageModule indelibleFSStorageModule = (IndelibleFSStorageModule)storageModule;
			XMLUtils.appendSingleValElement(buildDoc, targetElem, "targetVolumePath", indelibleFSStorageModule.getStoragePath().toString());
			XMLUtils.appendSingleValElement(buildDoc, targetElem, "fsID", indelibleFSStorageModule.getVolumeID().toString());
			XMLUtils.appendSingleValElement(buildDoc, targetElem, "size", sizeString(indelibleFSStorageModule.getSizeInBlocks() * indelibleFSStorageModule.getBlockSizeInBytes()));
			XMLUtils.appendSingleValElement(buildDoc, targetElem, "readBytesPerSecond", perfString(readBytesPerSecond));
			XMLUtils.appendSingleValElement(buildDoc, targetElem, "writeBytesPerSecond", perfString(writeBytesPerSecond));
			if (storageModule instanceof IndelibleFSStorageModule)
			{
				String state="Unknown";
				switch(((IndelibleFSStorageModule)storageModule).getState())
				{
				case kLoading:
					state = "Loading";
					break;
				case kStable:
					state = "Stable";
					break;
				case kWriteBack:
					state = "Pending";
					break;
				}
				XMLUtils.appendSingleValElement(buildDoc, targetElem, "state", state);
			}
			rootElem.appendChild(targetElem);
		}
		return buildDoc;
	}

	String sizeString(long sizeBytes)
	{
		double size;
		if (sizeBytes < 1000 * 1000 * 1000)
		{
			size = sizeBytes/(1000 * 1000);
			DecimalFormat df = new DecimalFormat("0.0 MB");
			return df.format(size);
		}
		
		size = sizeBytes/(1000 * 1000 * 1000);
		DecimalFormat df = new DecimalFormat("0.0 GB");
		return df.format(size);
	}
	
	String perfString(long bytesTransferredPerSecond)
	{
		double perf;
		if (bytesTransferredPerSecond < 1000 * 1000 * 1000)
		{
			perf = bytesTransferredPerSecond/(1000 * 1000);
			DecimalFormat df = new DecimalFormat("0.0 MB/s");
			return df.format(perf);
		}
		
		perf = bytesTransferredPerSecond/(1000 * 1000 * 1000);
		DecimalFormat df = new DecimalFormat("0.0 GB/s");
		return df.format(perf);
	}
	public void setTarget(IndelibleFSTarget target)
	{
		this.target = target;
	}
}
