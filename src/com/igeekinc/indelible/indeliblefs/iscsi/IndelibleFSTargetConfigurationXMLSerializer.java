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
import java.io.OutputStream;

import org.apache.xerces.dom.DocumentImpl;
import org.apache.xml.serialize.Method;
import org.apache.xml.serialize.OutputFormat;
import org.apache.xml.serialize.XMLSerializer;
import org.jscsi.target.settings.TargetConfiguration;
import org.jscsi.target.settings.TargetConfigurationXMLParser;
import org.jscsi.target.settings.TargetInfo;
import org.jscsi.target.settings.TextKeyword;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import com.igeekinc.util.XMLUtils;

public class IndelibleFSTargetConfigurationXMLSerializer
{   
    public Document serializeToDocument(TargetConfiguration configuration)
    {
        DocumentImpl serializeDoc = new DocumentImpl();
        Element configurationElement = serializeDoc.createElement("configuration");
        serializeDoc.appendChild(configurationElement);
        configurationElement.setAttribute("xmlns", "http://www.jscsi.org/2010-04");
        configurationElement.setAttribute("xmlns:xsi", "http://www.w3.org/2001/XMLSchema-instance");
        configurationElement.setAttribute("xsi:schemaLocation", "http://www.jscsi.org/2010-04 jscsi-target.xsd");
        
        Element targetListNode = serializeDoc.createElement(TargetConfigurationXMLParser.TARGET_LIST_ELEMENT_NAME);
        TargetInfo [] targetInfoList = configuration.getTargetInfo();
        for (TargetInfo curTargetInfo:targetInfoList)
        {
            IndelibleFSTargetInfo curIFSTargetInfo = (IndelibleFSTargetInfo)curTargetInfo;
            Element targetInfoNode = serializeTargetInfo(serializeDoc,
                    curIFSTargetInfo);
            
            targetListNode.appendChild(targetInfoNode);
        }
        
        configurationElement.appendChild(targetListNode);
        
        Element globalConfigNode = serializeDoc.createElement(TargetConfigurationXMLParser.GLOBAL_CONFIG_ELEMENT_NAME);
        XMLUtils.appendSingleValElement(serializeDoc, globalConfigNode, TargetConfigurationXMLParser.PORT_ELEMENT_NAME, 
                Integer.toString(configuration.getPort()));
        XMLUtils.appendSingleValElement(serializeDoc, globalConfigNode, TargetConfigurationXMLParser.ALLOW_SLOPPY_NEGOTIATION_ELEMENT_NAME,
                configuration.getAllowSloppyNegotiation()?"true":"false");
        configurationElement.appendChild(globalConfigNode); 
        
        return serializeDoc;
    }

    public Element serializeTargetInfo(DocumentImpl serializeDoc,
            IndelibleFSTargetInfo curIFSTargetInfo)
    {
        Element targetInfoNode = serializeDoc.createElement(TargetConfigurationXMLParser.TARGET_ELEMENT_NAME);
        XMLUtils.appendSingleValElement(serializeDoc, targetInfoNode, TextKeyword.TARGET_NAME, curIFSTargetInfo.getTargetName());
        String targetAlias = curIFSTargetInfo.getTargetAlias();
        if (targetAlias != null)
            XMLUtils.appendSingleValElement(serializeDoc, targetInfoNode, TextKeyword.TARGET_ALIAS, targetAlias);
        Element storageFileElement = serializeDoc.createElement(IndelibleFSTargetConfigurationXMLParser.kStorageFileElementName);
        XMLUtils.appendSingleValElement(serializeDoc, storageFileElement, 
                IndelibleFSTargetConfigurationXMLParser.kVolumeIDElementName, curIFSTargetInfo.getVolumeID());
        XMLUtils.appendSingleValElement(serializeDoc, storageFileElement, IndelibleFSTargetConfigurationXMLParser.kFilePathElementName,
                curIFSTargetInfo.getFilePath());
        targetInfoNode.appendChild(storageFileElement);
        return targetInfoNode;
    }
    
    public void serializeToOutputStream(OutputStream outStream, TargetConfiguration configuration) throws IOException
    {
        Document configurationDoc = serializeToDocument(configuration);
        OutputFormat defOF = new OutputFormat(Method.XML, "UTF-8", false);
        defOF.setOmitXMLDeclaration(true);
        defOF.setOmitDocumentType(true);
        defOF.setIndenting(true);
        
        XMLSerializer xmlSer = new XMLSerializer(outStream, defOF);

        xmlSer.serialize(configurationDoc);
    }
}
