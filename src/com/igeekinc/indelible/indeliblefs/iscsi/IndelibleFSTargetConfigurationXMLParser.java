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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.dom.DOMResult;
import javax.xml.transform.dom.DOMSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.jscsi.target.settings.TargetConfiguration;
import org.jscsi.target.settings.TargetConfigurationXMLParser;
import org.jscsi.target.settings.TargetInfo;
import org.jscsi.target.settings.TextKeyword;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;

import com.igeekinc.indelible.indeliblefs.IndelibleFSForkIF;
import com.igeekinc.indelible.indeliblefs.IndelibleFileNodeIF;
import com.igeekinc.indelible.indeliblefs.exceptions.ForkNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSForkRemote;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSForkRemoteInputStream;

/**
 * Instances of {@link TargetConfiguration} provides access target-wide
 * parameters, variables that are the same across all sessions and connections
 * that do not change after initialization and which play a role during text
 * parameter negotiation. Some of these parameters are provided or can be
 * overridden by the content of an XML file - <code>jscsi-target.xml</code>.
 * 
 * @author Andreas Ergenzinger
 */
public class IndelibleFSTargetConfigurationXMLParser extends TargetConfigurationXMLParser
{
    public static final String kVolumeIDElementName = "VolumeID";
    public static final String kFilePathElementName = "FilePath";
    public static final String kStorageFileElementName = "StorageFile";

    public TargetConfiguration parseSettings(IndelibleFileNodeIF configFileNode) throws ForkNotFoundException, IOException, SAXException, ParserConfigurationException, PermissionDeniedException
    {
        IndelibleFSForkIF readFork = configFileNode.getFork("data", false);
        IndelibleFSForkRemoteInputStream readStream = new IndelibleFSForkRemoteInputStream(readFork);
        BufferedReader reader = new BufferedReader(new InputStreamReader(readStream));
        String curLine;
        while((curLine = reader.readLine()) != null)
            System.out.println(curLine);
        reader.close();

        readStream = new IndelibleFSForkRemoteInputStream(readFork);
        BufferedInputStream bis = new BufferedInputStream(readStream);
    	URL schemaURL = null;
    	File overrideSchemaFile = getFile(RELEASE_CONFIGURATION_DIRECTORY, DEVELOPMENT_CONFIGURATION_DIRECTORY, SCHEMA_FILE_NAME);
    	if (overrideSchemaFile.exists())
    		schemaURL = new URL("file:"+overrideSchemaFile);
    	else
    		schemaURL = ClassLoader.getSystemResource("com/igeekinc/indelible/indeliblefs/iscsi/jscsi-target.xsd");
        Document configurationDoc = parse(schemaURL, bis);
        bis.close();
        return parseSettings(configurationDoc.getDocumentElement());
    }
    
    public Document parse(final URL schemaLocation,
            final InputStream configFileIS) throws SAXException,
            ParserConfigurationException, IOException {

        final SchemaFactory schemaFactory = SchemaFactory
                .newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
        final Schema schema = schemaFactory.newSchema(schemaLocation);

        // create a validator for the document
        final Validator validator = schema.newValidator();

        final DocumentBuilderFactory domFactory = DocumentBuilderFactory
                .newInstance();
        domFactory.setNamespaceAware(true); // never forget this
        final DocumentBuilder builder = domFactory.newDocumentBuilder();
        final Document doc = builder.parse(configFileIS);

        final DOMSource source = new DOMSource(doc);
        final DOMResult result = new DOMResult();

        validator.validate(source, result);
        return (Document) result.getNode();
    }
    @Override
    public TargetInfo parseTargetElement(Element targetElement)
    {
        String targetName = targetElement.getElementsByTagName(TextKeyword.TARGET_NAME).item(0)
                .getTextContent();
        // TargetAlias (optional)
        Node targetAliasNode = targetElement.getElementsByTagName(
                TextKeyword.TARGET_ALIAS).item(0);
        String targetAlias = null;
        if (targetAliasNode != null)
            targetAlias = targetAliasNode.getTextContent();
        NodeList fileProperties = targetElement
                .getElementsByTagName(kStorageFileElementName).item(0).getChildNodes();
        String storageFilePath = null;
        String volumeID = null;
        for (int i = 0; i < fileProperties.getLength(); ++i) {
            if (kFilePathElementName.equals(fileProperties.item(i).getNodeName()))
                storageFilePath = fileProperties.item(i).getTextContent();
            if (kVolumeIDElementName.equals(fileProperties.item(i).getNodeName()))
                volumeID = fileProperties.item(i).getTextContent();
        }
        if (storageFilePath == null)
            storageFilePath = "storage.dat";
        
        IndelibleFSTargetInfo returnInfo = new IndelibleFSTargetInfo(targetName, targetAlias, volumeID, storageFilePath);
        return returnInfo;
    }
    
    
}
