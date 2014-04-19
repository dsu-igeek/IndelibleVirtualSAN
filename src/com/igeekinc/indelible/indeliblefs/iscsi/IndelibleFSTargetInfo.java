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

import org.jscsi.target.settings.TargetInfo;

import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.indelible.oid.ObjectIDFactory;

public class IndelibleFSTargetInfo extends TargetInfo
{
    private String volumeID;
    private String filePath;
    
    public IndelibleFSTargetInfo(String targetName, String targetAlias, String volumeID, String filePath)
    {
        super(targetName, targetAlias);
        this.volumeID = volumeID;
        this.filePath = filePath;
    }

    public String getVolumeID()
    {
        return volumeID;
    }

    public String getFilePath()
    {
        return filePath;
    }

    public IndelibleFSObjectID getStorageVolumeID()
    {
        return (IndelibleFSObjectID) ObjectIDFactory.reconstituteFromString(volumeID);
    }
}
