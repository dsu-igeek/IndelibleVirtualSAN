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

import org.jscsi.target.storage.AbstractStorageModule;

import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.util.FilePath;

public abstract class IndelibleFSStorageModule extends AbstractStorageModule 
{
	public enum StorageState
	{
		kStable,
		kLoading,
		kWriteBack
	}
	protected IndelibleFSStorageModule(long sizeInBlocks) {
		super(sizeInBlocks);
		// TODO Auto-generated constructor stub
	}
	
	public abstract IndelibleFSObjectID getVolumeID() ;

	public abstract FilePath getStoragePath();
	
	public abstract StorageState getState();
	
	public abstract long getBytesToWriteBack();
}
