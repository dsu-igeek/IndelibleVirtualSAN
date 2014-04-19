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
import java.io.PrintWriter;
import java.io.StringWriter;
import java.rmi.RemoteException;
import java.security.InvalidKeyException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.util.Map;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.jscsi.target.Target;
import org.json.JSONException;
import org.json.JSONObject;

import com.igeekinc.indelible.indeliblefs.IndelibleFSClient;
import com.igeekinc.indelible.indeliblefs.IndelibleFSVolumeIF;
import com.igeekinc.indelible.indeliblefs.IndelibleServerConnectionIF;
import com.igeekinc.indelible.indeliblefs.datamover.DataMoverSession;
import com.igeekinc.indelible.indeliblefs.exceptions.FileExistsException;
import com.igeekinc.indelible.indeliblefs.exceptions.ForkNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.ObjectNotFoundException;
import com.igeekinc.indelible.indeliblefs.exceptions.PermissionDeniedException;
import com.igeekinc.indelible.indeliblefs.exceptions.VolumeNotFoundException;
import com.igeekinc.indelible.indeliblefs.proxies.IndelibleFSServerProxy;
import com.igeekinc.indelible.indeliblefs.remote.IndelibleFSVolumeRemote;
import com.igeekinc.indelible.indeliblefs.security.AuthenticationFailureException;
import com.igeekinc.indelible.indeliblefs.security.EntityAuthenticationServer;
import com.igeekinc.indelible.indeliblefs.server.IndelibleFSServerConnectionRemote;
import com.igeekinc.indelible.indeliblefs.server.IndelibleFSServerRemote;
import com.igeekinc.indelible.indeliblefs.webaccess.IndelibleMgmtException;
import com.igeekinc.indelible.oid.IndelibleFSObjectID;
import com.igeekinc.indelible.oid.ObjectIDFactory;
import com.igeekinc.indelible.server.IndelibleServerPreferences;
import com.igeekinc.util.FilePath;
import com.igeekinc.util.logging.ErrorLogMessage;

public class IndelibleiSCSIMgmtServlet extends HttpServlet
{
    private static final long serialVersionUID = 2374103832749294703L;

    protected IndelibleFSServerProxy fsServer;
    protected IndelibleServerConnectionIF connection;
    protected EntityAuthenticationServer securityServer;
    private DataMoverSession moverSession;
    protected Logger logger;
    IndelibleFSTarget target;

    
    public IndelibleiSCSIMgmtServlet() 
            throws IOException, UnrecoverableKeyException, InvalidKeyException, KeyStoreException, NoSuchAlgorithmException, CertificateException, IllegalStateException, NoSuchProviderException, SignatureException, AuthenticationFailureException, InterruptedException
    {
        logger = Logger.getLogger(getClass());

        IndelibleFSServerProxy[] servers = new IndelibleFSServerProxy[0];
        
        while(servers.length == 0)
        {
            servers = IndelibleFSClient.listServers();
            if (servers.length == 0)
                Thread.sleep(1000);
        }
        fsServer = servers[0];

        connection = fsServer.open();
    }
    
    public void setTarget(IndelibleFSTarget target)
    {
    	this.target = target;
    }
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException
    {
        Map<String, String[]>paramMap = req.getParameterMap();
        String [] keys = paramMap.get("cmd");
        if (keys.length != 1)
        {
            resp.sendError(200);
        }
        else
        {
            try
            {
                String command = keys[0].toLowerCase().trim();
                JSONObject resultObject = null;
                if (command.equals("export"))
                    resultObject = export(req, resp, paramMap);
                if (command.equals("unexport"))
                    resultObject = unexport(req, resp, paramMap);
                if (command.equals("listexports"))
                    resultObject = listExports();
                if (command.equals("serverinfo"))
                	resultObject = serverInfo();
                if (resultObject != null)
                {
                    resp.getWriter().print(resultObject.toString(5));
                }
                else
                {
                    throw new InternalError("No result object created");
                }
            } catch (IndelibleMgmtException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
                doError(resp, e);
            }
            catch(Throwable t)
            {
                doError(resp, new IndelibleMgmtException(IndelibleMgmtException.kInternalError, t));
            }
        }
    }
    
    public JSONObject listExports() throws RemoteException, JSONException
    {
        JSONObject returnObject = new JSONObject();
        String [] targetNames = target.getTargetServer().getTargetNames();
        for (String curTargetName:targetNames)
        {
            JSONObject curTargetObject = new JSONObject();
            curTargetObject.put("targetname", curTargetName);
            Target curTarget = target.getTargetServer().getTarget(curTargetName);
            IndelibleFSStorageModule curStorageModule = (IndelibleFSStorageModule)curTarget.getStorageModule();
            curTargetObject.put("blocks", curStorageModule.getSizeInBlocks());
            curTargetObject.put("blocksize", curStorageModule.getBlockSizeInBytes());
            curTargetObject.put("bytes", curStorageModule.getSizeInBlocks() * curStorageModule.getBlockSizeInBytes());
            curTargetObject.put("fsid", curStorageModule.getVolumeID().toString());
            curTargetObject.put("storagepath", curStorageModule.getStoragePath().toString());
            returnObject.append("listexports", curTargetObject);
        }
        return returnObject;
    }

    
    public JSONObject export(HttpServletRequest req, HttpServletResponse resp, Map<String, String[]>paramMap) throws JSONException, IndelibleMgmtException
    {
        JSONObject returnObject = new JSONObject();
        JSONObject exportObject = new JSONObject();
        returnObject.put("export", exportObject);
        String path=req.getPathInfo();
       
        FilePath reqPath = FilePath.getFilePath(path);        
        
        String [] exportTargetNameArr = paramMap.get("target");
        if (reqPath == null || exportTargetNameArr == null || exportTargetNameArr.length < 1 || reqPath.getNumComponents() < 3)
            throw new IndelibleMgmtException(IndelibleMgmtException.kInvalidArgument, null);
        String exportTargetName = exportTargetNameArr[0];
        if (reqPath == null || exportTargetName == null || reqPath.getNumComponents() < 3)
            throw new IndelibleMgmtException(IndelibleMgmtException.kInvalidArgument, null);

        // Should be an absolute path
        reqPath = reqPath.removeLeadingComponent();
        String fsIDStr = reqPath.getComponent(0);
        IndelibleFSVolumeIF volume = getVolume(fsIDStr);
        FilePath exportPath = reqPath.removeLeadingComponent();

        try
        {
            target.exportIndelibleFSFile(volume, exportPath, exportTargetName, null);
            exportObject.put("targetname", exportTargetName);
        } catch (ObjectNotFoundException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (PermissionDeniedException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (RemoteException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (ForkNotFoundException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (IOException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        }

        try
        {
            target.writeConfig();
        } catch (RemoteException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (PermissionDeniedException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (FileExistsException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (ForkNotFoundException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (IOException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        }
        return returnObject;
    }
    
    public JSONObject unexport(HttpServletRequest req, HttpServletResponse resp, Map<String, String[]>paramMap) throws JSONException, IndelibleMgmtException
    {
        JSONObject returnObject = new JSONObject();
        JSONObject unExportObject = new JSONObject();
        returnObject.put("unexport", unExportObject);
       
        String [] unExportTargetNameArr = paramMap.get("target");
        if (unExportTargetNameArr == null || unExportTargetNameArr.length < 1)
            throw new IndelibleMgmtException(IndelibleMgmtException.kInvalidArgument, null);
        String unExportTargetName = unExportTargetNameArr[0];
        
        if (unExportTargetName == null)
            throw new IndelibleMgmtException(IndelibleMgmtException.kInvalidArgument, null);

        if (target.unexport(unExportTargetName))
        {
            unExportObject.put("targetname", unExportTargetName);

            try
            {
                target.writeConfig();
            } catch (RemoteException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            } catch (PermissionDeniedException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            } catch (FileExistsException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            } catch (ForkNotFoundException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            } catch (IOException e)
            {
                Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
            }
        }
        else
        {
        	throw new IndelibleMgmtException(IndelibleMgmtException.kInvalidArgument, null);
        }
        return returnObject;
    }
    
    public JSONObject serverInfo() throws JSONException
    {
    	JSONObject returnObject = new JSONObject();
    	JSONObject serverInfoObject = new JSONObject();
    	returnObject.put("serverinfo", serverInfoObject);
    	serverInfoObject.put("port", target.getTargetServer().getConfig().getPort());
    	return returnObject;
    }
    
    public IndelibleFSVolumeIF getVolume(String fsIDStr) throws IndelibleMgmtException
    {
        IndelibleFSObjectID retrieveVolumeID = (IndelibleFSObjectID) ObjectIDFactory.reconstituteFromString(fsIDStr);
        if (retrieveVolumeID == null)
        {
            throw new IndelibleMgmtException(IndelibleMgmtException.kInvalidVolumeID, null);
        }
        
        IndelibleFSVolumeIF volume = null;
        try
        {
            volume = connection.retrieveVolume(retrieveVolumeID);
        } catch (VolumeNotFoundException e1)
        {
            throw new IndelibleMgmtException(IndelibleMgmtException.kVolumeNotFoundError, e1);
        } catch (IOException e1)
        {
            throw new IndelibleMgmtException(IndelibleMgmtException.kInternalError, e1);
        }
        if (volume == null)
        {
            throw new IndelibleMgmtException(IndelibleMgmtException.kVolumeNotFoundError, null);
        }
        return volume;
    }
    
    public void doError(HttpServletResponse resp, IndelibleMgmtException exception)
    {
        resp.setStatus(500);
        try
        {
            JSONObject resultObject = new JSONObject();
            JSONObject errorObject = new JSONObject();
            resultObject.put("error", errorObject);
            errorObject.put("code", exception.getErrorCode());
            
            Throwable cause = exception.getCause();
            if (cause != null)
            {
                StringWriter stringWriter = new StringWriter();
                cause.printStackTrace(new PrintWriter(stringWriter));
                errorObject.put("longReason", stringWriter.toString());
            }
            else
            {
                errorObject.put("longReason", "");
            }
            resp.getWriter().print(resultObject.toString());
            
        } catch (JSONException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        } catch (IOException e)
        {
            Logger.getLogger(getClass()).error(new ErrorLogMessage("Caught exception"), e);
        }
        
    }
}
