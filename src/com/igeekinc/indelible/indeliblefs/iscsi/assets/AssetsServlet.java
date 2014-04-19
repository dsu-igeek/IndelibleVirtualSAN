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
 
package com.igeekinc.indelible.indeliblefs.iscsi.assets;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.HashMap;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

public class AssetsServlet extends HttpServlet
{
	private static final long serialVersionUID = -2730268859412095578L;
	HashMap<String, byte[]>assets;
	public AssetsServlet() throws IOException
	{
		assets = new HashMap<String, byte[]>();
		String [] assetNames = {"NewLogo.jpg", "indelible_server_icon_cloud_256.png", "wiki.png"};
		for (String curAssetName:assetNames)
		{
			InputStream assetInputStream = AssetsServlet.class.getResourceAsStream(curAssetName);
			ArrayList<Byte>buffer = new ArrayList<Byte>();
			while (assetInputStream.available() > 0)
			{
				buffer.add((byte)assetInputStream.read());
			}
			assetInputStream.close();
			byte [] assetBuffer = new byte[buffer.size()];
			for (int curByteNum = 0; curByteNum < buffer.size() ; curByteNum ++)
			{
				assetBuffer[curByteNum] = buffer.get(curByteNum);
			}
				
			assets.put(curAssetName, assetBuffer);
		}
	}

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
            throws ServletException, IOException
    {
        String path=req.getPathInfo();
        if (path.lastIndexOf('/') >= 0)
        {
        	String assetName = path.substring(path.lastIndexOf('/') + 1);
        	byte [] assetBuffer = assets.get(assetName);
        	if (assetBuffer != null)
        	{
        		resp.getOutputStream().write(assetBuffer);
        		return;
        	}
        }
        resp.sendError(404);
    }
}
