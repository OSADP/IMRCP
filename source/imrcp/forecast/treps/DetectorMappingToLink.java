/* 
 * Copyright 2017 Federal Highway Administration.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package imrcp.forecast.treps;

import imrcp.geosrv.GeoUtil;
import imrcp.imports.Detector;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.util.ArrayList;

/**
 * This class is a one-off program to generate the detector_mapping_to_link.txt
 * file needed by Treps.
 */
public class DetectorMappingToLink
{

	/**
	 * Generates the detector_mapping_to_link.txt file needed by Treps from the
	 * detector mapping file.
	 *
	 * @param sArgs command line arguments. 1st argument: absolute path of the
	 * detector mapping file 2nd argument: absolute path where the
	 * detector_mapping_to_link.txt file will be written
	 */
	public static void main(String[] sArgs)
	{
		ArrayList<Detector> oDetectors = new ArrayList();

		try (BufferedReader oIn = new BufferedReader(new InputStreamReader(new FileInputStream(sArgs[0])))) //open the detector id mapping file
		{
			String sLine = oIn.readLine(); //read the header, don't need to add to list
			while ((sLine = oIn.readLine()) != null) //read in each line
			{
				try
				{
					oDetectors.add(new Detector(sLine));
				}
				catch (Exception oException)
				{
				}
			}
			try (BufferedWriter oWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(sArgs[1]))))
			{
				for (Detector oDet : oDetectors)
				{
					oWriter.write(Integer.toString(oDet.m_nArchiveId));
					oWriter.write(" ");
					oWriter.write(Double.toString(GeoUtil.fromIntDeg(oDet.m_nLon)));
					oWriter.write(" ");
					oWriter.write(Double.toString(GeoUtil.fromIntDeg(oDet.m_nLat)));
					oWriter.write(" ");
					oWriter.write(oDet.m_sLinkNodeId);
					oWriter.write(" ");
					int nIndex = oDet.m_sLinkNodeId.indexOf("-");
					oWriter.write(oDet.m_sLinkNodeId.substring(0, nIndex));
					oWriter.write(" ");
					oWriter.write(oDet.m_sLinkNodeId.substring(nIndex + 1));
					oWriter.write("\n");
				}
			}
		}
		catch (Exception oException)
		{
			oException.printStackTrace();
		}
	}
}
