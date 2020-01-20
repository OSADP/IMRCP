/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package imrcp.store.grib;

import java.io.BufferedReader;
import java.io.FileReader;

/**
 *
 * @author Federal Highway Administration
 */
public class Tables
{
	private static final String[] TEMPERATURE =
	{
		"Temperature",
		"Virtual temperature",
		"Poetential temperature",
		"Pseudo-adiabatic potential temperature or equivalent potential temperature",
		"Maximum temperature",
		"Minimum temperature",
		"Dew point temperature",
		"Dew point depression (or deficit)",
		"Lapse rate",
		"Temperature anomaly",
		"Latent heat net flux",
		"Sensible heat net flux",
		"Heat index",
		"Wind chill factor",
		"Minimum dew point depression",
		"Virtual potential temperature",
		"Snow phase change heat flux"
	};
	
	
	private static final String[] MOISTURE =
	{
		"Specific Humidity",
		"Relative Humidity",
		"Humidity Mixing Ratio",
		"Precipitable Water",
		"Vapour Pressure",
		"Saturation Deficit",
		"Evaporation",
		"Precipitation Rate",
		"Total Precipitation",
		"Large-Scale Precipitation (non-convective)",
		"Convective Precipitation",
		"Snow Depth",
		"Snowfall Rate Water Equivalent",
		"Water Equivalent of Accumulated Snow Depth",
		"Convective Snow",
		"Large-Scale Snow",
		"Snow Melt",
		"Snow Age",
		"Absolute Humidity",
		"Precipitation Type",
		"Integrated Liquid Water",
		"Condensate",
		"Cloud Mixing Ratio",
		"Ice Water Mixing Ratio",
		"Rain Mixing Ratio",
		"Snow Mixing Ratio",
		"Horizontal Moisture Convergence",
		"Maximum Relative Humidity",
		"Maximum Absolute Humidity",
		"Total Snowfall",
		"Precipitable Water Category",
		"Hail",
		"Graupel",
		"Categorical Rain",
		"Categorical Freezing Rain",
		"Categorical Ice Pellets",
		"Categorical Snow",
		"Convective Precipitation Rate",
		"Horizontal Moisture Divergence",
		"Precent frozen precipitation",
		"Potential Evaporation",
		"Potentail Evaporation Rate",
		"Snow Cover",
		"Rain Fraction of Total Cloud Water",
		"Rime Factor",
		"Total Column Integrated Rain",
		"Total Column Integrated Snow",
		"Large Scale Water Precipitation (Non-Convective)"
	};
	
	
	private static final String[] MOMENTUM =
	{
		
	};
	
	
	private static final String[] MASS =
	{
		
	};
	
	
	private static final String[] SHORT_WAVE_RADIATION =
	{
		
	};
	
	
	private static final String[] LONG_WAVE_RADIATION =
	{
		
	};
	
	
	private static final String[] CLOUD =
	{
		
	};
	
	
	private static final String[] THERMODYNAMIC_STABILITY_INDICIES =
	{
		
	};
	
	
	private static final String[] KINEMATIC_STABILITY_INDICIES =
	{
		
	};
	
	private static final String[] TEMPERATURE_PROBABILITIES =
	{
		
	};
	
	
	private static final String[] MOISTURE_PROBABILITIES =
	{
		
	};
	
	
	private static final String[] MOMENTUM_PROBABILITIES =
	{
		
	};
	
	
	private static final String[] MASS_PROBABILITIES =
	{
		
	};
	
	
	private static final String[] AEROSOLS =
	{
		
	};
	
	
	private static final String[] TRACE_GASES =
	{
		
	};
	
	
	private static final String[] RADAR =
	{
		
	};
	
	
	private static final String[] FORECAST_RADAR_IMAGERY =
	{
		
	};
	
	
	private static final String[] ELECTRODYANMICS =
	{
		
	};
	
	
	private static final String[] NUCLEAR_RADIOLOGY =
	{
		
	};
	
	
	private static final String[] PHYSICAL_ATMOSPHERIC_PROPERTIES =
	{
		
	};
	
	
	private static final String[] ATMOSPHERIC_CHEMICAL_CONSTITUENTS=
	{
		
	};	
	
	
	private static final String[][] METEOROLOGICAL = new String[][] 
	{
		TEMPERATURE,
		MOISTURE,
		MOMENTUM,
		MASS,
		SHORT_WAVE_RADIATION,
		LONG_WAVE_RADIATION,
		CLOUD,
		THERMODYNAMIC_STABILITY_INDICIES,
		KINEMATIC_STABILITY_INDICIES,
		TEMPERATURE_PROBABILITIES,
		MOISTURE_PROBABILITIES,
		MOMENTUM_PROBABILITIES,
		MASS_PROBABILITIES,
		AEROSOLS,
		TRACE_GASES,
		RADAR,
		FORECAST_RADAR_IMAGERY,
		ELECTRODYANMICS,
		NUCLEAR_RADIOLOGY,
		PHYSICAL_ATMOSPHERIC_PROPERTIES,
		ATMOSPHERIC_CHEMICAL_CONSTITUENTS
	};
	
	
	private static final String[][] HYDROLOGICAL = new String[][]
	{
		
	};
	
	
	private static final String[][] LAND_SURFACE = new String[][]
	{
		
	};
	
	
	private static final String[][] SPACE = new String[][]
	{
		
	};
	
	
	private static final String[][] OCEANOGRAPHIC = new String[][]
	{
		
	};
	
	
	
	private static final String[] MRMS_CAT2 = new String[]
	{
		"NLDN_CG_001min",
		"NLDN_CG_005min",
		"NLDN_CG_015min",
		"NLDN_CG_030min",
		"LightningProbabilityNext30min"		
	};
	
	
	private static final String[] MRMS_CAT3 = new String[]
	{
		"MergedAzShear0to2kmAGL",
		"MergedAzShear3to6kmAGL",
		"RotationTrack30min",
		"RotationTrack60min",
		"RotationTrack120min",
		"RotationTrack240min",
		"RotationTrack360min",
		"RotationTrack1440min",
		null,
		null,
		null,
		null,
		null,
		null,
		"RotationTrackML30min",
		"RotationTrackML60min",
		"RotationTrackML120min",
		"RotationTrackML240min",
		"RotationTrackML360min",
		"RotationTrackML1440min",
		null,
		null,
		null,
		null,
		null,
		null,
		"SHI",
		"POSH",
		"MESH",
		"MESHMax30min",
		"MESHMax60min",
		"MESHMax120min",
		"MESHMax240min",
		"MESHMax360min",
		"MESHMax1440min",
		null,
		null,
		null,
		null,
		null,
		null,
		"VIL",
		"VILDensity",
		"VII",
		"EchoTop_18",
		"EchoTop_30",
		"EchoTop_50",
		"EchoTop_60",
		"H50AboveM20C",
		"H50Above0C",
		"H60AboveM20C",
		"H60Above0C",
		"Reflectivity0C",
		"ReflectivityM5C",
		"ReflectivityM10C",
		"ReflectivityM15C",
		"ReflectivityM20C",
		"ReflectivityAtLowestAltitude",
		"MergedReflectivityAtLowestAltitude"
	};
	
	
	private static final String[] MRMS_CAT4 = new String[]
	{
		"IRband4",
		"Visible",
		"WaterVapor",
		"CloudCover"
	};
	
	
	private static final String[] MRMS_CAT6 = new String[]
	{
		"PrecipFlag",
		"PrecipRate",
		"RadarOnlyQPE01H",
		"RadarOnlyQPE03H",
		"RadarOnlyQPE06H",
		"RadarOnlyQPE12H",
		"RadarOnlyQPE24H",
		"RadarOnlyQPE48H",
		"RadarOnlyQPE72H",
		"GaugeCorrQPE01H",
		"GaugeCorrQPE03H",
		"GaugeCorrQPE06H",
		"GaugeCorrQPE12H",
		"GaugeCorrQPE24H",
		"GaugeCorrQPE48H",
		"GaugeCorrQPE72H",
		"GaugeOnlyQPE01H",
		"GaugeOnlyQPE03H",
		"GaugeOnlyQPE06H",
		"GaugeOnlyQPE12H",
		"GaugeOnlyQPE24H",
		"GaugeOnlyQPE48H",
		"GaugeOnlyQPE72H",
		"MountainMapperQPE01H",
		"MountainMapperQPE03H",
		"MountainMapperQPE06H",
		"MountainMapperQPE12H",
		"MountainMapperQPE24H",
		"MountainMapperQPE48H",
		"MountainMapperQPE72H"
	};
	
	
	private static final String[] MRMS_CAT7 = new String[]
	{
		"ModelSurfaceTemp",
		"ModelWetBulbTemp",
		"WarmRainProbability",
		"ModelHeight0C",
		"BrightBandTopHeight",
		"BrightBandBottomHeight"
	};
	
	
	private static final String[] MRMS_CAT8 = new String[]
	{
		"RadarQualityIndex",
		"GaugeInflIndex01H",
		"GaugeInflIndex03H",
		"GaugeInflIndex06H",
		"GaugeInflIndex12H",
		"GaugeInflIndex24H",
		"GaugeInflIndex48H",
		"GaugeInflIndex72H",
		"SeamlessHSR",
		"SeamlessHSRHeight"
	};
	
	
	private static final String[] MRMS_CAT9 = new String[]
	{
		"ConusMergedReflectivityQC",
		"ConusPlusMergedReflectivityQC"
	};
	
	
	private static final String[] MRMS_CAT10 = new String[]
	{
		"MergedReflectivityQCComposite",
		"HeightCompositeReflectivity",
		"LowLevelCompositeReflectivity",
		"HeightLowLevelCompositeReflectivity",
		"LayerCompositeReflectivity_Low",
		"LayerCompositeReflectivity_High",
		"LayerCompositeReflectivity_Super",
		"ReflectivityCompositeHourlyMax",
		null,
		"LayerCompositeReflectivity_ANC"
	};
	
	
	private static final String[] MRMS_CAT11 = new String[]
	{
		"MergedBaseReflectivityQC",
		"MergedReflectivityComposite",
		"MergedReflectivityQComposite",
		"MergedBaseReflectivity",
		"Merged_LVL3_BaseDHC"
	};
	
	
	private static final String[] MRMS_CAT12 = new String[]
	{
		"FLASH_CREST_MAXUNITSTREAMFLOW",
		"FLASH_CREST_MAXSTREAMFLOW",
		"FLASH_CREST_MAXSOILSAT",
		null,
		"FLASH_SAC_MAXUNITSTREAMFLOW",
		"FLASH_SAC_MAXSTREAMFLOW",
		"FLASH_SAC_MAXSOILSAT",
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		"FLASH_QPE_ARI30M",
		"FLASH_QPE_ARI01H",
		"FLASH_QPE_ARI03H",
		"FLASH_QPE_ARI06H",
		"FLASH_QPE_ARI12H",
		"FLASH_QPE_ARI24H",
		"FLASH_QPE_MAX",
		null,
		null,
		null,
		null,
		null,
		"FLASH_QPE_FFG01H",
		"FLASH_QPE_FFG03H",
		"FLASH_QPE_FFG06H",
		"FLASH_QPE_FFGMAX",
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		"FLASH_HP_MAXUNITSTREAMFLOW",
		"FLASH_HP_MAXSTREAMFLOW"
	};
	
	
	private static final String[] MRMS_CAT13 = new String[]
	{
		"ANC_ConvectiveLikelihood",
		"ANC_FinalForecast"
	};
	
	
	private static final String[] MRMS_CAT14 = new String[]
	{
		"LVL3_HREET",
		"LVL3_HighResVIL",
	};
	
	
	private static final String[][] MRMS = new String[][]
	{
		null,
		null,
		MRMS_CAT2,
		MRMS_CAT3,
		MRMS_CAT4,
		null,
		MRMS_CAT6,
		MRMS_CAT7,
		MRMS_CAT8,
		MRMS_CAT9,
		MRMS_CAT10,
		MRMS_CAT11,
		MRMS_CAT12,
		MRMS_CAT13,
		MRMS_CAT14,
	};
	
	
	private static final String[][][] PRODUCTS = new String[][][] 
	{
		METEOROLOGICAL,
		HYDROLOGICAL,
		LAND_SURFACE,
		SPACE,
		null,
		null,
		null,
		null,
		null,
		null,
		OCEANOGRAPHIC,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		null,
		MRMS
	};
	
	
	public static String getTableValue(int nDiscipline, int nCategory, int nParameter)
	{
		return PRODUCTS[nDiscipline][nCategory][nParameter];
	}
	
	
	public static void main(String[] sArgs)
	{
		try (BufferedReader oIn = new BufferedReader(new FileReader("C:\\Users\\aaron.cherney\\Documents\\CarmaCloud\\grib\\UserTable_MRMS_v11.5.5.csv")))
		{
			String sLine = oIn.readLine();
			int nPar = 0;
			while ((sLine = oIn.readLine()) != null)
			{
				
				String[] sCols = sLine.split(",", -1);
				int nCat = Integer.parseInt(sCols[1]);
				if (nCat == 14)
				{
					int nParameter = Integer.parseInt(sCols[2]);
					if (nPar == nParameter)
					{
						System.out.println(String.format("\t\t\"%s\",", sCols[3]));
					}
					else
					{
						while (nPar != nParameter)
						{
							System.out.println(String.format("\t\tnull,"));
							++nPar;
						}
						System.out.println(String.format("\t\t\"%s\",", sCols[3]));
					}
					++nPar;
				}
			}
		}
		catch (Exception ex)
		{
			ex.printStackTrace();
		}
	}
}
