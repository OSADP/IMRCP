import socket
from multiprocessing import Pool
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from urllib.parse import parse_qs
from urllib.parse import urlparse
import urllib.request
import urllib.parse
import imrcp_implementation_online as huronline
import imrcp_implementation_oneshot as huroneshot
import imrcp_implementation_realtime as realtime
import torch
import torch.nn as nn
import pandas as pd
import math
import sys
import pickle
import warnings
import logging
import numpy as np
import json
import os
import traceback

import matplotlib.pyplot as plt
import statistics as sta
from sklearn.preprocessing import MinMaxScaler
import time
import random
import glob
import haversine as hs
from datetime import datetime, timedelta


from mlph_functions import extract_csvfile
from mlph_functions import spatial_temporal_feature
from mlph_functions import create_dummy_oneshot
from mlph_functions import create_dummy_online
from mlph_functions import filter_mata
from mlph_functions import oneshot_annotate
from mlph_functions import mlp_log
from mlph_functions import log_exception

from model_training import train_oneshot
from model_training import train_online

warnings.filterwarnings("ignore")
logging.basicConfig(filename = sys.argv[4], format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
logger = logging.getLogger('pymrcp')
# Define the custom request handler by subclassing BaseHTTPRequestHandler
class MlpHandler(BaseHTTPRequestHandler):
	# Override the do_GET function
	def do_GET(self):
		try:
			if self.path.startswith('/shutdown_server'):
				try:
					self.send_response(200)
					self.send_header('Content-type', 'text/html')
					self.end_headers()
					self.wfile.write(b'Server shutdown')
				except:
					pass

				httpd.shutdown() # stop
				return
			# Call the compute function using apply_async in a multiprocessing pool
			oUrlParse = urlparse(self.path)
			oDict = parse_qs(oUrlParse.query)
			oDict['port_post'] = self.port_post
			oDict['log_dir'] = self.log_dir
			self.pool.apply_async(compute, kwds=oDict)
			# Send a response to the client
			self.send_response(200)
			self.send_header('Content-type', 'text/html')
			self.end_headers()
			self.wfile.write(b'Compute function called asynchronously!')
		except:
			logger.exception('')

class MyServer(ThreadingHTTPServer):
	def server_bind(self):
		self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.socket.bind(self.server_address)
		
# Define the compute function
def compute(**kwargs):
	sFunction = kwargs['function'][0]
	try:
		if sFunction == 'hur':
			sErrors = runHurOnline(kwargs)
		elif sFunction == 'hos':
			sErrors = runHurOneshot(kwargs)
		elif sFunction == 'rt':
			sErrors = runRealTime(kwargs)
		elif sFunction == 'lts':
			sErrors = runLongTs(kwargs)
		elif sFunction == 'os':
			sErrors = runOneshot(kwargs)
		elif sFunction == 'trainhur':
			sErrors = trainHur(kwargs)
			logger.error(sErrors)
			return
	except BaseException as oEx:
		sErrors = 'Uncaught error in {}'.format(sFunction)
		log_exception(oEx,'{}{}.log'.format(kwargs['log_dir'], os.getpid()))
	port_post = kwargs['port_post']
	req = urllib.request.Request('http://127.0.0.1:{}/api/{}/done'.format(port_post, kwargs['return'][0]), data = urllib.parse.urlencode({'errors': sErrors, 'session': kwargs['session'][0]}).encode())
	urllib.request.urlopen(req)

def loadData(sPath):
	return pd.read_csv(sPath, encoding='utf_8')


def trainHur(kwds):
	sMetadataFile = kwds['metadatafile'][0]
	
	sLogFile = '{}{}.log'.format(kwds['log_dir'], os.getpid())
	mlp_log('training', sLogFile)
	oMetadataObj = None
	with open(sMetadataFile, 'r') as oFile:
		oMetadataObj = json.load(oFile)
	
	oStats = oMetadataObj['stats']
	data_mata = loadData(oMetadataObj['linkid'])
	freeway_name = oMetadataObj['freeway_name']
	filtered_mata = filter_mata(data_mata, freeway_name)
	file_pattern_prefix = oMetadataObj['file_pattern_prefix']
	folder_path = oMetadataObj['folder_path']
	lf_time = oMetadataObj['lf_time']
	category = oMetadataObj['category']
	lf_zone = oMetadataObj['lf_zone']
	lf_coord = oMetadataObj['lf_coord']
	hurricane = oMetadataObj['hurricane']
	sStatusLog = oMetadataObj['statuslog']
	mlp_log('Starting training for {}'.format(sMetadataFile), sStatusLog)
	data_w_feature_7d = None
	data_w_feature_8d = None
	data_6hr_7d_all = None
	for i in range(len(lf_time)):
		mlp_log('Assembling data for storm {}'.format(hurricane[i]), sStatusLog)
		mlp_log(hurricane[i], sLogFile)
		oMeansAndStds = oStats[hurricane[i]]
		combined_past7d,combined_next7d,combined_next8d = extract_csvfile(folder_path[i],file_pattern_prefix,hurricane[i], category[i] ,lf_time[i],lf_zone[i],lf_coord[i])
		if combined_past7d is None:
			log_exception(combined_next7d, sLogFile)
			mlp_log('Failed to load data for {}'.format(hurricane[i]), sStatusLog)
			continue
		data_w_feature_7d_single = spatial_temporal_feature(combined_next7d,hurricane[i],lf_time[i],category[i],lf_zone[i],lf_coord[i],filtered_mata)
		data_w_feature_8d_single = spatial_temporal_feature(combined_next8d,hurricane[i],lf_time[i],category[i],lf_zone[i],lf_coord[i],filtered_mata)
		
		data_6hr_7d_all_single = oneshot_annotate(data_w_feature_7d_single,combined_past7d,lf_time[i], sLogFile, sStatusLog, oMeansAndStds, hurricane[i])
		
		if data_w_feature_7d is None:
			data_w_feature_7d = data_w_feature_7d_single
			data_w_feature_8d = data_w_feature_8d_single
			data_6hr_7d_all = data_6hr_7d_all_single
		else:
			data_w_feature_7d = pd.concat([data_w_feature_7d, data_w_feature_7d_single])
			data_w_feature_8d = pd.concat([data_w_feature_8d, data_w_feature_8d_single])
			data_6hr_7d_all = pd.concat([data_6hr_7d_all, data_6hr_7d_all_single])
	
	if data_w_feature_7d is None:
		mlp_log('No data available to train model', sStatusLog)
		return
	data_w_feature_7d = pd.DataFrame(data_w_feature_7d)
	data_w_feature_8d = pd.DataFrame(data_w_feature_8d)
	data_6hr_7d_all = pd.DataFrame(data_6hr_7d_all)
	sOutputDir = oMetadataObj['networkdir']
	os.makedirs(sOutputDir, exist_ok=True)
	mlp_log('Starting training for oneshot model', sStatusLog)
	data_6hr_7d_all.to_csv('{}data_6hr_7d_all.csv'.format(sOutputDir), index=False)
	data_w_feature_8d.to_csv('{}data_w_feature_8d.csv'.format(sOutputDir), index=False)
	dummy_oneshot = create_dummy_oneshot(data_w_feature_8d,data_6hr_7d_all)
	dummy_oneshot.to_csv('{}dummy_oneshot.csv'.format(sOutputDir),index=False)
	model_oneshot = train_oneshot(data_6hr_7d_all,dummy_oneshot, sLogFile, sStatusLog)
	
	mlp_log('Saving trained oneshot model', sStatusLog)
	torch.save(model_oneshot.state_dict(), '{}oneshot_model.pth.tmp'.format(oMetadataObj['networkdir']))
	
	dummy_online = create_dummy_online(data_w_feature_8d,data_6hr_7d_all)
	dummy_online.to_csv('dummy_online.csv',index=False)
	
	for horizon in range(1, 7):
		mlp_log('Starting online model for hour {}'.format(horizon), sStatusLog)
		model_online = train_online(data_6hr_7d_all,data_w_feature_8d,dummy_online,horizon, sLogFile, sStatusLog)
		torch.save(model_online.state_dict(), '{}online_model_{}hour.pth'.format(oMetadataObj['networkdir'], horizon))
		mlp_log('Finished online model for hour {}'.format(horizon), sStatusLog)
	
	os.rename('{}oneshot_model.pth.tmp'.format(oMetadataObj['networkdir']), '{}oneshot_model.pth'.format(oMetadataObj['networkdir']))

def runOneshot(kwds):
	oPredictions = []
	oErrors = []
	start_time = kwds['starttime'][0] # define the start time of the prediction
	sDir = kwds['dir'][0]
	nOutputIndex = int(kwds['outputindex'][0])
	sPickleDir = kwds['model'][0]
	loaded_data = None
	with open('{}mlp_python_data.pkl'.format(sPickleDir), "rb") as file:
		loaded_data = pickle.load(file)
	oHistDat = loadData('{}mlp_oneshot_input.csv'.format(sDir))
	oGroupsById = oHistDat.groupby('Id')
	idlist = [sId for sId in oGroupsById.groups.keys()]
	nIdCount = len(idlist)
	oLongTs = loadData('{}mlp_lts_output.csv'.format(sDir))
	oLongTsById = oLongTs.groupby('Id')
# 	nWeatherIntvl = 60
# 	nOutputs = int(60 / nWeatherIntvl * 168)
	for sGroupName, oDf in oGroupsById:
		oDf = oDf.reset_index(drop=True)
		try:
			if sGroupName in oLongTsById.groups:
				oLts = oLongTsById.get_group(sGroupName)
				oLts = oLts.reset_index(drop=True)
				#oPred = realtime.oneshot(nWeatherIntvl, start_time, oDf, oLts, loaded_data)
				oPred = realtime.oneshot_new(oDf, start_time, oLts, loaded_data)
				bAllNans = True
				for dVal in oPred[nOutputIndex:nOutputIndex + 25]:
					if not math.isnan(dVal):
						bAllNans = False
						break
				if bAllNans:
					oPred = None
			else:
				oPred = None
		except BaseException as oEx:
			oErrors.append('error with {}: {}'.format(sGroupName, str(oEx)))
			oPred = None
		oPredictions.append(oPred)
		
	with open('{}mlp_oneshot_output.csv'.format(sDir), 'w') as oFile:
		for i in range(0, nIdCount):
			if oPredictions[i] is None:
				continue
			oFile.write(idlist[i])
			for dVal in oPredictions[i][nOutputIndex:nOutputIndex + 25]:
				oFile.write(',{:.2f}'.format(dVal))
			oFile.write('\n')
	
	if len(oErrors) == 0:
		return ''
	
	return str(oErrors)

def runLongTs(kwds):
	oPredictions = []
	oErrors = []
	start_time = kwds['starttime'][0] # define the start time of the prediction
	sDir = kwds['dir'][0]
	oHistDat = loadData('{}mlp_lts_input.csv'.format(sDir))
	oGroupsById = oHistDat.groupby('Id')
	idlist = [sId for sId in oGroupsById.groups.keys()]
	nIdCount = len(idlist)
	
	sTsFormat = '%Y-%m-%d %H:%M'
	oStartTime = datetime.strptime(start_time, sTsFormat)
	oBeginningOfDay = oStartTime - timedelta(hours=oStartTime.hour + 1)
	startt = oBeginningOfDay.strftime(sTsFormat)
	for sGroupName, oDf in oGroupsById:
		oDf = oDf.reset_index(drop=True)
		try:
			if len(oDf['Speed'].dropna()) < 5:
				oPred = None
			else:
				oPred = realtime.long_ts_update(startt, oDf)
				oPred = np.concatenate([oPred[1728:2016], oPred, oPred[0:288]])
		except BaseException as oEx:
			oErrors.append('error with {}: {}'.format(sGroupName, str(oEx)))
			oPred = None
		oPredictions.append(oPred)
	
	oStartOfForecast = oBeginningOfDay - timedelta(hours=23, minutes=55)
	oFiveMinutes = timedelta(minutes=5)
	with open('{}mlp_lts_output.csv'.format(sDir), 'w') as oFile:
		oFile.write('Id,timestamplist,speed\n')
		for i in range(0, nIdCount):
			if oPredictions[i] is None:
				continue
			oTs = datetime.fromtimestamp(oStartOfForecast.timestamp())
			sId = idlist[i]
			for dVal in oPredictions[i]:
				oFile.write(sId)
				oFile.write(',{}'.format(oTs.strftime(sTsFormat)))
				oTs = oTs + oFiveMinutes
				oFile.write(',{:.2f}\n'.format(dVal))
			oFile.write('\n')
	

	if len(oErrors) == 0:
		return ''
	
	return str(oErrors)
	
def runRealTime(kwds):
	oPredictions = []
	oErrors = []
	start_time = kwds['starttime'][0] # define the start time of the prediction
	sDir = kwds['dir'][0]
	sPickleDir = kwds['model'][0]
	oHistDat = loadData('{}mlp_input.csv'.format(sDir))
	oGroupsById = oHistDat.groupby('Id')
	idlist = [sId for sId in oGroupsById.groups.keys()]
	nIdCount = len(idlist)
	
	# load the decision tree model
	tree = None
	with open('{}decision_tree.pickle'.format(sPickleDir),'rb') as file:
		tree = pickle.load(file) # here is a re-trained decision tree model, load this before starting the online prediction
	
	# Load variables from the data file
	loaded_data = None
	with open('{}mlp_python_data.pkl'.format(sPickleDir), "rb") as file:
		loaded_data = pickle.load(file)
	horz = 120
	resolution = 15
# 	nObs = int(horz / resolution)

	long_ts = pd.DataFrame(columns=['timestamplist', 'speed'])
	for sGroupName, oDf in oGroupsById:
		oDf = oDf.reset_index(drop=True)
		try:
			if len(oDf['Speed'].dropna()) < 5:
				oPred = None
			else:
				oPred = realtime.pred_short(horz, resolution, start_time, oDf, long_ts, loaded_data, tree)
		except BaseException as oEx:
			oErrors.append('error with {}: {}'.format(sGroupName, str(oEx)))
			oPred = None
		oPredictions.append(oPred)

	with open('{}mlp_output.csv'.format(sDir), 'w') as oFile:
		for i in range(0, nIdCount):
			if oPredictions[i] is None:
				continue
			oFile.write(idlist[i])
			for dVal in oPredictions[i]:
				oFile.write(',{:.2f}'.format(dVal))
			oFile.write('\n')
	

	if len(oErrors) == 0:
		return ''
	
	return str(oErrors)
	
	

def runHurOnline(kwds):
	oPredictions = []
	oErrors = []
	start_time = kwds['starttime'][0] # define the start time of the online prediction
	sDir = kwds['dir'][0]
	dummy_input_lstm = loadData('{}dummy_input_lstm.csv'.format(sDir))
	link_speed_next7d = loadData('{}link_speed_next7d.csv'.format(sDir))
	modelpath = kwds['model'][0] + 'online_model_{}hour.pth'
	oGroupsById = link_speed_next7d.groupby('Id')
	idlist = [sId for sId in oGroupsById.groups.keys()]
	nIdCount = len(idlist)
	for i in range(0, nIdCount):
		oPredictions.append([])
	for horizon in range(1, 7): # define the prediction horizon (e.g., 1,2,3,4,5,6)
		try:
			model_online = huronline.LSTM(21,64,2) # initialize the model with 21 input features,2 hidden layers and each hidden layer with 64 nodes
			model_online.load_state_dict(torch.load(modelpath.format(horizon), map_location=torch.device('cpu')))
			model_online.eval()  # set the model in evaluation mode

			#sampling_rate = 3 # The raw data are sampled at a fixed interval before being input to the model
			#train_seq = int((12/sampling_rate)*24) # sequence length of the input data (24 hours*(12 data per hour)/(sampling rate))
			#for linkid in idlist:
			oDfsById = []
			for sGroupName, oDf in oGroupsById:
				try:
					oDf = oDf.reset_index(drop=True)
					strt_index = oDf[oDf['time']==start_time].index.values[0]
					oDfsById.append(oDf[strt_index-287:strt_index:3])
				except BaseException as oEx:
					oErrors.append('error with {}: {}'.format(sGroupName, str(oEx)))
			all_link_input = pd.concat(oDfsById)
			all_link_input = pd.concat([dummy_input_lstm,all_link_input]).reset_index(drop=True)
		
			inputs = huronline.preprocessing_data(all_link_input) # obtain normalized input
			#inputs = inputs.reshape(len(idlist),96,inputs.shape[-1]) # reshape the large 2d array to a 3d array, 1st dimension refers to link, 2nd dimension refers to timestamp, 2rd dimenstion refers to features
			inputs = inputs.reshape(nIdCount,96,inputs.shape[-1]) # reshape the large 2d array to a 3d array, 1st dimension refers to link, 2nd dimension refers to timestamp, 2rd dimenstion refers to features
			tensor_x = torch.FloatTensor(inputs)
			test_loader = torch.utils.data.DataLoader(tensor_x, batch_size=nIdCount, shuffle=False)
		
			for test_x in test_loader:
				outputs = model_online(test_x)*90
			oOutput = outputs.detach().numpy().flatten()
			for i in range(0, nIdCount):
				oPredictions[i].append(oOutput[i])
		except Exception as e:
			for i in range(0, nIdCount):
				oPredictions[i].append(math.nan)
			oErrors.append(str(e))

	with open('{}hur_output.csv'.format(sDir), 'w') as oFile:
		for i in range(0, nIdCount):
			oFile.write(idlist[i])
			for dVal in oPredictions[i]:
				oFile.write(',{:.2f}'.format(dVal))
			oFile.write('\n')
	
	if len(oErrors) == 0:
		return ''
	
	return str(oErrors)
	
def runHurOneshot(kwds):
	oPredictions = []
	oErrors = []
	sDir = kwds['dir'][0]
	oneshot_input = loadData('{}hos_input.csv'.format(sDir)) # load features for 7-day congestion status prediction
	dummy_input_oneshot = loadData('{}dummy_input_oneshot.csv'.format(sDir)) # load dummy input
	link_speed_past7d = loadData('{}link_speed_past7d.csv'.format(sDir)) # load past 7-day speed for 7-day speed pattern generation
	modelpath = kwds['model'][0] + 'oneshot_model.pth'
	model_oneshot = huroneshot.MultiClassCongestion(20,3) # initialize the model with 20 inputs features and 3 output labels
	model_oneshot.load_state_dict(torch.load(modelpath, map_location=torch.device('cpu'))) # load the trained model from file
	model_oneshot.eval() # set the model in evaluation mode
	oneshot_input = oneshot_input[['Id','t_to_lf', 'Direction', 'Lanes', 'lat', 'lon', 'dis_to_lf', 'timeofday', 'spd_mean_past7', 'spd_std_past7', 'category', 'lf_loc']]
	oOneshotGroupsById = oneshot_input.groupby('Id')
	oSpeedPastGroupsById = link_speed_past7d.groupby('Id')
	idlist = [sId for sId in oOneshotGroupsById.groups.keys()]
	nIdCount = len(idlist)
	for sGroupName, oDf in oOneshotGroupsById:
		try:
			single_link_input = pd.concat([dummy_input_oneshot,oDf]).reset_index(drop=True) # concat the dummy input with the demo link input, so that the input feature dimension aligns with the trained model's input feature dimension
			inputs = huroneshot.data_normalization(single_link_input) # input data normalization
			inputs_tensor = torch.tensor(inputs, dtype=torch.float32) # convert the input into a pytorch tensor to be processed by the pre-trained model
			link_history_speed_7d = oSpeedPastGroupsById.get_group(sGroupName)['Speed'] # prepare the past 7 days' speed records

			link_speed_predicted = huroneshot.OneShotSpeedPredict(model_oneshot,inputs_tensor,link_history_speed_7d) # generate 7-day horizon speed pattern
			oPredictions.append(link_speed_predicted)
		except KeyError:
			oPredictions.append(None)
			pass
		except BaseException as oEx:
			oPredictions.append(None)
			oErrors.append('error with {}: {}'.format(sGroupName, str(oEx)))

	with open('{}hos_output.csv'.format(sDir), 'w') as oFile:
		for i in range(0, nIdCount):
			if oPredictions[i] is None:
				continue
			oFile.write(idlist[i])
			for dVal in oPredictions[i]:
				oFile.write(',{:.2f}'.format(dVal))
			oFile.write('\n')
			
	if len(oErrors) == 0:
		return ''
	
	return str(oErrors)


# =============================================================================
# if __name__ == '__main__':
# 	oDict = {}
# 	oDict['function'] = ['oneshot']
# 	oDict['dir'] = ['/dev/shm/imrcp-test/mlphurricane/KRaN77RTMG4Kmz77UyAsHg/1630022400000/']
# 	oDict['model'] = ['/zpmain/data/imrcp-data-test/mlphurricane/KRaN77RTMG4Kmz77UyAsHg/KRaN77RTMG4Kmz77UyAsHg_{}.pth']
# 	oDict['session'] = ['13852602324539661']
# 	oDict['starttime'] = ['2021-08-26 23:55:00']
# 	runOneshot(oDict)
# =============================================================================
	
# =============================================================================
# 	oDict = {}
# 	oDict['function'] = ['online']
# 	oDict['dir'] = ['/dev/shm/imrcp-test/mlphurricane/DwJf_MAfzTUKVzgnrv5pdw/1630022400000/']
# 	oDict['model'] = ['/zpmain/data/imrcp-data-test/mlphurricane/DwJf_MAfzTUKVzgnrv5pdw/DwJf_MAfzTUKVzgnrv5pdw_{}.pth']
# 	oDict['session'] = ['13852602324539661']
# 	oDict['starttime'] = ['2021-08-26 23:55:00']
# 	runOnline(oDict)
# =============================================================================

# =============================================================================
# 	oDict = {}
# 	oDict['function'] = ['rt']
# 	oDict['dir'] = ['/zpmain/data/temp/mlprt/1686320100000_rt/127_211/']
# 	oDict['session'] = ['16110012526208421']
# 	oDict['starttime'] = ['2023-06-09 09:10']
# 	oDict['model'] = ['/zpmain/data/temp/mlp/']
# 	oDict['return'] = ['mlp']
# 	runRealTime(oDict)
# =============================================================================
# =============================================================================
# if __name__ == '__main__':
# 	oDict = {}
# 	oDict['function'] = ['lts']
# 	oDict['dir'] = ['/zpmain/data/imrcp-data-test/scenarios/zsXUBebgvUD-SsrZdOx50-xYb7xVOIYIMOAd2gi0L44/123_210/']
# 	oDict['session'] = ['16110012526208421']
# 	oDict['starttime'] = ['2023-06-16 06:55']
# 	oDict['model'] = ['/zpmain/data/temp/mlp/']
# 	oDict['return'] = ['mlp']
# 	print(runLongTs(oDict))
# =============================================================================
# =============================================================================
# if __name__ == '__main__':
# 	oDict = {}
# 	oDict['function'] = ['os']
# 	oDict['dir'] = ['/zpmain/data/imrcp-data-test/scenarios/zsXUBebgvUD-SsrZdOx50-xYb7xVOIYIMOAd2gi0L44/123_210/']
# 	oDict['session'] = ['16110012526208421']
# 	oDict['starttime'] = ['2023-06-16 08:00']
# 	oDict['model'] = ['/zpmain/data/temp/mlp/']
# 	oDict['return'] = ['mlp']
# 	oDict['outputindex'] = ['0']
# 	print(runOneshot(oDict))
# =============================================================================

if __name__ == '__main__':
	# Create a multiprocessing pool with 4 processes
	pool = Pool(processes=int(sys.argv[3]))
	port_listen = int(sys.argv[1])
	
	# Create the server with MlpHandler as the request handler
	handler = MlpHandler
	handler.pool = pool
	httpd = MyServer(("", port_listen), handler)
	httpd.allow_reuse_address = True
	logger.info('Server running on port {}'.format(port_listen))
	handler.port_post = int(sys.argv[2])
	handler.log_dir = sys.argv[4][:sys.argv[4].rfind('/') + 1]
	# Start the server
	httpd.serve_forever()
	httpd.server_close()
	pool.close()
	pool.join()
	logger.info('Shutdown gracefully')

