import socket
from multiprocessing import Pool
from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
from urllib.parse import parse_qs
from urllib.parse import urlparse
import urllib.request
import urllib.parse
import imrcp_implementation_online as online
import imrcp_implementation_oneshot as oneshot
import imrcp_implementation_realtime as realtime
import torch
import pandas as pd
import math
import sys
import pickle
import warnings
import logging
from multiprocessing import Lock
import time


# Define the custom request handler by subclassing BaseHTTPRequestHandler
class MlpHandler(BaseHTTPRequestHandler):
	# Override the do_GET function
	def do_GET(self):
		try:
			oUrlParse = urlparse(self.path)
			oDict = parse_qs(oUrlParse.query)
			oDict['port_post'] = self.port_post
			oDict['port_listen'] = self.port_listen
			if self.path.startswith('/finished'):
				sSession = oDict['session'][0]
				LOGGER.info('hello')
				LOCK.acquire()
				LOGGER.info('{}'.format(PROCESSING))
				for i in range(0, len(PROCESSING)):
					LOGGER.info('{} {}'.format(i, PROCESSING[i]['session'][0]))
					
					if sSession == PROCESSING[i]['session'][0]:
						del PROCESSING[i]
						LOGGER.debug('Finished {}'.format(sSession))
						break
				if len(QUEUE) > 0:
					oDict = QUEUE.pop(0)
					PROCESSING.append(oDict)
					LOGGER.info('Processing queued {}'.format(oDict['session'][0]))
					POOL.apply_async(compute, kwds=oDict)
				LOCK.release()
				self.send_response(200)
				self.send_header('Content-type', 'text/html')
				self.end_headers()
				self.wfile.write(b'Finished')
				return
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
			oDict['port_post'] = self.port_post
			LOCK.acquire()
			if len(PROCESSING) < LIMIT:
				PROCESSING.append(oDict)
				LOGGER.info('{}'.format(PROCESSING))
				LOGGER.info('Processing {}'.format(oDict['session'][0]))
				LOCK.release()
				POOL.apply_async(compute, kwds=oDict)
			else:
				sFunction = oDict['function'][0]
				LOGGER.info('Queuing {}'.format(oDict['session'][0]))
				if sFunction == 'hur' or sFunction == 'hos':
					QUEUE.insert(0, oDict)
				else:
					QUEUE.append(oDict)
				LOCK.release()
			
			# Send a response to the client
			self.send_response(200)
			self.send_header('Content-type', 'text/html')
			self.end_headers()
			self.wfile.write(b'Compute function called asynchronously!')
		except:
			LOGGER.exception('')

class MyServer(ThreadingHTTPServer):
	def server_bind(self):
		self.socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
		self.socket.bind(self.server_address)
		
# Define the compute function
def compute(**kwargs):
	try:
		sSession = kwargs['session'][0]
		LOGGER.debug('Compute {}'.format(sSession))
		sFunction = kwargs['function'][0]
		bRequest = True
		if sFunction == 'hur':
			sErrors = runOnline(kwargs)
		elif sFunction == 'hos':
			sErrors = runOneshot(kwargs)
		elif sFunction == 'rt':
			sErrors = runRealTime(kwargs)
		elif sFunction == 'sleep':
			LOGGER.info('sleep')
			time.sleep(10)
			bRequest = False
		if bRequest:
			port_post = kwargs['port_post']
			req = urllib.request.Request('http://127.0.0.1:{}/api/{}/done'.format(port_post, kwargs['return'][0]), data = urllib.parse.urlencode({'errors': sErrors, 'session': kwargs['session'][0]}).encode())
			urllib.request.urlopen(req)
	except:
		LOGGER.exception('')
	finally:
		try:
			req = urllib.request.Request('http://127.0.0.1:{}/finished?session={}'.format(kwargs['port_listen'], sSession))
			urllib.request.urlopen(req)
		except:
			LOGGER.exception('')


def runRealTime(kwds):
	print('run rt')
	oPredictions = []
	oErrors = []
	start_time = kwds['starttime'][0] # define the start time of the prediction
	sDir = kwds['dir'][0]
	sPickleDir = kwds['model'][0]
	oHistDat = online.loadData('{}mlp_input.csv'.format(sDir))
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
	nObs = int(horz / resolution)
	oDefault = []
	for i in range(0, nObs):
		oDefault.append(math.nan)
	long_ts = pd.DataFrame(columns=['timestamplist', 'speed'])
	for sGroupName, oDf in oGroupsById:
		oDf = oDf.reset_index(drop=True)
		try:
			if len(oDf['Speed'].dropna()) < 5:
				oPred = oDefault
			else:
				oPred = realtime.pred_short(horz, resolution, start_time, oDf, long_ts, loaded_data, tree)
		except BaseException as oEx:
			oErrors.append('error with {}: {}'.format(sGroupName, str(oEx)))
			oPred = oDefault
		oPredictions.append(oPred)

	with open('{}mlp_output.csv'.format(sDir), 'w') as oFile:
		for i in range(0, nIdCount):
			oFile.write(idlist[i])
			for dVal in oPredictions[i]:
				oFile.write(',{:.2f}'.format(dVal))
			oFile.write('\n')
	
	for e in oErrors:
		print(e)
	if len(oErrors) == 0:
		return 'No errors'
	else:
		return str(oErrors)
	
	

def runOnline(kwds):
	oPredictions = []
	oErrors = []
	start_time = kwds['starttime'][0] # define the start time of the online prediction
	sDir = kwds['dir'][0]
	dummy_input_lstm = online.loadData('{}dummy_input_lstm.csv'.format(sDir))
	link_speed_next7d = online.loadData('{}link_speed_next7d.csv'.format(sDir))
	modelpath = kwds['model'][0]
	oGroupsById = link_speed_next7d.groupby('Id')
	idlist = [sId for sId in oGroupsById.groups.keys()]
	nIdCount = len(idlist)
	for i in range(0, nIdCount):
		oPredictions.append([])
	for horizon in range(1, 7): # define the prediction horizon (e.g., 1,2,3,4,5,6)
		try:
			model_online = online.LSTM(21,64,2) # initialize the model with 21 input features,2 hidden layers and each hidden layer with 64 nodes
			model_online.load_state_dict(torch.load(modelpath.format(horizon), map_location=torch.device('cpu')))
			model_online.eval()  # set the model in evaluation mode

			#sampling_rate = 3 # The raw data are sampled at a fixed interval before being input to the model
			#train_seq = int((12/sampling_rate)*24) # sequence length of the input data (24 hours*(12 data per hour)/(sampling rate))
			#for linkid in idlist:
			nCount = 0
			oDfsById = []
			for sGroupName, oDf in oGroupsById:
				try:
					oDf = oDf.reset_index(drop=True)
					if nCount % 1000 == 0:
						print(nCount)
					nCount += 1
					strt_index = oDf[oDf['time']==start_time].index.values[0]
					oDfsById.append(oDf[strt_index-287:strt_index:3])
				except BaseException as oEx:
					oErrors.append('error with {}: {}'.format(sGroupName, str(oEx)))
			all_link_input = pd.concat(oDfsById)
			all_link_input = pd.concat([dummy_input_lstm,all_link_input]).reset_index(drop=True)
		
			inputs = online.preprocessing_data(all_link_input) # obtain normalized input
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
	for s in oErrors:
		print(s)
	with open('{}online_output.csv'.format(sDir), 'w') as oFile:
		for i in range(0, nIdCount):
			oFile.write(idlist[i])
			for dVal in oPredictions[i]:
				oFile.write(',{:.2f}'.format(dVal))
			oFile.write('\n')
	
	if len(oErrors) == 0:
		return 'No errors'
	else:
		return str(oErrors)
	
def runOneshot(kwds):
	print('running oneshot')
	oPredictions = []
	oErrors = []
	sDir = kwds['dir'][0]
	oneshot_input = online.loadData('{}oneshot_input.csv'.format(sDir)) # load features for 7-day congestion status prediction
	dummy_input_oneshot = online.loadData('{}dummy_input_oneshot.csv'.format(sDir)) # load dummy input
	link_speed_past7d =online.loadData('{}link_speed_past7d.csv'.format(sDir)) # load past 7-day speed for 7-day speed pattern generation
	modelpath = kwds['model'][0]
	model_oneshot = oneshot.MultiClassCongestion(20,3) # initialize the model with 20 inputs features and 3 output labels
	model_oneshot.load_state_dict(torch.load(modelpath.format('oneshot'), map_location=torch.device('cpu'))) # load the trained model from file
	model_oneshot.eval() # set the model in evaluation mode
	oneshot_input = oneshot_input[['Id','t_to_lf', 'Direction', 'Lanes', 'lat', 'lon', 'dis_to_lf', 'timeofday', 'spd_mean_past7', 'spd_std_past7', 'category', 'lf_loc']]
	oOneshotGroupsById = oneshot_input.groupby('Id')
	oSpeedPastGroupsById = link_speed_past7d.groupby('Id')
	idlist = [sId for sId in oOneshotGroupsById.groups.keys()]
	nIdCount = len(idlist)
	for sGroupName, oDf in oOneshotGroupsById:
		try:
			single_link_input = pd.concat([dummy_input_oneshot,oDf]).reset_index(drop=True) # concat the dummy input with the demo link input, so that the input feature dimension aligns with the trained model's input feature dimension
			inputs = oneshot.data_normalization(single_link_input) # input data normalization
			inputs_tensor = torch.tensor(inputs, dtype=torch.float32) # convert the input into a pytorch tensor to be processed by the pre-trained model
			link_history_speed_7d = oSpeedPastGroupsById.get_group(sGroupName)['Speed'] # prepare the past 7 days' speed records

			link_speed_predicted = oneshot.OneShotSpeedPredict(model_oneshot,inputs_tensor,link_history_speed_7d) # generate 7-day horizon speed pattern
			oPredictions.append(link_speed_predicted)
		except KeyError:
			oPredictions.append(None)
			pass
		except BaseException as oEx:
			oPredictions.append(None)
			print(str(oEx))
			oErrors.append('error with {}: {}'.format(sGroupName, str(oEx)))

	with open('{}oneshot_output.csv'.format(sDir), 'w') as oFile:
		for i in range(0, nIdCount):
			if oPredictions[i] is None:
				continue
			oFile.write(idlist[i])
			for dVal in oPredictions[i]:
				oFile.write(',{:.2f}'.format(dVal))
			oFile.write('\n')
	return 'No errors'


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

warnings.filterwarnings("ignore")
logging.basicConfig(filename = sys.argv[4], format='%(asctime)s - %(levelname)s - %(message)s', level=logging.DEBUG)
LOGGER = logging.getLogger('pymrcp')
LIMIT = int(sys.argv[3])
QUEUE = []
PROCESSING = []
LOCK = Lock()
POOL = Pool(processes=LIMIT)

if __name__ == '__main__':
	port_listen = int(sys.argv[1])
	
	# Create the server with MlpHandler as the request handler
	handler = MlpHandler
	httpd = MyServer(("", port_listen), handler)
	httpd.allow_reuse_address = True
	LOGGER.info('Server running on port {}'.format(port_listen))
	handler.port_post = int(sys.argv[2])
	handler.port_listen = port_listen
	# Start the server
	httpd.serve_forever()
	httpd.server_close()
	POOL.close()
	POOL.join()
	LOGGER.info('Shutdown gracefully')

