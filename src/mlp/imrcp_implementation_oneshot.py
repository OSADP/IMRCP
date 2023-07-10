import pandas as pd 
import numpy as np 
from sklearn.preprocessing import MinMaxScaler
import torch
import torch.nn as nn


# 2. Load the model
# define the model
class MultiClassCongestion(nn.Module):
	def __init__(self, input_size, output_size):
		super(MultiClassCongestion, self).__init__()
		self.fc1 = nn.Linear(input_size, 64)
		self.fc2 = nn.Linear(64, 32)
		self.fc3 = nn.Linear(32, output_size)
		self.relu = nn.ReLU()
		self.softmax = nn.Softmax(dim=1)
		
	def forward(self, x):
		out = self.fc1(x)
		out = self.relu(out)
		out = self.fc2(out)
		out = self.relu(out)
		out = self.fc3(out)
		out = self.softmax(out)
		return out


#%% 4. Normalize the input data 

def data_normalization(df):
	# categorical features to be onverted to One Hot Encoding
	categ = ['Direction','Lanes','lf_loc']
	# One Hot Encoding conversion
	df_onehot = pd.get_dummies(df, prefix_sep="_", columns=categ) # expand the categorical variable columns from one column to multiple columns based on the number of classes
	df_onehot = df_onehot.drop(['Id'], axis=1) 
	# scale the numerical data to a range between 0 and 1
	scaler1 = MinMaxScaler(feature_range=(0, 1)) 
	array_norm_onehot = scaler1.fit_transform(df_onehot.loc[:, (df_onehot.columns != 'category')&(df_onehot.columns != 'timeofday')]) # 'category' and 'timeofday' are ordinal variables, which will not be normalized 
	array_norm_onehot = np.append(array_norm_onehot, df_onehot['timeofday'].to_numpy().reshape(-1,1), 1)
	array_norm_onehot = np.append(array_norm_onehot, df_onehot['category'].to_numpy().reshape(-1,1), 1)
	array_norm_onehot = array_norm_onehot[6::] # get rid of the first 6 rows (dummy inputs)
	return array_norm_onehot

#%% 5. make prediction
def OneShotSpeedPredict(model_oneshot,inputs_tensor,link_history_speed_7d):
	outputs = model_oneshot(inputs_tensor)
	predicted_class = torch.argmax(outputs, dim=1) # predict congestion labels for each 6-hour window across 7 days, giving 28 output labels for a link
	predicted_class = predicted_class.tolist()

	df_pred = pd.DataFrame({'prediction':predicted_class})
	df_pred = df_pred.reset_index(drop=True)
	df_pred['ratio'] = 1
	df_pred.loc[df_pred['prediction']==1,'ratio'] = np.random.uniform(low = 0.5, high = 0.8, size=len(df_pred[df_pred['prediction']==1]))
	df_pred.loc[df_pred['prediction']==2,'ratio'] = np.random.uniform(low = 0.1, high = 0.5, size=len(df_pred[df_pred['prediction']==2]))
	df_congestion = pd.DataFrame(np.repeat(df_pred.values, 72, axis=0))
	df_next7_pred = pd.DataFrame({'Speed':link_history_speed_7d.reset_index(drop=True),'congestion':df_congestion.loc[:,0],'ratio': df_congestion.loc[:,1]})
	df_next7_pred['Speed'] = df_next7_pred['Speed']*df_next7_pred['ratio']
	return df_next7_pred['Speed'].tolist()
