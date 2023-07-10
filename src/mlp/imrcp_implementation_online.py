import pandas as pd 
import numpy as np 
from sklearn.preprocessing import MinMaxScaler
import torch
import torch.nn as nn



class LSTM(nn.Module):
	def __init__(self, input_size, hidden_size, num_layers):
		super(LSTM, self).__init__()
		self.hidden_size = hidden_size
		self.num_layers = num_layers
		self.lstm = nn.LSTM(input_size, hidden_size, num_layers, batch_first=True)
		self.fc = nn.Linear(hidden_size, 1)

	def forward(self, x):
		# initialize hidden state and cell state
		h0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size)
		c0 = torch.zeros(self.num_layers, x.size(0), self.hidden_size)
		# forward propagate LSTM
		out, _ = self.lstm(x, (h0, c0))
		# decode the hidden state of the last time step
		out = self.fc(out[:, -1, :])
		return out


def preprocessing_data(data):
	# categorical features to be onverted to One Hot Encoding
	categ = ['Lanes', 'Direction', 'lf_loc']
	# One Hot Encoding conversion
	data = pd.get_dummies(data, prefix_sep="_", columns=categ) # expand the categorical variable columns from one column to multiple columns based on the number of classes
	# drop uniquid column
	data_out_raw = data.drop(['Id', 'time', 'name'], axis=1)
	# scale our data into range of 0 and 1
	scaler = MinMaxScaler(feature_range=(0, 1))
	data_out_raw = scaler.fit_transform(data_out_raw.loc[:, (data_out_raw.columns != 'category')]) # 'category' are ordinal variables, which will not be normalized 
	data_out_raw = np.append(data_out_raw, data['category'].to_numpy().reshape(-1, 1), 1)
	data_out_raw = data_out_raw[6::] # get rid of the first 6 rows (dummy inputs)
	return data_out_raw

class TimeseriesDataset(torch.utils.data.Dataset):
	def __init__(self, X: torch.tensor, seq_len: int = 1):
		self.X = X
		self.seq_len = seq_len
		
	def __len__(self) -> int:
		return self.X.__len__()
	
	def __getitem__(self, index):
		return (self.X[index:index + self.seq_len]) 