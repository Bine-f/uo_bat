import pandas as pd
import matplotlib.pyplot as plt
from data_loader import DataLoader
from preprocess import Preprocess
import warnings
from models.trafo_model import TrafoModel
import os
from models.feeder_model import FeederModel
from models.battery_model import BatteryModel
import config


warnings.filterwarnings('ignore')

NET_PATH = config.NET_PATH
TRAFO_NAME = config.TRAFO_NAME
FOLDER_PATH =   config.FOLDER_PATH

dl = DataLoader(FOLDER_PATH)
voltage_data, power_data, trafo_data = dl.load_trafo_data()

pr = Preprocess(voltage_data, power_data, trafo_data)
voltage_data, undervoltage_data, df_vol, df_p, df_q, df_tr = pr.preprocess_data()

tm = TrafoModel(voltage_data, undervoltage_data, df_vol, df_p, df_q, df_tr, TRAFO_NAME, NET_PATH)
tm.create_and_populate_snet()

for feeder in tm.feeders:
    fm = FeederModel(tm, feeder)
    fm.calculate_uv_data_and_slopes()

    bm = BatteryModel(fm)
    bm.calculate_battery_parameters()

print(tm.trafo_res_df)