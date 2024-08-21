import pandas as pd
from data_loader import DataLoader
from preprocess import Preprocess
import warnings
from models.trafo_model import TrafoModel
from models.feeder_model import FeederModel
from models.battery_model import BatteryModel
import config


warnings.filterwarnings('ignore')

NET_PATH = config.NET_PATH
TRAFO_NAME = config.TRAFO_NAME
FOLDER_PATH =   config.FOLDER_PATH
create_trafo_results = True #IF True, results for all feeders will be saved, otherwise only results for feeders suitable for battery will be saved
create_battery_results = False #If True, results for feeders suitable for battery will be saved
# all_trafos_path = r"C:\Users\Public\Battery_calculator\uo_bat\src\data\all_transformers.csv"
# all_trafos_df = pd.read_csv(all_trafos_path, sep=";")
# trafos_list = list(all_trafos_df["TransformatorskaPostajaNaziv"])
if create_battery_results:
    battery_res = pd.DataFrame()
# for TRAFO_NAME in trafos_list[20:40]:
if create_trafo_results:
    trafo_res_df = pd.DataFrame()
trafo_name = TRAFO_NAME
# try:
dl = DataLoader(load_manual=True,
                trafo_name=trafo_name[:6]) 
voltage_data= dl.load_voltage_data()
pr = Preprocess(voltage_data)
voltage_data, undervoltage_data, trafo_suitable_for_battery = pr.preprocess_voltage_data()
# if trafo_suitable_for_battery:
#     tm = TrafoModel(voltage_data, undervoltage_data, None, None, None,
#                    trafo_name, NET_PATH)
#     print("Trafo suitable for battery")
#     trafo_suitable_for_battery = tm.is_there_enough_voltage_data()
#     print(trafo_suitable_for_battery)
if trafo_suitable_for_battery:
    # There are undervoltages, we need to fix
    power_data = dl.load_power_data()
    df_vol, df_p, df_q = pr.preprocess_powers_create_pivot_tables(power_data)

    tm = TrafoModel(voltage_data, undervoltage_data, df_vol, df_p, df_q,
                   trafo_name, NET_PATH)
    
    tm.create_and_populate_snet()
    for feeder in tm.feeders:
        fm = FeederModel(tm, feeder)
        fm.calculate_uv_data_and_slopes()
        if create_trafo_results:
            bm = BatteryModel(fm)
            bm.calculate_battery_parameters()
            trafo_res_df = pd.concat([trafo_res_df, fm.feeder_res],
                        ignore_index=True)
            
        else:
            if fm.suitable_for_battery:
                bm = BatteryModel(fm)
                bm.calculate_battery_parameters()
                battery_res = pd.concat([battery_res, fm.feeder_res],
                                        ignore_index=True)
    if create_battery_results:
        print(battery_res)
else:
    if create_trafo_results:
        tm = TrafoModel(voltage_data, undervoltage_data,None, None, None,
                   trafo_name, NET_PATH)
        tm.create_and_populate_snet()
        for feeder in tm.feeders:
            fm = FeederModel(tm, feeder)
            fm.calculate_and_write_uv_data(empty_battery_columns=True)

            trafo_res_df = pd.concat([trafo_res_df, fm.feeder_res],
                        ignore_index=True)

if create_trafo_results:
    print(trafo_res_df)

# except    :
#     print("Calculation not possible for trafo: " + TRAFO_NAME)