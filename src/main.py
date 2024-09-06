import pandas as pd
from data_loader import DataLoader
from preprocess import Preprocess
import warnings

from models.trafo_model import TrafoModel
from models.feeder_model import FeederModel
from models.battery_model import BatteryModel
import config
import time

from datetime import datetime, timedelta

# Get the current time
now = datetime.now()

# Get the last midnight by replacing the hour, minute, second, and microsecond
last_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
one_year_ago = last_midnight - timedelta(days=365)


warnings.filterwarnings('ignore')

NET_PATH = config.NET_PATH
# TRAFO_NAME = config.TRAFO_NAME
# FOLDER_PATH =   config.FOLDER_PATH
create_trafo_results = False #IF True, results for all feeders will be saved, otherwise only results for feeders suitable for battery will be saved
create_battery_results = True #If True, results for feeders suitable for battery will be saved
# all_trafos_path = r"C:\Users\Public\uo_bat\uo_bat\src\data\uv_10min_transformers.csv"
# all_trafos_df = pd.read_csv(all_trafos_path, sep=";")
# trafos_list = list(all_trafos_df["TransformatorskaPostajaNaziv"])
# trafos_list.reverse()
# print(trafos_list)
# dl = DataLoader()
# trafos_list = dl.find_trafo_candidates()
# print(trafos_list)
trafos_list = ["T348- TAVČARJEVA"]
# trafos_list = ["T0948 USKOVNIK",  "T0068 DRAŽGOŠE", "T1195 VOGLJE KABELSKA", "T0107 BINKELJ",  "T0497 KRŽIŠE", "T369- VRTEC BLED", "T0483 BLEJC", "T0320 TRSTENIK JAMBOR", "T069- LAZE", "T494- DOLENJE BREZJE", "T0330 NEMILJE", "T051- LIPNICA", "T0309 JAMA", "T0039 LETENCE", "T0427 APNO", "T0648 OŽBOLT", "T0445 BESNICA ČEPULJE", "T0395 POVLJE", "T0015 ZGORNJE STRUŽEVO", "T1157 JELENDOL", "T0675 PRESKA TRŽIČ"]
res_df = pd.DataFrame()
if create_battery_results:
    battery_res = pd.DataFrame()
time0 = time.time()
for TRAFO_NAME in trafos_list:
    if create_trafo_results:
        trafo_res_df = pd.DataFrame()
    trafo_name = TRAFO_NAME
    print(trafo_name)
    # try:
    dl = DataLoader(load_manual=False,
                    trafo_name=trafo_name[:6],
                    start = one_year_ago,
                    end = last_midnight)
    voltage_data = dl.load_voltage_data()
    pr = Preprocess(voltage_data)
    voltage_data, undervoltage_data, trafo_suitable_for_battery = pr.preprocess_voltage_data_get_undervoltages()
    if trafo_suitable_for_battery:
        # There are undervoltages, we need to fix
        power_data = dl.load_power_data()
        df_vol, df_p, df_q = pr.preprocess_powers_create_pivot_tables(
            power_data)

        tm = TrafoModel(voltage_data, undervoltage_data, df_vol, df_p,
                        df_q, trafo_name, NET_PATH)

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
            tm = TrafoModel(voltage_data, undervoltage_data, None, None,
                            None, trafo_name, NET_PATH)
            tm.create_and_populate_snet()
            for feeder in tm.feeders:
                fm = FeederModel(tm, feeder)
                fm.calculate_and_write_uv_data(empty_battery_columns=True)

                trafo_res_df = pd.concat([trafo_res_df, fm.feeder_res],
                                         ignore_index=True)

    if create_trafo_results:
        print(trafo_res_df)
    # res_df = pd.concat([res_df, trafo_res_df],
    #                                      ignore_index=True)
    # except Exception as e:
    #     print(e)
print(time.time() - time0)
# res_df.to_excel("rezultati_investicij.xlsx", index = False)
