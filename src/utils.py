import numpy as np
import pandas as pd
import pandapower as pp

def order_smms_by_undervoltage_sum(uv_data_avg, vol_lim=207/230):
    """Orders smms in uv_data_avg by sum of undervoltages
    Function sums negative deviations from vol_lim for each smm and orders them from highest to lowest
    Args:
    --------
        uv_data_avg: pd.DataFrame
            dataframe with low average voltage occurrences
        vol_lim: float
            critical average voltage
    Returns:
    --------
        smms_ordered: list
            list of smms ordered by sum of undervoltages from highest to lowest"""
    smms_feeder_uv = uv_data_avg.smm.unique()
    sum_undervoltages = []
    for smm in smms_feeder_uv:
        df_smm = uv_data_avg[uv_data_avg.smm == smm]    
        sum_undervoltages.append((vol_lim - df_smm["avg_u"]).sum())
        # sum_undervoltages.append(df_smm["avg_u"].min())
    inds = np.argsort(sum_undervoltages)
    smms_ordered = [smms_feeder_uv[i] for i in inds]
    smms_ordered.reverse()
    return smms_ordered


def get_feeder_smms(snet, feeder):
    """Returns smms in given feeder"""
    return snet.load[snet.load["feeder"] == feeder].smm



def find_battery_smm(snet, smms_ordered):
    """Finds optimal bus for battery"""
    battery_smm_found = False
    while not battery_smm_found:
        if len(smms_ordered) == 0:
            raise Exception("No battery smm found")
        battery_smm = smms_ordered.pop(0)
        if battery_smm in snet.load.smm.values:
            battery_smm_found = True
            return battery_smm
    raise Exception("No battery smm found")

def get_bus_from_smm(snet, smm):
    """Returns bus from smm"""
    return snet.load.loc[snet.load.smm == smm].bus.values[0]

def get_smm_from_bus(snet, bus):
    """Returns smm from bus"""
    return snet.load.loc[snet.load.bus == bus].smm.values[0]

def get_data_from_smm_list(df_vol, smm_list):
    """Returns data from smm list"""
    return df_vol[df_vol["smm"].isin(smm_list)]

def get_dates_from_df(df):
    """Returns dates from dataframe"""
    return df["date_time"].unique()

def limit_voltage(vol_df, lim_vol=0.9, average=False):
    """In voltage_data data, limit voltage to lim_vol"""
    if average:
        return(vol_df[(vol_df["avg_u"] <= lim_vol)])
    else:
        return(vol_df[(vol_df["min_u"] <= lim_vol)])
    
def create_network(json_path):
	net = pp.from_json(json_path)
	return net