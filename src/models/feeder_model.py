import pandas as pd
import pandapower as pp
from slope_calculation import calculate_slopes
from models.trafo_model import TrafoModel
from utils import *


class FeederModel():

    def __init__(self, tm: TrafoModel, feeder_name):
        self.tm = tm
        self.feeder_name = feeder_name
        self.feeder_res_df = pd.DataFrame()
        self.snet = self.tm.snet
        self.smms = get_feeder_smms(self.snet, self.feeder_name)
        self.voltage_data = get_data_from_smm_list(self.tm.voltage_data,
                                                   self.smms)
        self.undervoltage_data = get_data_from_smm_list(
            self.tm.undervoltage_data, self.smms)
        self.feeder_res = pd.DataFrame()
        self.battery_smm = None
        self.suitable_for_battery = False
        self.slopes = None
        self.bm = None
        self.N_of_UV = None
        self.N_of_OV = None
        self.N_of_smms = None
        self.N_of_uv_smms = None
        self.N_of_ov_smms = None
        self.N_dates_UV = None
        self.N_dates_OV = None
        self.enough_voltage_data = self.tm.enough_voltage_data

    def define_calibration_lim_vol(self, over = False):
        """Defines calibration limit voltage for feeder based on undervoltage data,
        so that we have enough undervoltage data for calibration"""
        if over:
            max_voltages = self.voltage_data.groupby("date_time")["u_123"].max() #Find maximum average voltage for each date
            self.lim_vol_avg = min(max_voltages.quantile(0.995) / 230, 253 / 230)
        else:
            min_voltages = self.voltage_data.groupby("date_time")["u_123"].min()
            self.lim_vol_avg = max(min_voltages.quantile(0.005) / 230, 207 / 230)

    def define_and_limit_voltage(self, average=True, over = False):
        """Calculates limit average voltage and limits voltage data    	
        Function calculates limit average voltage for given feeder, and
        limits average voltage data based on this limit."""
        self.define_calibration_lim_vol(over = over)
        if over:
            self.ov_data = limit_voltage(self.voltage_data,
                                                  lim_vol=self.lim_vol_avg,
                                                  average=average, over = True)
            self.avg_dates = self.ov_data["date_time"].unique()
        else:
            
            self.uv_data_avg = limit_voltage(self.voltage_data,
                                         lim_vol=self.lim_vol_avg,
                                         average=average,
                                         over = False)
            self.avg_dates = self.uv_data_avg["date_time"].unique()

    def write_voltage_data(self, empty_battery_columns=False):
        """Writes under and over voltage parameters for given feeder to results dataframe"""
        self.feeder_res["Trafo"] = [self.tm.trafo_name]
        self.feeder_res["Feeder"] = [self.feeder_name]
        self.feeder_res["N_of_UV"] = [self.N_of_UV]
        self.feeder_res["N_of_OV"] = [self.N_of_OV]
        self.feeder_res["N_of_smms"] = [self.N_of_smms]
        self.feeder_res["N_of_smms_with_UV"] = [self.N_of_uv_smms]
        self.feeder_res["N_of_smms_with_OV"] = [self.N_of_ov_smms]
        self.feeder_res["N_of_dates_with_UV"] = [self.N_dates_UV]
        self.feeder_res["N_of_dates_with_OV"] = [self.N_dates_OV]
        if empty_battery_columns:
            self.feeder_res["battery_smm"] = [None]
            self.feeder_res["battery_capacity"] = [None]
            self.feeder_res["battery_power"] = [None]
            self.feeder_res["battery_cycles"] = [None]

    def calculate_uv_parameters(self):
        """Calculates undervoltage parameters for given feeder, initializes average
        undervoltage dataframe for calibration"""
        self.N_of_UV = len(self.undervoltage_data)
        self.N_of_smms = len(self.smms)
        self.N_of_uv_smms = len(self.undervoltage_data["smm"].unique())
        self.N_dates_UV = len(self.undervoltage_data["date_time"].unique())

    def calculate_ov_parameters(self):  
        """Calculates overvoltage parameters for given feeder"""
        self.N_of_OV = len(self.overvoltage_data)
        if self.N_of_smms == None:
            self.N_of_smms = len(self.smms)
        self.N_of_ov_smms = len(self.overvoltage_data["smm"].unique())
        self.N_dates_OV = len(self.overvoltage_data["date_time"].unique())

    def determine_battery_smm(self, over = False):
        """Determines on which smm to place battery based on undervoltage data"""
        smms_ordered = order_smms_by_undervoltage_sum(self.uv_data_avg,
                                                      vol_lim=self.lim_vol_avg)
        if over:
            smms_ordered = smms_ordered[::-1]
        self.battery_smm = find_battery_smm(self.snet, smms_ordered)

    def calculate_slopes(self, over=False):
        """Calculates slopes for given feeder, for battery smm"""
        self.define_and_limit_voltage()
        self.determine_battery_smm(over)
        self.slopes = calculate_slopes(self.snet, [self.battery_smm],
                                       self.avg_dates,
                                       self.smms,
                                       self.tm.df_p,
                                       self.tm.df_q,
                                       self.tm.df_vol,
                                       calibrate=self.enough_voltage_data)

    def calculate_and_write_uv_data(self, empty_battery_columns=False):
        """Calculates undervoltage parameters for given feeder, determines if solving with battery is needed, calculates voltage-power slopes"""
        self.calculate_uv_parameters()
        self.write_voltage_data(empty_battery_columns)
        self.suitable_for_battery = self.N_dates_UV >= 4
    
    def calculate_and_write_ov_data(self, empty_battery_columns=False):
        """Calculates undervoltage parameters for given feeder, determines if solving with battery is needed, calculates voltage-power slopes"""
        self.calculate_ov_parameters()
        self.write_voltage_data(empty_battery_columns)
        self.suitable_for_battery = self.N_dates_OV >= 4

    def calculate_uv_data_and_slopes(self):
        """Calculates undervoltage parameters for given feeder, determines if solving with battery is needed, calculates voltage-power slopes"""

        self.calculate_and_write_uv_data()
        if self.suitable_for_battery:
            self.calculate_slopes()

    def calculate_ov_data_and_slopes(self):
        """Calculates undervoltage parameters for given feeder, determines if solving with battery is needed, calculates voltage-power slopes"""

        self.calculate_and_write_ov_data()
        if self.suitable_for_battery:
            self.calculate_slopes(over = True)

