import pandas as pd
from utils import *

class Preprocess:
    def __init__(self, voltage_data, power_data, trafo_data):
        self.voltage_data = voltage_data
        self.power_data = power_data
        self.trafo_data = trafo_data
        self.minimal_vol = 150/230
        self.max_diff = 50/230
        self.fillna_method = None
        self.df_vol = None
        self.df_p = None
        self.df_q = None
        self.df_trafo = None
    


    def handle_names(self):
        """function changes the column names of the dataframes with energy and voltage data, according to code standards
        Args:
        --------
            power_data:
                dataframe with energy data
            voltage_data:
                dataframe with voltage data
            trafo_data:
                dataframe with transformer data
        """
        self.handle_names_df_vol()
        self.handle_names_power_data()
        if self.trafo_data is not None:
            self.handle_names_df_tr()

    def handle_names_df_vol(self):
        """function changes the column names of the dataframe with energy data, according to code standards
        Args:
        --------
            voltage_data:
                dataframe with voltage data  
        """    
        self.voltage_data.rename(columns={"DatumUraCET": "date_time","Napetost_L1": "u_1", "Napetost_L2": "u_2", "Napetost_L3": "u_3", 
                                     "Napetost_L123": "u_123", "SMM": "smm","TransformatorskaPostajaSID": "tp_sid"}, inplace=True)
    def handle_names_power_data(self):
        """function changes the column names of the dataframe with energy data, according to code standards
        Args:
        --------
            power_data:
                dataframe with energy
        """
        self.power_data.rename(columns={"DatumUraCET": "date_time", "DelovnaMoč": "p", "JalovaMoč": "q" ,"TransformatorskaPostajaSID": "tp_sid", "SMM": "smm",}, inplace=True)
    def handle_names_df_tr(self):
        """"function changes the column names of the dataframe with transformer data, according to code standards
        Args:
        --------
            trafo_data:

                dataframe with transformer data
        """
        self.trafo_data.rename(columns={"DatumUraCET": "date_time", "TransformatorskaPostajaSID": "tp_sid", "SMM": "smm", "SOM_Napetost_L1": "u_1", "SOM_Napetost_L2": "u_2", "SOM_Napetost_L3": "u_3",
                                    "SOM_Delovna_moc_P1": "p_1", "SOM_Delovna_moc_P2": "p_2", "SOM_Delovna_moc_P3": "p_3", "SOM_Jalova_moc_Q1" : "q_1", "SOM_Jalova_moc_Q2" : "q_2", "SOM_Jalova_moc_Q3" : "q_3",
                                    "SOM_Skupna_delovna_moc_P": "p", "SOM_Skupna_jalova_moc_Q": "q",	"SOM_Skupna_navidezna_moc_S": "s"}, inplace=True)

    def crop_to_one_year (self):
        """Crops the data to one year, if data is longer than one year"""
        end_date = self.voltage_data.date_time.max()
        start_date = end_date - pd.Timedelta("365 days")
        self.voltage_data = self.voltage_data[(self.voltage_data.date_time >= start_date)]
        self.power_data = self.power_data[(self.power_data.date_time >= start_date)]
        if self.trafo_data is not None:
            self.trafo_data = self.trafo_data[(self.trafo_data.date_time >= start_date)]


    def create_voltage_pivot_table(self):
        """Resamples the power data down to 10 minutes and then up to 10 minutes, creates pivoted dataframe"""
        pivoted_data = self.voltage_data.pivot_table(index='date_time', columns='smm', values='u_123', aggfunc='mean')
        pivoted_data.index = pd.to_datetime(pivoted_data.index)
        if self.fillna_method != None:
            pivoted_data = pivoted_data.fillna(method=self.fillna_method)
        pivoted_data = pivoted_data.resample('5T').bfill()
        pivoted_data.index = pd.to_datetime(pivoted_data.index) - pd.Timedelta('5T')
        pivoted_data = pivoted_data.resample('10T', label = "right").mean()
        self.df_vol = pivoted_data
    
    def create_power_pivot_table(self):
        """Resamples the power data down to 10 minutes and then up to 10 minutes, creates pivoted dataframe"""
        pivoted_data = self.power_data.pivot_table(index='date_time', columns='smm', values='p', aggfunc='mean')
        pivoted_data.index = pd.to_datetime(pivoted_data.index)
        if self.fillna_method != None:
            pivoted_data = pivoted_data.fillna(method=self.fillna_method)
        pivoted_data = pivoted_data.resample('5T').bfill()
        pivoted_data.index = pd.to_datetime(pivoted_data.index) - pd.Timedelta('5T')
        pivoted_data = pivoted_data.resample('10T', label = "right").mean()
        self.df_p = pivoted_data
    
    def create_reactive_power_pivot_table(self):
        """Resamples the power data down to 10 minutes and then up to 10 minutes, creates pivoted dataframe"""
        pivoted_data = self.power_data.pivot_table(index='date_time', columns='smm', values='q', aggfunc='mean')
        pivoted_data.index = pd.to_datetime(pivoted_data.index)
        if self.fillna_method != None:
            pivoted_data = pivoted_data.fillna(method=self.fillna_method)
        pivoted_data = pivoted_data.resample('5T').bfill()
        pivoted_data.index = pd.to_datetime(pivoted_data.index) - pd.Timedelta('5T')
        pivoted_data = pivoted_data.resample('10T', label = "right").mean()
        self.df_q = pivoted_data
    
    def resample_trafo_data(self):
        """Resamples the transformer data up to 10 minutes, creates dataframe"""

        if self.df_trafo is None:
            return None        
        else:
            trafo_df = self.df_trafo.copy()
            trafo_df.index = pd.to_datetime(trafo_df.date_time)
            trafo_df["u_123"] = trafo_df[["u_1", "u_2", "u_3"]].mean(axis=1)
            trafo_df = trafo_df[["u_123"]]
            trafo_df_res = trafo_df.copy()
            trafo_df_res = trafo_df_res[~trafo_df_res.index.duplicated(keep='first')]
            trafo_df_res = trafo_df_res.fillna(method='bfill').resample('5T').bfill()
            trafo_df_res.index = pd.to_datetime(trafo_df_res.index) - pd.Timedelta('5T')
            trafo_df_res = trafo_df_res.resample('10T', label = "right").mean()
            self.df_trafo = trafo_df_res
    
    def create_pivot_tables(self):
        """Creates pivoted dataframes for power, voltage and transformer data"""
        self.create_voltage_pivot_table()
        self.create_power_pivot_table()
        self.create_reactive_power_pivot_table()
        
    def preprocess_trafo_data(self):
        """Preprocesses transformer data"""
        self.resample_trafo_data()

    def preprocess_voltages(self):
        """preprocesses voltages, limits minimal voltage to minimal_vol and maximal difference between phases to max_diff
        If voltage at any phase is below minimal_vol, we consider it faulty and remove it, 
        If difference between phases is above max_diff, we consider it faulty and remove
        """
        df_cop = self.voltage_data.copy()
        df_cop["min_u"] = df_cop[["u_1", "u_2", "u_3"]].min(axis=1)/230
        df_cop["avg_u"] = df_cop[["u_1", "u_2", "u_3"]].mean(axis=1)/230
        df_cop["max_u"] = df_cop[["u_1", "u_2", "u_3"]].max(axis=1)/230
        df_cop["diff_u"] = df_cop["max_u"] - df_cop["min_u"]
        
        df_cop = df_cop[(df_cop["min_u"] >= self.minimal_vol)]
        df_cop = df_cop[(df_cop["diff_u"] <= self.max_diff)]
        df_cop.sort_values(by=["smm", "date_time"], inplace=True)
        self.voltage_data = df_cop
    
    def get_undervoltage_data(self, lim_vol=0.9, remove_single_occurences=True):
        """Asigns dataframe with undervoltage data
        Args:
        --------
            lim_vol:
               Voltage, below which we consider it undervoltage
            remove_single_occurences:
                if True, undervoltage must occur at least twice in a row
        """
        if lim_vol != None:
            undervoltage_data = limit_voltage(self.voltage_data.copy(), lim_vol = lim_vol, average = False)
        undervoltage_data.sort_values(by=["smm", "date_time"], inplace=True)
        undervoltage_data["timedelta"] = undervoltage_data.groupby("smm")["date_time"].diff()
        if remove_single_occurences:
            undervoltage_data_20_min = undervoltage_data[(undervoltage_data["timedelta"] == pd.Timedelta("10 minutes")) | (
                undervoltage_data["timedelta"].shift(-1) == pd.Timedelta("10 minutes"))]
        else:
            undervoltage_data_20_min = undervoltage_data
        self.undervoltage_data = undervoltage_data_20_min
    

    def preprocess_data(self):
        """preprocesses voltage, energy and transformer data, removes faulty voltage data
        Creates pivoted dataframes for energy, voltage and transformer data
        """
        self.handle_names()
        self.crop_to_one_year()
        self.preprocess_voltages()
        self.create_pivot_tables()
        self.preprocess_trafo_data()
        self.get_undervoltage_data()
        return self.voltage_data, self.undervoltage_data, self.df_vol, self.df_p, self.df_q, self.df_trafo	

    