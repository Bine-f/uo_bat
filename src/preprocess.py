import pandas as pd
from utils import *

class Preprocess:
    def __init__(self, voltage_data, power_data = None):
        self.voltage_data = voltage_data
        self.power_data = power_data
       
        self.minimal_vol = 150/230
        self.max_diff = 50/230
        self.fillna_method = None
        self.df_vol = None
        self.df_p = None
        self.df_q = None
        


    def handle_names(self):
        """function changes the column names of the dataframes with energy and voltage data, according to code standards
        Args:
        --------
            power_data:
                dataframe with energy data
            voltage_data:
                dataframe with voltage data
            
        """
        self.handle_voltage_data_names()
        self.handle_power_data_names()
       

    def handle_voltage_data_names(self):
        """function changes the column names of the dataframe with energy data, according to code standards
        Args:
        --------
            voltage_data:
                dataframe with voltage data  
        """    
        self.voltage_data.rename(columns={"DatumUraCET": "date_time","Napetost_L1": "u_1", "Napetost_L2": "u_2", "Napetost_L3": "u_3", 
                                     "Napetost_L123": "u_123", "SMM": "smm","TransformatorskaPostajaSID": "tp_sid"}, inplace=True)
    def handle_power_data_names(self):
        """function changes the column names of the dataframe with energy data, according to code standards
        Args:
        --------
            power_data:
                dataframe with energy
        """
        self.power_data.rename(columns={"DatumUraCET": "date_time", "DelovnaMoč": "p", "JalovaMoč": "q" ,"TransformatorskaPostajaSID": "tp_sid", "SMM": "smm",}, inplace=True)
    
    def crop_to_one_year (self):
        """Crops the data to one year, if data is longer than one year"""
        self.end_date = self.voltage_data.date_time.max()
        self.start_date = self.end_date - pd.Timedelta("365 days")
        self.voltage_data = self.voltage_data[(self.voltage_data.date_time >= self.start_date)]
        self.power_data = self.power_data[(self.power_data.date_time >= self.start_date)]

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
    
    # def resample_trafo_data(self):
    #     """Resamples the transformer data up to 10 minutes, creates dataframe"""

    #     if self.df_trafo is None:
    #         return None        
    #     else:
    #         trafo_df = self.df_trafo.copy()
    #         trafo_df.index = pd.to_datetime(trafo_df.date_time)
    #         trafo_df["u_123"] = trafo_df[["u_1", "u_2", "u_3"]].mean(axis=1)
    #         trafo_df = trafo_df[["u_123"]]
    #         trafo_df_res = trafo_df.copy()
    #         trafo_df_res = trafo_df_res[~trafo_df_res.index.duplicated(keep='first')]
    #         trafo_df_res = trafo_df_res.fillna(method='bfill').resample('5T').bfill()
    #         trafo_df_res.index = pd.to_datetime(trafo_df_res.index) - pd.Timedelta('5T')
    #         trafo_df_res = trafo_df_res.resample('10T', label = "right").mean()
    #         self.df_trafo = trafo_df_res
    
    def create_pivot_tables(self):
        """Creates pivoted dataframes for power, voltage and transformer data"""
        self.create_voltage_pivot_table()
        self.create_power_pivot_table()
        self.create_reactive_power_pivot_table()
        
    def preprocess_powers_create_pivot_tables(self, power_data):
        """preprocesses power data, removes faulty power data, creates pivoted dataframes for power data
        Args:
        --------
            power_data:
                dataframe with power data
        """
        self.power_data = power_data
        self.handle_power_data_names()
        self.crop_to_one_year()
        self.create_pivot_tables()
        return self.df_vol, self.df_p, self.df_q

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

    def is_trafo_suitable_for_battery(self):
        """Returns True if there are more than 4 datetimes with undervoltage in voltage data"""
        if self.undervoltage_data is None:
            return False
        else:
            return len(self.undervoltage_data["date_time"].unique()) >= 4

    def preprocess_data(self):
        """preprocesses voltage, energy and transformer data, removes faulty voltage data
        Creates pivoted dataframes for energy, voltage and transformer data
        """
        self.handle_names()
        self.crop_power_data()
        self.preprocess_voltages()
        self.create_pivot_tables()        
        self.get_undervoltage_data()
        return self.voltage_data, self.undervoltage_data, self.df_vol, self.df_p, self.df_q
    
    def crop_voltage_data(self):
        """Crops voltage data to one year"""
        self.end_date = self.voltage_data.date_time.max()
        self.start_date = self.end_date - pd.Timedelta("365 days")
        self.voltage_data = self.voltage_data[(self.voltage_data.date_time >= self.start_date)]
    
    def crop_power_data(self):
        """Crops power data to one year"""
        self.power_data = self.power_data[(self.power_data.date_time >= self.start_date)]
        
    def remove_smm_from_voltage_and_undevoltage_data(self, smm):
        """Removess smm from voltage and undervoltage data"""
        self.voltage_data = self.voltage_data[self.voltage_data.smm != smm]
        self.undervoltage_data = self.undervoltage_data[self.undervoltage_data.smm != smm]
    
    def remove_asymetric_smms(self):
        """Checks if there are smms in undervoltage data, that have too asymetric voltages, to be real. Removes them"""
        smm_counts = self.undervoltage_data["smm"].value_counts()
        uv_smms = smm_counts[smm_counts > 300].index 
        # We find smms that have a lot of undervoltage data, and check if they have too asymetric voltages
        for smm in uv_smms:
            smm_data = self.voltage_data[self.voltage_data["smm"] == smm]
            avg1, avg2, avg3 = np.average(smm_data.u_1), np.average(smm_data.u_2), np.average(smm_data.u_3)
            sorted_averages = np.sort([avg1, avg2, avg3])
            # If difference between average of two highest phases and lowest phase is too high, we remove the whole smm data
            if (sorted_averages[1] + sorted_averages[2])/2 - sorted_averages[0] > 7:
                self.remove_smm_from_voltage_and_undevoltage_data(smm)
                print(f"Removed smm {smm} from voltage and undervoltage data")

    def preprocess_voltage_data_get_undervoltages(self):
        """preprocesses voltage data, removes faulty voltage data, finds undervoltage data, determines if trafo is suitable for battery
        """
        self.handle_voltage_data_names()
        # self.crop_to_one_year()
        self.crop_voltage_data()
        self.preprocess_voltages()
        self.get_undervoltage_data()
        self.remove_asymetric_smms()
        suitable_for_battery = self.is_trafo_suitable_for_battery()
        return self.voltage_data, self.undervoltage_data, suitable_for_battery

    