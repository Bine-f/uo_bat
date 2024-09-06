import pandas as pd
import numpy as np
from tqdm import tqdm
from models.feeder_model import FeederModel

class BatteryModel:
    def __init__(self, fm: FeederModel):
        self.fm = fm
        self.tm = self.fm.tm
        self.battery_smm = fm.battery_smm
        self.voltage_data = fm.voltage_data
        self.undervoltage_data = fm.undervoltage_data
        self.slopes = fm.slopes
        self.vol_lim = 207/230
        self.smms = fm.smms
        self.fix_all_phases = True
        self.battery_capacity = None
        self.battery_power = None
        self.battery_powers = None
        self.battery_cycles = None
        self.max_energy_start_date = None
        self.battery_df = pd.DataFrame()
        self.powers_with_charging = True

    def calculate_battery_powers(self):
        """Calculates battery operating schedule for given dates and battery smm
        Function returns list of powers, and list of dates, where the battery is needed to solve undervoltages.
        Powers are calculated using slopes of the battery smm."""
        
        powers_slope = []
        dates_uv = []
        dates = np.sort(self.undervoltage_data.date_time.unique())
        for date in tqdm(self.voltage_data.date_time.unique()):
            if date in dates:
                vol_state = self.voltage_data.loc[self.voltage_data.date_time == date]
                powers_list = []
                for smm in self.smms:
                    # get slope of smm we are fixing
                    vol_slope = self.slopes[str(self.battery_smm)][smm]
                    # Get voltage deviation from vol_lim for each phase, if a phase is missing in the data, it is not fixed
                    try:                    
                        vol1 = vol_state.loc[vol_state.smm ==
                                             smm].u_1.values[0]
                        vol1_diff = self.vol_lim*230 - vol1
                    except:
                        vol1 = self.vol_lim
                        vol1_diff = 0
                    try:
                        vol2 = vol_state.loc[vol_state.smm ==
                                             smm].u_2.values[0]
                        vol2_diff = self.vol_lim*230 - vol2
                        if not np.isfinite(vol2):
                            vol2 = self.vol_lim
                            vol2_diff = 0

                    except Exception as e:
                        vol2 = self.vol_lim
                        vol2_diff = 0
                    try:
                        vol3 = vol_state.loc[vol_state.smm ==
                                             smm].u_3.values[0]
                        vol3_diff = self.vol_lim*230 - vol3
                        if not np.isfinite(vol3):
                            vol3 = self.vol_lim
                            vol3_diff = 0
                    except:
                        vol3 = self.vol_lim
                        vol3_diff = 0
                    # calculate power for each phase
                    p1 = vol1_diff / vol_slope /3
                    p2 = vol2_diff / vol_slope /3
                    p3 = vol3_diff / vol_slope /3
                    if self.fix_all_phases:
                        # if all phases are fixed, we sum the positive powers
                        p = (p1*(p1 > 0) + p2*(p2 > 0) + p3*(p3 > 0))
                        powers_list.append(p)
                    else:
                        # if only average voltage is fixed, we sum all powers
                        powers_list.append(p1 + p2 + p3)
                # For needed power at certain datetime, we take the maximum power from all smms
                sp_max = max(powers_list)
                # If power is unrealistic, we set it to 0
                if sp_max < 100:
                    powers_slope.append(sp_max)
                    dates_uv.append(date)
                else:
                    print("Power is too high, setting to 0")
                    print(sp_max)
                    powers_slope.append(0)
                    dates_uv.append(date)
            else:
                # if we dont have undervoltage, we set power to 0
                dates_uv.append(date)
                powers_slope.append(0)
        self.battery_powers = powers_slope
        self.battery_dates = dates_uv
        self.battery_df = pd.DataFrame(
            {"date_time": dates_uv, "battery_power": powers_slope})
        self.battery_df.set_index("date_time", inplace=True)


    def calculate_battery_powers_with_charging(self):
        """Calculates battery operating schedule for given dates and battery smm
        Function returns list of powers, and list of dates, where the battery is needed to solve undervoltages.
        Powers are calculated using slopes of the battery smm. Function takes state of charge into account, so 
        when battery is not fulll, we are charging it, if possible"""
        
        powers_slope = []
        dates_uv = []
        dates = np.sort(self.undervoltage_data.date_time.unique())
        soc = 0.
        socs = []
        for date in tqdm(self.voltage_data.date_time.unique()):
            if date in dates or soc < 0:
                if date in dates:
                    charging = False
                else:
                    charging = True
                vol_state = self.voltage_data.loc[self.voltage_data.date_time == date]
                powers_list = []
                for smm in self.smms:
                    # get slope of smm we are fixing
                    vol_slope = self.slopes[str(self.battery_smm)][smm]
                    # Get voltage deviation from vol_lim for each phase, if a phase is missing in the data, it is not fixed
                    try:                    
                        vol1 = vol_state.loc[vol_state.smm ==
                                             smm].u_1.values[0]
                        vol1_diff = self.vol_lim*230 - vol1
                    except:
                        vol1 = self.vol_lim
                        vol1_diff = -5000. # Unrealisticly big negative power, so it does not effect results
                    try:
                        vol2 = vol_state.loc[vol_state.smm ==
                                             smm].u_2.values[0]
                        vol2_diff = self.vol_lim*230 - vol2
                        if not np.isfinite(vol2):
                            vol2 = self.vol_lim
                            vol2_diff = -5000.

                    except Exception as e:
                        vol2 = self.vol_lim
                        vol2_diff = -5000.
                    try:
                        vol3 = vol_state.loc[vol_state.smm ==
                                             smm].u_3.values[0]
                        vol3_diff = self.vol_lim*230 - vol3
                        if not np.isfinite(vol3):
                            vol3 = self.vol_lim
                            vol3_diff = -5000.
                    except:
                        vol3 = self.vol_lim
                        vol3_diff = -5000.
                    # calculate power for each phase
                    p1 = vol1_diff / vol_slope /3
                    p2 = vol2_diff / vol_slope /3
                    p3 = vol3_diff / vol_slope /3
                    if not charging:
                        # we have undervoltage
                        if self.fix_all_phases:
                            # if all phases are fixed, we sum the positive powers
                            p = (p1*(p1 > 0) + p2*(p2 > 0) + p3*(p3 > 0))
                            powers_list.append(p)
                        else:
                            # if only average voltage is fixed, we sum all powers
                            powers_list.append(p1 + p2 + p3)
                    else:
                        # we have to charge the battery
                        if self.fix_all_phases:
                            # if all phases are fixed, we sum the negative powers
                            p = (p1*(p1 < 0) + p2*(p2 < 0) + p3*(p3 < 0))
                            powers_list.append(p)
                        else:
                            # if only average voltage is fixed, we sum all powers
                            powers_list.append(p1 + p2 + p3)
                # For needed power at certain datetime, we take the maximum power from all smms
                df_powers_state = pd.DataFrame({"smm": self.smms, "power": powers_list}) 
                sp_max = max(powers_list)
                # If power is unrealistic, we set it to 0
                if sp_max < 150:
                    if charging:
                        if sp_max < soc*6:
                            sp_max = soc*6
                            soc = 0
                        else:
                            soc -= sp_max/6
                    else:
                        soc -= sp_max/6
                    powers_slope.append(sp_max)
                    dates_uv.append(date)
                    socs.append(soc)
                else:
                    print("Power is too high, setting to 0")
                    print(sp_max)
                    powers_slope.append(0)
                    dates_uv.append(date)
                    socs.append(soc)
                
            else:
                #We dont have undervoltage, and we dont have to charge the battery
                dates_uv.append(date)
                powers_slope.append(0)
                socs.append(soc)


                
        self.battery_powers = powers_slope
        self.battery_dates = dates_uv
        self.battery_socs = socs
        self.battery_df = pd.DataFrame(
            {"date_time": dates_uv, "battery_power": powers_slope, "soc": socs})
        self.battery_df.set_index("date_time", inplace=True)



    def get_max_energy(self):
        """Calculates needed battery capacity from power data
        Function calculates biggest integral of powers between two datetimes, where power is zero.
        """
        if self.powers_with_charging:
            self.battery_capacity = -1*min(self.battery_df["soc"])
            self.max_energy_start_date = self.battery_df["soc"].idxmin()

        else:
            max_sum = 0
            current_sum = 0
            in_segment = False
            max_sum_start_date = None
            current_segment_start_date = None

            for idx, value in self.battery_df['battery_power'].items():

                if value == 0:
                    if in_segment:
                        if current_sum > max_sum:
                            max_sum = current_sum
                            max_sum_start_date = current_segment_start_date
                        current_sum = 0
                        in_segment = False
                else:
                    if not in_segment:
                        current_segment_start_date = idx
                        in_segment = True
                    current_sum += value
            self.battery_capacity =  max_sum/6
            self.max_energy_start_date =  max_sum_start_date

    def calculate_battery_parameters(self):
        """Function that calculates battery operating schedule and the battery characteristics"""
        if self.fm.suitable_for_battery:
            print("Calculating battery for", self.fm.feeder_name)
            self.calculate_battery_characteristics()
        self.write_battery_results()
        self.add_feeder_res_to_trafo_res()
        print(self.fm.feeder_name, "done")

        

    def calculate_battery_characteristics(self):
        """
        Function that calculates battery operating schedule and the battery characteristics"""
        if self.powers_with_charging:
            self.calculate_battery_powers_with_charging()
        else:
            self.calculate_battery_powers()

        self.get_max_energy()
        self.get_battery_power()
        self.get_battery_cycles()
        

    def get_battery_power(self):
        """Calculate needed battery power """
        powers = self.battery_df['battery_power']
        self.battery_power = max(powers)

    def get_battery_cycles(self):
        """Calculates number of cycles from power data and battery capacity 
        """
        powers = self.battery_df['battery_power']
        energy = sum(powers*(powers>0))/6
        self.battery_cycles = energy/self.battery_capacity
            
    def write_battery_results(self):
        """Writes battery results to results dataframe"""
        if self.fm.suitable_for_battery:
            self.fm.feeder_res["battery_smm"] = [self.battery_smm]
            self.fm.feeder_res["battery_capacity"] = [self.battery_capacity]
            self.fm.feeder_res["battery_power"] = [self.battery_power]
            self.fm.feeder_res["battery_cycles"] = [self.battery_cycles]
        else:
            self.fm.feeder_res["battery_smm"] = [None]
            self.fm.feeder_res["battery_capacity"] = [None]
            self.fm.feeder_res["battery_power"] = [None]
            self.fm.feeder_res["battery_cycles"] = [None]
    
    def add_feeder_res_to_trafo_res(self):
        """Adds feeder results to transformer results dataframe"""
        self.tm.trafo_res_df = pd.concat([self.tm.trafo_res_df, self.fm.feeder_res], ignore_index=True)
        



   

