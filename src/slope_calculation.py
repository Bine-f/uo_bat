import warnings

import networkx as nx
import numpy as np
import pandapower as pp
import scipy.optimize as opt
import pandas as pd

warnings.filterwarnings('ignore')
from network_manipulation import run_powerflow, set_volts, populate_snet
from plotting import plot_volts, plot_feeder_volts


def get_opt_res_f(snet, min_bus, id_first, state_vol, x0=0.2, t0=0.4):
    """
    Optimizes the resistance factor to match the voltage difference between min_bus and id_first 
    with real data voltage differences.

    Args:
    --------
        snet:
            network in pandapower format
        min_bus:
            bus with the minimum voltage, or last bus in the feeder
        id_first:
            bus in path from transformer to min_bus with the first measured voltage
        state_vol:
            dictionary of measured voltages
        x0:
            initial guess for the resistance factor
        t0:
            initial guess for the transformer voltage level
    Returns:
    --------
        res.root:
            result of the optimization process
    """    
    trafo_lv = t0
    def get_delta_delta(res_factor):
        run_powerflow(snet, res_factor=res_factor, trafo_lv=trafo_lv)
        volts = set_volts(snet, state_vol, warn=False)
        vol_min_bus_pp = volts[volts["bus"] == min_bus].vol_pp.values[0]
        vol_first_pp = volts[volts["bus"] == id_first].vol_pp.values[0]
        delta_v_pp = float(vol_first_pp - vol_min_bus_pp)
        vol_min_bus_real = volts[volts["bus"] == min_bus].vol_real.values[0]
        vol_first_real = volts[volts["bus"] == id_first].vol_real.values[0]
        delta_v_real = float(vol_first_real - vol_min_bus_real)
        delta_delta = delta_v_real - delta_v_pp
        return delta_delta
    res = opt.root_scalar(get_delta_delta, x0=x0, x1=x0 + 0.1, method='secant')
 
    return res.root

def get_opt_trafo_lv(snet, res_factor, bus, state_vol, t0=0.4):
    """
    Optimizes the trafo voltage level to match the simulated voltage at bus with the real data voltage.

    Args:
    --------
        snet:
            network in pandapower format
        bus:
            bus at which we want to match the simulated voltage with the real data voltage
        state_vol:
            dictionary of measured voltages
        res_factor:
            resistance factor used in the powerflow calculation
        t0:
            initial guess for the transformer voltage level
    Returns:
    --------
        opt_trafo_lv.root:
            result of the optimization process
    """    
    trafo_lv = t0
    def calculate_volts_diff_first_smm(trafo_lv):        
        run_powerflow(snet, res_factor=res_factor, trafo_lv=trafo_lv)
        volts = set_volts(snet, state_vol, warn=False)
        difference = volts[volts["bus"] == bus]["vol_real"].values[0] - \
            volts[volts["bus"] == bus]["vol_pp"].values[0]
        return difference
    opt_trafo_lv = opt.root_scalar(
        calculate_volts_diff_first_smm, method="secant", x0=t0, x1=trafo_lv + 0.05, xtol=0.00002)
    
    return opt_trafo_lv.root


def is_id_suitable(id, snet, volts):
    "Checks if bus id is suitable for calibration, i.e. if it is in the"
    "network,has voltage data and has 3 phases"
    row = volts[volts["bus"] == id]
    try:
        phases = snet.load.loc[snet.load.bus == id].phases.values[0]
    except:
        phases = 3
    return len(row) > 0 and phases == 3


def find_id_first(snet, state_vol, min_bus):
    """
    Finds the first suitable bus in the path from the transformer to the min bus.

    Args:
    --------
        snet:
            network in pandapower format
        state_vol:
            dictionary of measured voltages
        min_bus:
            bus with the minimum voltage, or last bus in the feeder
    Returns:
    --------
        id_first:
            first suitable bus in the path from the transformer to the min bus
    """
    id_first_found = False
    i = 0
    run_powerflow(snet, res_factor=0.3, trafo_lv=0.425)
    volts = set_volts(snet, state_vol, warn=False)
    min_bus = list(volts.bus)[0]
    tr = snet.bus[snet.bus["aclass_id"] == "TR"] 
    tr_bus = tr.index[0]  
    mg = pp.topology.create_nxgraph(snet, respect_switches=True)  
    # Create path from transformer to min bus
    pth = nx.shortest_path(mg, source=tr_bus, target=min_bus) 
    while not id_first_found:
        id = pth[i]
        if is_id_suitable(id, snet, volts):
            id_first = id
            id_first_found = True
        else:
            # id is not suitable, check neighbours
            vol_id = snet.res_bus.loc[id].vm_pu
            neighbours = list(mg.adj[id])
            delta_vols = []
            suitable_neighbours = []
            for neighbour_id in neighbours:
                if is_id_suitable(neighbour_id, snet, volts):
                    vol_neighbour = snet.res_bus.loc[neighbour_id].vm_pu
                    delta_vols.append(np.abs(vol_neighbour - vol_id))
                    suitable_neighbours.append(neighbour_id)
            if len(delta_vols) > 0 and min(delta_vols) < 0.001:
                # If there are suitable neighbours, with very similar voltage as bus in path
                # choose the one with the smallest voltage difference
                id_first = suitable_neighbours[np.argsort(delta_vols)[0]]
                id_first_found = True
            else:
                i += 1
    if not id_first_found:
        raise ValueError("No suitable bus found in path from transformer to min bus")
    return id_first



def find_min_bus(snet, state_vol, volts):
    "Finds bus with lowest measured voltage that is suitable for calibration"
    smms_sorted = state_vol.sort_values().index
    tries = 0
    while tries < len(smms_sorted):
        try:
            # Find smm with lowest measured voltage that is in snet
            smm_min = smms_sorted[tries]

            min_bus_candidate = snet.load[snet.load.smm ==
                                          smm_min].bus.values[0]
            if is_id_suitable(min_bus_candidate, snet, volts):
                min_bus = min_bus_candidate
                return min_bus
            else:
                tries += 1
        except:
            tries += 1
            continue
    return None


def calculate_difference_sum(volts, smm_list):
    "Calculates sum of absolute values of differences between real and simulatedvoltages for all smms in smm_list"
    volts_feeder = volts.loc[volts["smm"].isin(smm_list)]
    return (volts_feeder.vol_pp - volts_feeder.vol_real).abs().mean()

def get_opt_res_f(snet, state_vol, smms_feeder, x0=1., t0=0.425):
    """
    Optimizes the resistance factor to minimize the difference between real and simulated voltages for all smms in the feeder.

    Args:
    --------
        snet:
            network in pandapower format
        state_vol:
            dictionary of measured voltages
        smms_feeder:
            list of smms in the feeder
        x0:
            initial guess for the resistance factor
        t0:
            initial guess for the transformer voltage level
    Returns:
    --------
        res.x[0]:
            result of the optimization process
    """
    def get_difference_sum_res_f(res_f):
       
        run_powerflow(snet, res_f[0], t0)
      
        volts = set_volts(snet, state_vol, warn = False)
        return calculate_difference_sum(volts, smms_feeder)
    res = opt.minimize(get_difference_sum_res_f, x0, method='Nelder-Mead')
    
    return res.x[0]

def calibrate_snet(snet, state_vol, smms_feeder, plot=False, x0=1., t0=0.425, calculate_res_f=True):
    """
    Calculates the optimal resistance factor and transformer voltage level for the network.

    Args:
    --------
        snet:
            network in pandapower format
        state_vol:
            dictionary of measured voltages
        smms_feeder:
            list of smms in the feeder
        plot:
            if True, plots the voltages before and after calibration
        x0:
            initial guess for the resistance factor
        t0:
            initial guess for the transformer voltage level
        calculate_res_f:
            if True, calculates the resistance factor, otherwise uses x0
    Returns:
    --------
        opt_trafo_lv:
            optimal transformer voltage level
        opt_res_f:
            optimal resistance factor
    """
    if plot:
        run_powerflow(snet, res_factor=x0, trafo_lv=opt_trafo_lv)
        volts = set_volts(snet, state_vol, warn=False)
        plot_feeder_volts(volts, smms_feeder,
                   title="Before calibration")
    run_powerflow(snet, res_factor=x0, trafo_lv = t0)
    volts = set_volts(snet, state_vol, warn=False)
    min_bus = find_min_bus(snet, state_vol, volts)    
    id_first = find_id_first(snet, state_vol, min_bus)    
    if calculate_res_f == True and len(smms_feeder) > 2:
        try:
            #Calculate res_f using min_bus and id_first
            opt_res_f = get_opt_res_f(snet, min_bus, id_first, state_vol, x0=x0, t0 = t0)
        except:
            opt_res_f = 1.

        if opt_res_f < 0.7 or opt_res_f > 1.3:
            #If we get weird results, try to calibrate res_f using all smms in feeder
            try:
               
                opt_res_f = get_opt_res_f(snet, state_vol, smms_feeder, x0, t0)
            except:
                opt_res_f = 1.
            if plot:
                run_powerflow(snet, res_factor=opt_res_f, trafo_lv=t0)
                volts = set_volts(snet, state_vol, warn=False)
                plot_feeder_volts(volts, smms_feeder, title="After res_f calibration")
        else:
            if plot:
                run_powerflow(snet, res_factor=opt_res_f, trafo_lv = t0)
                volts = set_volts(snet, state_vol, warn=False)
                plot_feeder_volts(volts, smms_feeder, min_bus, id_first,
                           title="After res_f calibration")
    else:
        opt_res_f = 1.
    try:    
        
        opt_trafo_lv = get_opt_trafo_lv(snet, opt_res_f, min_bus, state_vol, t0)
    except:
        opt_trafo_lv = t0
        print("Trafo_lv optimization failed")
    if plot:
        run_powerflow(snet, res_factor=opt_res_f, trafo_lv=opt_trafo_lv)
        volts = set_volts(snet, state_vol, warn=False)
        plot_feeder_volts(volts, smms_feeder, title="After trafo_lv calibration")
    return opt_trafo_lv, opt_res_f


def calculate_slopes(snet, battery_smms, dates, smms_feeder, df_p, df_q, df_vol, df_trafo=None, N_of_dates=4, plot = False):
    """
    Calculates difference of voltage, when power is decreased by 1 kW at smms at battery_smms.

    For multiple dates function runs powerflow with measured power data and stores simulated voltages.
    Then it decreases power by 1 kW at smms in battery_smms and calculates the difference of voltage, which is called slope, and has units V/kW.
    Use average of slopes over multiple dates for better results.
    Args:
    --------
        snet:
            network in pandapower format
        battery_smms:
            list of smms for which the slope is calculated
        dates:
            list of dates suitable for calculation of slopes
        smms_feeder
            list of all smms in the feeder
        df_p:
            dataframe with average power for all smms
        df_q: 
            dataframe with average reactive power for all smms
        df_vol:
            dataframe with average voltage for all smms
        df_trafo:
            dataframe with transformator voltage and power data
        N_of_dates:
            number of dates used for calculation of slopes
        plot:
            if True, plots calibration process
    Returns:
    --------
        slopes_smms:
            dataframe with smms for which slopes are calculated as columns, all smms in smms_feeder as index and slopes as rows   
    """    
    # choose dates for calibration and slope calculation
    dates_cal_index = np.arange(
        len(dates)//(N_of_dates + 1), len(dates), len(dates)//(N_of_dates + 1))[:-1]
    dates_cal = [dates[i] for i in dates_cal_index]
    slopes_smms = pd.DataFrame()
    for battery_smm in battery_smms:
        # in slope df we save slopes for different dates for one battery smm
        slope_df = pd.DataFrame()
        slope_df["smm"] = snet.load.smm
        for i in range(len(dates_cal)):
            date = dates_cal[i]
            try:
                state_tr = df_trafo.loc[date]
                snet.ext_grid["vm_pu"] = state_tr.values[0]/230
            except:
                pass
            state_p = df_p.loc[date]
            state_vol = df_vol.loc[date]
            state_q = df_q.loc[date]        
            populate_snet(snet, state_p, state_q, warn=False)
            # calibration            
            opt_trafo_lv, res_f = calibrate_snet(
                snet, state_vol, smms_feeder, x0=1., t0=0.425, plot=plot)
            run_powerflow(snet, res_factor=res_f, trafo_lv=opt_trafo_lv) 
            # saving initial voltages, simulated with measured power data           
            volts_0 = set_volts(snet, state_vol, warn=False)
            # decreasing power by 1 kW at battery smm
            snet.load.loc[snet.load.smm == battery_smm, 'p_mw'] -= 0.001
            run_powerflow(snet, res_factor=res_f, trafo_lv=opt_trafo_lv)
            # saving simulated voltages after power decrease
            volts_1 = set_volts(snet, state_vol, warn=False)
            # calculating difference of voltage
            volts_diff = (volts_1["vol_pp"] - volts_0["vol_pp"])*230
            volts_diff = volts_diff.to_frame()
            volts_diff["bus"] = volts_0["bus"]
            volts_diff["smm"] = volts_0["smm"]
            volts_diff.rename(columns={"vol_pp": "vol_diff"}, inplace=True)
            slope_df["slope"+str(i)] = 0
            # populating slope_df with calculated slopes
            for slope_smm in slope_df.smm:
                try:
                    vol_slope = volts_diff[volts_diff.smm ==
                                           slope_smm].vol_diff.values[0]
                    slope_df.loc[slope_df.smm == slope_smm,
                                 "slope"+str(i)] = vol_slope
                except:
                    vol_slope = np.nan
                    slope_df.loc[slope_df.smm == slope_smm,
                                 "slope"+str(i)] = vol_slope
        # populating slopes_smms with average slopes for battery smm
        slopes_smms[str(battery_smm)] = sum(slope_df["slope"+str(i)]
                                       for i in range(len(dates_cal)))/len(dates_cal)
    slopes_smms.index = slope_df.smm
    return slopes_smms