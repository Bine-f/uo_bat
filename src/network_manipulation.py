import networkx as nx
import numpy as np
import pandapower as pp


def create_network(json_path):
	net = pp.from_json(json_path)
	return net

def set_volts(snet, state_vol, warn=True, sort=True):
    """Creates a dataframe with measured voltage data from state_vol and powerflow results from snet
    Args:
        --------
        snet:
            network in pandapower format
        state_vol: dict
            dictionary with average measured voltages, keys are smms
        warn: bool
            if True, prints warnings if some data is missing
        sort: bool
            if True, sorts the dataframe by real voltage"""
    volts = snet.load[['bus', 'p_mw', "q_mvar", "smm", "name"]]
    volts["vol_pp"] = 0
    volts["vol_real"] = 0
    for row in snet.load.itertuples():
        bus = row.bus
        vol_pp = snet.res_bus.loc[bus]
        volts.loc[volts.bus == bus, 'vol_pp'] = vol_pp.vm_pu
        smm = row.smm
        try:
            volts.loc[volts.smm == smm, 'vol_real'] = float(state_vol[smm])/230

        except:
            # delete row from volts
            # volts = volts[volts.smm != smm]
            volts.loc[volts.smm == smm, 'vol_real'] = np.nan
            if warn:
                print("Manjka realen podatek o napetosti za smm: ", smm)
    if sort:
        volts.sort_values(by=['vol_real'], ascending=True, inplace=True)
    return volts


def run_powerflow(snet, res_factor=1., trafo_lv=0.4):
    """Runs powerflow on snet with adjusted resistance factor and transformer voltage
    Makes sure that the original values are restored in case of an error
    Args:
        --------
        snet:
            network in pandapower format
        res_factor: float
            resistance factor for lines
        trafo_lv: float
            transformer voltage
    """
    if not np.isfinite(res_factor) or not np.isfinite(trafo_lv):
       
        raise Exception("res_factor or trafo_lv is not finite")
    orig_lv = snet.trafo['vn_lv_kv'].copy()
    # print("orig_lv: ", orig_lv)
    snet.line['r_ohm_per_km'] = snet.line['r_ohm_per_km'] * res_factor
    snet.line['x_ohm_per_km'] = snet.line['x_ohm_per_km'] * res_factor
    snet.trafo['vn_lv_kv'] = trafo_lv
    try:
        pp.runpp(snet, numba=False)
        snet.trafo['vn_lv_kv'] = orig_lv
        snet.line['r_ohm_per_km'] = snet.line['r_ohm_per_km'] / res_factor
        snet.line['x_ohm_per_km'] = snet.line['x_ohm_per_km'] / res_factor
    except:
        snet.trafo['vn_lv_kv'] = orig_lv
        snet.line['r_ohm_per_km'] = snet.line['r_ohm_per_km'] / res_factor
        snet.line['x_ohm_per_km'] = snet.line['x_ohm_per_km'] / res_factor
        raise Exception("Powerflow did not converge")
    

def populate_snet(snet, state_p, state_q, warn=True):
    """Populate powers in the network load based on measured powers in state_p
    Args:
        --------
        snet:
            network in pandapower format
        state_p: dict
            dictionary with measured powers in kW, keys are smms
        state_q: dict
            dictionary with measured reactive powers
        warn: bool
            if True, prints warnings if some data is missing"""
    smms = snet.load.smm.unique()
    snet.load["p_mw"] = 0
    snet.load["q_mvar"] = 0
    for smm in smms:
        try:
            p_mw = float(state_p[smm])
            if np.isnan(p_mw):
                p_mw = 0
            snet.load.loc[snet.load.smm == smm, 'p_mw'] = p_mw/1000
        except:
            if warn:
                print("manjka moc za smm: ", smm)
            snet.load.loc[snet.load.smm == smm, 'p_mw'] = 0
        try:
            q_mvar = float(state_q[smm])
            if np.isnan(q_mvar):
                q_mvar = 0
            snet.load.loc[snet.load.smm == smm, 'q_mvar'] = q_mvar/1000
        except:
            if warn:
                print("manjka jalova moc za smm: ", smm)
            snet.load.loc[snet.load.smm == smm, 'q_mvar'] = 0
