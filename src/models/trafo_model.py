import pandas as pd
import pandapower as pp
import networkx as nx
from utils import *
from subnet_creation import Subnet

class TrafoModel:
    def __init__(self, voltage_data, undervoltage_data, overvoltage_data, df_vol, df_p, df_q,  trafo_name, network_path):
        self.voltage_data = voltage_data
        self.undervoltage_data = undervoltage_data
        self.df_vol = df_vol
        self.df_p = df_p
        self.df_q = df_q
        self.trafo_name = trafo_name
        self.network_path = network_path
        self.feeders = None
        self.trafo_res_df = pd.DataFrame()
        self.snet = None
       
        self.overvoltage_data = overvoltage_data
        if self.voltage_data is not None:
            self.enough_voltage_data = self.is_there_enough_voltage_data()

    
    def create_snet(self):
        """Create pandapower network for trafo"""
        net = create_network(self.network_path)
        subnet = Subnet(net)
        self.snet = subnet.create_subnet_from_TP(self.trafo_name)

    def populate_snet_feeders_phases(self):
        """Populates snet.load with feeder name and number of phases"""
        self.populate_snet_feeders()
        self.populate_snet_phases()
        self.feeders = self.snet.load.feeder.unique()

    def populate_snet_feeders(self):
        """Adds feeder column to snet.load based on the path from transformer to bus"""
        tr = self.snet.bus[self.snet.bus["aclass_id"] == "TR"]  # 4
        tr_bus = tr.index[0]  # 4   đ
        feeders = []
        mg = pp.topology.create_nxgraph(self.snet, respect_switches=True)  # 4
        # Create path from transformer to min bus
        for bus in self.snet.load.bus:
            pth = nx.shortest_path(mg, source=tr_bus, target=bus)  # 4
            lines = self.snet.line[self.snet.line["from_bus"].isin(
                pth) & self.snet.line["to_bus"].isin(pth)]  # 4
            # Every path from trafo to bus contains a line with name IZV, that is feeder name
            feeder = lines[lines.name.str.contains("IZV")].name.values[0]  # 4
            feeders.append(feeder)
        self.snet.load["feeder"] = feeders

    def populate_snet_phases(self):
        """Adds phases column to snet.load based on the number of phases in the data

        Args:
            --------
            snet:
                network in pandapower format
            voltage_data: pd.DataFrame
                dataframe with voltage data for all smms in trafo network"""
        phs = []
        for smm in self.snet.load.smm:
            try:
                df_vol_smm = self.voltage_data.loc[self.voltage_data["smm"] == smm]
                one_phase = df_vol_smm.u_2.isna().sum() > (len(df_vol_smm)//2) or df_vol_smm.u_3.isna(
                ).sum() > (len(df_vol_smm)//2) or df_vol_smm.u_3.isna().sum() > (len(df_vol_smm)//2)
                if one_phase:
                    phs.append(1)
                else:
                    phs.append(3)
            except:
                phs.append(0)
        self.snet.load["phases"] = phs

    def create_and_populate_snet(self):
        """Creates subnet of trafo network and populates it with feeders and phases for each row"""
        if self.snet is None:
            self.create_snet()
        self.populate_snet_feeders_phases()

    def percentage_of_voltage_data(self):
        """Calculates precentage of smms, for which we have voltage data"""
        smms_voltage = self.voltage_data.smm.unique()
        smms_snet = self.snet.load.smm.unique()
        return len(smms_voltage)/len(smms_snet)
    
    def is_there_enough_voltage_data(self):
        """Returns True if we have voltage data for more than 90% of smms"""
        if self.snet is None:
            self.create_snet()
        return self.percentage_of_voltage_data() > 0.6
        
