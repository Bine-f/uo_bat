import pandapower as pp
import pandapower.topology as top


class Subnet():
    def __init__(self, net):
        self.net = net
        self.subnet = None

    def filter_connected_components(self, cc):
        comps = []
        for c in cc:
            if len(c)>1:
                comps.append(c)
        return comps

    def create_subnet_from_smm(self, smm):
        assert smm in self.net.load.smm.values.tolist(), 'The provided SMM id does not exist in the network!'
        #if smm not in self.net.load.smm.values.tolist():
            #bus_id = self.net.load.loc[self.net.load.smm==smm ,'bus'].iloc[0]
        nx_net = top.create_nxgraph(self.net)
        cc = top.connected_components(nx_net)
        cc_filtered = self.filter_connected_components(cc)
        try:
            bus_id = self.net.load.loc[self.net.load.smm==smm ,'bus'].iloc[0]
        except:
            print('Mapping from the particular SMM id to bus_id does not exist!')
        for comp in cc_filtered:
            if bus_id in comp:
                subnet = pp.select_subnet(self.net, comp)
                break
        return subnet
    
    def create_subnet_from_bus(self, bus):
        assert bus in self.net.bus.index.values.tolist(), 'The provided bus_id does not exist in the network!'
        nx_net = top.create_nxgraph(self.net)
        cc = top.connected_components(nx_net)
        cc_filtered = self.filter_connected_components(cc)
        for comp in cc_filtered:
            if bus in comp:
                subnet = pp.select_subnet(self.net, comp)
                #TODO: To reset values, you first have to reset bus indexes and save the mapping, then map loads, lines, trafor and ext_grid using the mapping
                # subnet.bus = subnet.bus.reset_index()
                break
        return subnet
    
    def create_subnet_from_TP(self, TP):
        tps = self.net.trafo.loc[self.net.trafo.name.str.contains(TP)]
        if len(tps)>1:
            print(f'There are more than one transformers with the provided TP name: {TP}')
            return None
        elif len(tps)==0:
            print(f'There is no transformer with the provided TP name: {TP}')
            return None
        else:
            trafo_lv_bus = tps.lv_bus.values[0]
            return self.create_subnet_from_bus(trafo_lv_bus)
    
    def set_subnet(self, subnet):
        self.subnet = subnet

if __name__ == "__main__":
    PATH = "/Users/blazdobravec/Documents/WORK/INTERNI-PROJEKTI/PLANNINGAPP/DTool/data/EG_LV.json"
    bus = 10
    net = pp.from_json(PATH)
    snet = Subnet(net)
    subnet = snet.create_subnet_from_TP("T0629")
    print(subnet)