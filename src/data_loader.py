import pandas as pd


class DataLoader:
    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.voltage_data = None
        self.power_data = None
        self.trafo_data = None
    
    def load_trafo_data(self):
        """Loads energy and voltage data, and transformer data if available."""
        self.voltage_data = pd.read_csv(self.folder_path + "/napetost.csv", parse_dates=["DatumUraCET"])
        self.power_data = pd.read_csv(self.folder_path + "/energije.csv", parse_dates=["DatumUraCET"])
        try:
            self.trafo_data = pd.read_csv(self.folder_path + "/energije_napetosti.csv", parse_dates=["DatumUraCET"])
        except:
            self.trafo_data = None
        return self.voltage_data, self.power_data, self.trafo_data