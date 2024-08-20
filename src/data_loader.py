import pandas as pd
import pyodbc
import config


class DataLoader:

    def __init__(self,
                 trafo_name=None,
                 load_manual=False,
                 folder_path=None,
                 start=None,
                 end=None):
        if folder_path is None:
            self.folder_path = config.FOLDER_PATH
        else:   
            self.folder_path = folder_path
        if trafo_name is None:
            self.trafo_name = config.TRAFO_NAME
        else:            
            self.trafo_name = trafo_name
        self.load_manual = load_manual
        self.voltage_data = None
        self.power_data = None
        self._con = None
        self.con_string = config.CON_STRING
        self.start = start
        self.end = end

    def load_trafo_data(self):
        """Loads energy and voltage data for all smms of given trafo network"""
        if self.load_manual:
            self.load_data_manual()
        else:
            self.load_from_sql()
        return self.voltage_data, self.power_data, None

    def load_voltage_data(self):
        """Loads voltage data for all smms of given trafo network"""
        if self.load_manual:
             self.voltage_data = pd.read_csv(self.folder_path + "/napetost.csv",
                                        parse_dates=["DatumUraCET"])
        else:
            self.load_voltage_data_from_sql()
        return self.voltage_data
    
    def load_power_data(self):
        """Loads power data for all smms of given trafo network"""
        if self.load_manual:
            self.power_data = pd.read_csv(self.folder_path + "/energije.csv",
                                      parse_dates=["DatumUraCET"])
        else:
            self.load_powers_from_sql()
        return self.power_data

    def load_data_manual(self):
        """Loads energy and voltage data, and transformer data if available."""
        self.voltage_data = pd.read_csv(self.folder_path + "/napetost.csv",
                                        parse_dates=["DatumUraCET"])
        self.power_data = pd.read_csv(self.folder_path + "/energije.csv",
                                      parse_dates=["DatumUraCET"])
    def load_from_sql(self):
        """Loads energy and voltage data from SQL database."""
        self._con = lambda: pyodbc.connect(self.con_string)
        self.load_powers_from_sql()
        self.load_voltage_data_from_sql()

    

    def load_powers_from_sql(self):
        if self._con == None:
            self._con = lambda: pyodbc.connect(self.con_string)
        """Loads power data for all smms of given trafo network, for the given time period."""
        query = """SELECT [SMM]
            ,[DelovnaMoč]
            ,[JalovaMoč]
            ,[DatumUraCET]
        FROM 
        		[DW_Star].[dbo].[FactKrivuljeNMC] AS mp
        JOIN 
        		[DW_Star].[dbo].[DimTransformatorskaPostaja] AS mpp
        		ON mp.TransformatorskaPostajaSID = mpp.TransformatorskaPostajaSID
        WHERE mpp.TransformatorskaPostajaNaziv like '%{}%'AND DatumVeljavnostiCETID >= '{}' AND DatumVeljavnostiCETID < '{}'
                        ORDER BY DatumUraCET""".format(self.trafo_name,
                                                    self.start, self.end)
        self.power_data = pd.read_sql(query, self._con())

    def load_voltage_data_from_sql(self):
        """Loads voltage data for all smms of given trafo network, for the given time period."""
        if self._con == None:
            self._con = lambda: pyodbc.connect(self.con_string)
        query = """SELECT [SMM]
            ,Napetost_L1
	        ,Napetost_L2
	        ,Napetost_L3
	        ,Napetost_L123
	        ,DatumUraCET
        FROM 
		    [DW_Star].[dbo].[FactKrivuljeNapetostiNMC] AS mp
        JOIN 
		    [DW_Star].[dbo].[DimTransformatorskaPostaja] AS mpp
		    ON mp.TransformatorskaPostajaSID = mpp.TransformatorskaPostajaSID
        WHERE mpp.TransformatorskaPostajaNaziv like '%{}%' AND DatumVeljavnostiCETID >= '{}' AND DatumVeljavnostiCETID < '{}'
                        ORDER BY DatumUraCET""".format(self.trafo_name,
                                                    self.start, self.end)
        self.voltage_data = pd.read_sql(query, self._con())
