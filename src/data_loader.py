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
        con = self._con()
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
        con.close()

    def load_voltage_data_from_sql(self):
        """Loads voltage data for all smms of given trafo network, for the given time period."""
        if self._con == None:
            self._con = lambda: pyodbc.connect(self.con_string)
        con = self._con()
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
        self.voltage_data = pd.read_sql(query, con)
        con.close()

    def find_trafo_candidates(self):
        """Creates a list of trafos that have undervoltages, and could be suitable for battery installation.
        Returns a list of trafos that have at least 4 undervoltage events with at least 20 min of undervoltages in a row,
        and data is not faulty"""
        if self._con == None:
            self._con = lambda: pyodbc.connect(self.con_string)
        con = self._con()
        query = """WITH VoltageEvents AS (
            SELECT 
                mpp.TransformatorskaPostajaNaziv,
                mp.DatumUraCET,
                CASE 
                    WHEN COALESCE(mp.Napetost_L1, 208) < 207 THEN 1
                    WHEN COALESCE(mp.Napetost_L2, 208) < 207 THEN 1
                    WHEN COALESCE(mp.Napetost_L3, 208) < 207 THEN 1
                    ELSE 0
                END AS LowVoltageEvent,
                CASE 
                    WHEN COALESCE(mp.Napetost_L1, -1) >= COALESCE(mp.Napetost_L2, -1) AND COALESCE(mp.Napetost_L1, -1) >= COALESCE(mp.Napetost_L3, -1) THEN mp.Napetost_L1
                    WHEN COALESCE(mp.Napetost_L2, -1) >= COALESCE(mp.Napetost_L1, -1) AND COALESCE(mp.Napetost_L2, -1) >= COALESCE(mp.Napetost_L3, -1) THEN mp.Napetost_L2
                    ELSE mp.Napetost_L3
                END AS MaxVoltage,
                CASE 
                    WHEN COALESCE(mp.Napetost_L1, 999999) <= COALESCE(mp.Napetost_L2, 999999) AND COALESCE(mp.Napetost_L1, 999999) <= COALESCE(mp.Napetost_L3, 999999) THEN mp.Napetost_L1
                    WHEN COALESCE(mp.Napetost_L2, 999999) <= COALESCE(mp.Napetost_L1, 999999) AND COALESCE(mp.Napetost_L2, 999999) <= COALESCE(mp.Napetost_L3, 999999) THEN mp.Napetost_L2
                    ELSE mp.Napetost_L3
                END AS MinVoltage
            FROM 
                [DW_Star].[dbo].[FactKrivuljeNapetostiNMC] AS mp
            JOIN 
                [DW_Star].[dbo].[DimTransformatorskaPostaja] AS mpp
                ON mp.TransformatorskaPostajaSID = mpp.TransformatorskaPostajaSID
            WHERE 
                mp.DatumVeljavnostiCETID >= '2023-03-03 00:00:00'
                AND mp.DatumVeljavnostiCETID < '2024-03-03 00:00:00'
            )
            , EventPairs AS (
                SELECT 
                    ve1.TransformatorskaPostajaNaziv,
                    ve1.DatumUraCET AS EventTime1,
                    ve2.DatumUraCET AS EventTime2
                FROM 
                    VoltageEvents ve1
                JOIN 
                    VoltageEvents ve2
                    ON ve1.TransformatorskaPostajaNaziv = ve2.TransformatorskaPostajaNaziv
                    AND ve1.DatumUraCET <> ve2.DatumUraCET
                    AND ABS(DATEDIFF(MINUTE, ve1.DatumUraCET, ve2.DatumUraCET)) = 10
                WHERE 
                    ve1.LowVoltageEvent = 1
                    AND ve2.LowVoltageEvent = 1
                    AND (ve1.MaxVoltage - ve1.MinVoltage) < 30
                    AND ve1.MinVoltage > 170
            )
            SELECT 
                TransformatorskaPostajaNaziv,
                COUNT(DISTINCT EventTime1) AS EventCountWithNearbyInstances
            FROM 
                EventPairs
            GROUP BY 
                TransformatorskaPostajaNaziv
            HAVING 
                COUNT(DISTINCT EventTime1) >= 4
            ORDER BY 
                EventCountWithNearbyInstances DESC, TransformatorskaPostajaNaziv""".format(
            self.start, self.end)
        trafos_df = pd.read_sql(query, con)
        con.close()
        trafos_list = list(trafos_df["TransformatorskaPostajaNaziv"])
        trafos_list.reverse()
        return trafos_list
