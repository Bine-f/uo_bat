NET_PATH = r"C:\Users\binef\EG\uo_bat\src\EG_LV_SMM_28032024.json" 
TRAFO_NAME = "T113- LESCE"
FOLDER_PATH =   r"C:\Users\binef\EG\uo_bat\src\T113- LESCE"
DRIVER = "{SQL Server}"
SERVER = "SRVEGBIDB01P"
DATABASE = "DW_Star"
TRUSTED_CONNECTION = "yes"

CON_STRING = """Driver={};
                Server={};
                Database={};
                Trusted_Connection={};""".format(DRIVER, SERVER, DATABASE,
                                                 TRUSTED_CONNECTION)
