from fastapi import FastAPI, HTTPException
import pandas as pd
import time
import warnings
from datetime import datetime, timedelta

from data_loader import DataLoader
from preprocess import Preprocess
from models.trafo_model import TrafoModel
from models.feeder_model import FeederModel
from models.battery_model import BatteryModel
import config

warnings.filterwarnings('ignore')

app = FastAPI()

# Get the current time and calculate the date range for one year
now = datetime.now()
last_midnight = now.replace(hour=0, minute=0, second=0, microsecond=0)
one_year_ago = last_midnight - timedelta(days=365)

@app.get("/")
def read_root():
    return {"message": "Welcome to the Trafo Processing API"}

def sanitize_df(df):
    # Replace NaN, inf, -inf with None (which is JSON null)
    return df.replace([float('inf'), float('-inf')], float('nan')).replace({float('nan'): None})

@app.post("/get_results/")
@app.get("/get_results/")
def get_results(
                number_of_trafos: int = None):
    """
    Finds transformer candidates and determines optimal battery parameters for each feeder.
     Args:
        -------- 
          
            number_of_trafos: int
                number of transformers to process

        Returns:
        --------
          
            dictionary with results for feeders, where battery is needed
            
    """
    try:

        dl = DataLoader()
        if number_of_trafos is None:
            trafos_list = dl.find_trafo_candidates()
        else:
            trafos_list = dl.find_trafo_candidates()[:number_of_trafos]
        # trafos_list = ["T348- TAVČARJEVA"]
        print(trafos_list)
        # Initialize empty dataframes to store results
    
        battery_res = pd.DataFrame()
        time0 = time.time()

        # Loop through each transformer in the trafo list
        for TRAFO_NAME in trafos_list:
            print(TRAFO_NAME)
           
            trafo_name = TRAFO_NAME

            try:
                # DataLoader loads the voltage and power data
                dl = DataLoader(load_manual=False,
                                trafo_name=trafo_name,
                                start=one_year_ago,
                                end=last_midnight)

                voltage_data = dl.load_voltage_data()
                pr = Preprocess(voltage_data)
                voltage_data, undervoltage_data, trafo_suitable_for_battery = pr.preprocess_voltage_data_get_undervoltages(
                )

                # If trafo is suitable for battery, process power data
                if trafo_suitable_for_battery:
                    power_data = dl.load_power_data()
                    df_vol, df_p, df_q = pr.preprocess_powers_create_pivot_tables(
                        power_data)

                    tm = TrafoModel(voltage_data, undervoltage_data, df_vol,
                                    df_p, df_q, trafo_name, config.NET_PATH)
                    tm.create_and_populate_snet()

                    for feeder in tm.feeders:
                        fm = FeederModel(tm, feeder)
                        fm.calculate_uv_data_and_slopes()


                        if fm.suitable_for_battery:
                            bm = BatteryModel(fm)
                            bm.calculate_battery_parameters()
                            battery_res = pd.concat(
                                [battery_res, fm.feeder_res],
                                ignore_index=True)

            
            
            except Exception as e:
               print(e, "Error processing transformer "+ trafo_name)
            
            sanitized_battery_res = sanitize_df(battery_res)                
            battery_res.to_csv("battery_res.csv")
        
        
        print(f"Processing completed in {time.time() - time0} seconds.")
        return sanitized_battery_res.to_dict(
            orient="records")
       
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
