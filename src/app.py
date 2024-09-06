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
def get_results(create_trafo_results: bool = False,
                create_battery_results: bool = True,
                number_of_trafos: int = None):
    """
    Finds transformer candidates and determines optimal battery parameters for each feeder.
     Args:
        -------- 
            create_trafo_results: bool
                if True, results for all feeders will be saved, for each transformer
            create_battery_results: bool
                if True, results for feeders suitable for battery will be saved
            number_of_trafos: int
                number of transformers to process

        Returns:
        --------
            If create_trafo_results is True:
                List of dictionaries, each containing results for a single feeder
            If create_battery_results is True:
                List of dictionaries, each containing results for a single feeder
            If create_trafo_results and create_battery_results are False:
                Dictionary with message "Processing completed"
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
        res_df = pd.DataFrame() if create_trafo_results else None
        battery_res = pd.DataFrame() if create_battery_results else None
        time0 = time.time()

        # Loop through each transformer in the trafo list
        for TRAFO_NAME in trafos_list:
            print(TRAFO_NAME)
            if create_trafo_results:
                trafo_res_df = pd.DataFrame()
            trafo_name = TRAFO_NAME

            try:
                # DataLoader loads the voltage and power data
                dl = DataLoader(load_manual=False,
                                trafo_name=trafo_name[:6],
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

                        if create_trafo_results:
                            bm = BatteryModel(fm)
                            bm.calculate_battery_parameters()
                            trafo_res_df = pd.concat(
                                [trafo_res_df, fm.feeder_res],
                                ignore_index=True)

                        if create_battery_results and fm.suitable_for_battery:
                            bm = BatteryModel(fm)
                            bm.calculate_battery_parameters()
                            battery_res = pd.concat(
                                [battery_res, fm.feeder_res],
                                ignore_index=True)

                else:
                    if create_trafo_results:
                        tm = TrafoModel(voltage_data, undervoltage_data, None,
                                        None, None, trafo_name,
                                        config.NET_PATH)
                        tm.create_and_populate_snet()

                        for feeder in tm.feeders:
                            fm = FeederModel(tm, feeder)
                            fm.calculate_and_write_uv_data(
                                empty_battery_columns=True)
                            trafo_res_df = pd.concat(
                                [trafo_res_df, fm.feeder_res],
                                ignore_index=True)

                if create_trafo_results:
                    res_df = pd.concat([res_df, trafo_res_df],
                                       ignore_index=True)
            
            except Exception as e:
               print(e, "Error processing transformer"+ trafo_name)
            if create_battery_results:
                sanitized_battery_res = sanitize_df(battery_res)                
                battery_res.to_csv("battery_res.csv")
        if create_battery_results:
            sanitized_battery_res = sanitize_df(battery_res)
            print(f"Processing completed in {time.time() - time0} seconds.")
            battery_res.to_csv("battery_res.csv")
            return sanitized_battery_res.to_dict(
                orient="records"), res_df.to_dict(orient="records")
        elif create_trafo_results:
            sanitized_res_df = sanitize_df(res_df)
            print(f"Processing completed in {time.time() - time0} seconds.")
            res_df.to_csv("res_df.csv")
            return sanitized_res_df.to_dict(orient="records")
        print(f"Processing completed in {time.time() - time0} seconds.")
        return {"message": "Processing completed"}

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
