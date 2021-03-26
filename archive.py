from pathlib import Path
import re
from glob import glob
import gzip
from zipfile import ZipFile
from collections import defaultdict
from datetime import datetime
import pandas as pd
import geopandas as gpd
from shapely.geometry import Point, LineString, Polygon

flights_date_items = [
    "FILED OFF BLOCK TIME",
    "FILED ARRIVAL TIME",
    "ACTUAL OFF BLOCK TIME",
    "ACTUAL ARRIVAL TIME",
]
flight_points_date_items = ["Time Over"]
flight_airspaces_date_items = ["Entry Time", "Exit Time"]


def get_df(file, date_items):
    if len(date_items) == 0:
        return pd.read_csv(file)
    return pd.read_csv(file, parse_dates=date_items, infer_datetime_format=True)


def load_info(zip_file_object, info, date_items):
    name = Path(info.filename).name
    gz_file = zip_file_object.extract(info)
    with gzip.open(gz_file) as file:
        df = get_df(file, date_items)
    return (name, df)


def filter_df(df, ids):
    return df[df["ECTRL ID"].isin(ids)]


def get_airports(df, atype):  # df of type flights_df
    gdf = gpd.GeoDataFrame(
        df,
        geometry=gpd.points_from_xy(df[atype + " Longitude"], df[atype + " Latitude"]),
        crs="epsg:4326",
    )
    return gdf[[atype, "geometry"]]


def get_adeps(df):
    return get_airports(df, "ADEP")


def get_adess(df):
    return get_airports(df, "ADES")


def get_all_airports(df):  # df of type flights_df
    a_df_list = []
    a_df_list.append(get_adeps(df).rename(columns={"ADEP": "airport"}))
    a_df_list.append(get_adess(df).rename(columns={"ADES": "airport"}))
    return pd.concat(a_df_list, ignore_index=True)


def get_trajs(df):  # df of type flight_points
    gdf = gpd.GeoDataFrame(
        df, geometry=gpd.points_from_xy(df.Longitude, df.Latitude), crs="epsg:4326",
    )
    gs = gdf.groupby(["ECTRL ID"])["geometry"].apply(lambda x: LineString(x.tolist()))
    return gpd.GeoDataFrame(gs, geometry="geometry", crs="epsg:4326")  # WGS 84


def get_polygon(df, aid):
    sdf = df[df["Airspace ID"] == aid]
    lon_pt_list = list(sdf.Longitude.values)
    lat_pt_list = list(sdf.Latitude.values)
    return Polygon(zip(lon_pt_list, lat_pt_list))


def get_airspaces(df):  # FIR/UIR from firs_df
    fir_dict = defaultdict(lambda: defaultdict(Polygon))
    for aid in list(df["Airspace ID"].unique()):
        fir_dict["geometry"][aid] = get_polygon(df, aid)
    return gpd.GeoDataFrame(fir_dict, crs="epsg:4326")  # WGS 84


class Sample:
    def __init__(self, sample_zip):
        extract = ZipFile(sample_zip[0])
        self.flights_df = get_df(
            extract.open("extract/Flights_extract.csv"), flights_date_items
        )
        self.flight_points_filed_df = get_df(
            extract.open("extract/Flight_Points_Filed_extract.csv"),
            flight_points_date_items,
        )
        self.flight_points_actual_df = get_df(
            extract.open("extract/Flight_Points_Actual_extract.csv"),
            flight_points_date_items,
        )
        self.flight_firs_filed_df = get_df(
            extract.open("extract/Flight_FIRs_Filed_extract.csv"),
            flight_airspaces_date_items,
        )
        self.flight_firs_actual_df = get_df(
            extract.open("extract/Flight_FIRs_Actual_extract.csv"),
            flight_airspaces_date_items,
        )
        self.flight_auas_filed_df = get_df(
            extract.open("extract/Flight_AUAs_Filed_extract.csv"),
            flight_airspaces_date_items,
        )
        self.flight_auas_actual_df = get_df(
            extract.open("extract/Flight_AUAs_Actual_extract.csv"),
            flight_airspaces_date_items,
        )
        self.routes_df = get_df(extract.open("extract/Route_1502_extract.csv"), [])
        self.firs_df = get_df(extract.open("extract/FIR_1502_extract.csv"), [])

    def __repr__(self):
        return self.flights_df.shape


class Archive:
    def __init__(self, archive_path):
        self.archive_name = Path(archive_path).stem
        print("Loading archive: ", self.archive_name)
        zip_file_object = ZipFile(archive_path)
        routed, fired = (False, False)

        for info in zip_file_object.infolist():
            if re.match(r"(.*)Flights(.*)", info.filename):
                (self.flights_name, self.flights_df) = load_info(
                    zip_file_object, info, flights_date_items
                )
                print(f"{self.flights_name} loaded")
            elif re.match(r"(.*)Flight_Points_Filed(.*)", info.filename):
                date_items = ["Time Over"]
                (
                    self.flight_points_filed_name,
                    self.flight_points_filed_df,
                ) = load_info(zip_file_object, info, flight_points_date_items)
                print(f"{self.flight_points_filed_name} loaded")
            elif re.match(r"(.*)Flight_Points_Actual(.*)", info.filename):
                date_items = ["Time Over"]
                (
                    self.flight_points_actual_name,
                    self.flight_points_actual_df,
                ) = load_info(zip_file_object, info, flight_points_date_items)
                print(f"{self.flight_points_actual_name} loaded")
            elif re.match(r"(.*)Flight_FIRs_Filed(.*)", info.filename):
                date_items = ["Entry Time", "Exit Time"]
                (self.flight_firs_filed_name, self.flight_firs_filed_df) = load_info(
                    zip_file_object, info, flight_airspaces_date_items
                )
                print(f"{self.flight_firs_filed_name} loaded")
            elif re.match(r"(.*)Flight_FIRs_Actual(.*)", info.filename):
                date_items = ["Entry Time", "Exit Time"]
                (self.flight_firs_actual_name, self.flight_firs_actual_df) = load_info(
                    zip_file_object, info, flight_airspaces_date_items
                )
                print(f"{self.flight_firs_actual_name} loaded")
            elif re.match(r"(.*)Flight_AUAs_Filed(.*)", info.filename):
                date_items = ["Entry Time", "Exit Time"]
                (self.flight_auas_filed_name, self.flight_auas_filed_df) = load_info(
                    zip_file_object, info, flight_airspaces_date_items
                )
                print(f"{self.flight_auas_filed_name} loaded")
            elif re.match(r"(.*)Flight_AUAs_Actual(.*)", info.filename):
                date_items = ["Entry Time", "Exit Time"]
                (self.flight_auas_actual_name, self.flight_auas_actual_df) = load_info(
                    zip_file_object, info, flight_airspaces_date_items
                )
                print(f"{self.flight_auas_actual_name} loaded")
            elif re.match(r"(.*)Route(.*)", info.filename) and (not routed):
                (self.routes_name, self.routes_df) = load_info(
                    zip_file_object, info, []
                )
                print(f"{self.routes_name} loaded")
                routed = True  # only the first version found
            elif re.match(r"(.*)FIR(.*)", info.filename) and (not fired):
                (self.firs_name, self.firs_df) = load_info(zip_file_object, info, [])
                print(f"{self.firs_name} loaded")
                fired = True  # only the first version found

    def update_from_ids(self, ids):
        self.flights_df = self.flights_df[self.flights_df["ECTRL ID"].isin(ids)]
        self.flight_points_filed_df = self.flight_points_filed_df[
            self.flight_points_filed_df["ECTRL ID"].isin(ids)
        ]
        self.flight_points_actual_df = filter_df(self.flight_points_actual_df, ids)
        self.flight_firs_filed_df = filter_df(self.flight_firs_filed_df, ids)
        self.flight_firs_actual_df = filter_df(self.flight_firs_actual_df, ids)
        self.flight_auas_filed_df = filter_df(self.flight_auas_filed_df, ids)
        self.flight_auas_actual_df = filter_df(self.flight_auas_actual_df, ids)

    def clip(self, fir_list):  # clip to flights that actually crossed one of the FIRs
        print(f"Keeping flights from FIRs: {fir_list}...")
        df = self.flight_firs_actual_df
        ids = list(df[df["FIR ID"].isin(fir_list)]["ECTRL ID"].unique())
        print(f"{len(ids)} flights remaining")
        self.update_from_ids(ids)

    def datetime_filtering(
        self, d_min_str, d_max_str
    ):  # filter on day of actual off block times
        print(f"Filtering flights from {d_min_str} to {d_max_str}...")
        d_min = datetime.strptime(d_min_str, "%Y-%m-%d %H:%M:%S")
        d_max = datetime.strptime(d_max_str, "%Y-%m-%d %H:%M:%S")
        df = self.flights_df
        ids = list(
            df[
                (df["ACTUAL OFF BLOCK TIME"] > d_min)
                & (df["ACTUAL OFF BLOCK TIME"] < d_max)
            ]["ECTRL ID"].unique()
        )
        print(f"{len(ids)} flights remaining")
        self.update_from_ids(ids)

    def distance_filtering(
        self, distance_min, distance_max
    ):  # filter on distance flown in nautical miles
        print(
            f"Keeping flights with distance flown from {distance_min} to {distance_max}..."
        )
        df = self.flights_df
        ids = list(
            df[
                (df["Actual Distance Flown (nm)"] > distance_min)
                & (df["Actual Distance Flown (nm)"] < distance_max)
            ]["ECTRL ID"].unique()
        )
        print(f"{len(ids)} flights remaining")
        self.update_from_ids(ids)

    def adep_filtering(self, adep_list):  # filter on departure airports
        print(f"Keeping flights with following departure airports: {adep_list}...")
        df = self.flights_df
        ids = list(df[df["ADEP"].isin(adep_list)]["ECTRL ID"].unique())
        print(f"{len(ids)} flights remaining")
        self.update_from_ids(ids)

    def ades_filtering(self, ades_list):  # filter on departure airports
        print(f"Keeping flights with following destination airports: {ades_list}...")
        df = self.flights_df
        ids = list(df[df["ADES"].isin(ades_list)]["ECTRL ID"].unique())
        print(f"{len(ids)} flights remaining")
        self.update_from_ids(ids)

    def to_archive(self, archive_name=None):
        if archive_name == None:
            archive_name = self.archive_name + "_filter.zip"

        self.flights_df.to_csv(self.flights_name, index=False, compression="gzip")
        self.flight_points_filed_df.to_csv(
            self.flight_points_filed_name, index=False, compression="gzip"
        )
        self.flight_points_actual_df.to_csv(
            self.flight_points_actual_name, index=False, compression="gzip"
        )
        self.flight_firs_filed_df.to_csv(
            self.flight_firs_filed_name, index=False, compression="gzip"
        )
        self.flight_firs_actual_df.to_csv(
            self.flight_firs_actual_name, index=False, compression="gzip"
        )
        self.flight_auas_filed_df.to_csv(
            self.flight_auas_filed_name, index=False, compression="gzip"
        )
        self.flight_auas_actual_df.to_csv(
            self.flight_auas_actual_name, index=False, compression="gzip"
        )
        self.routes_df.to_csv(self.routes_name, index=False, compression="gzip")
        self.firs_df.to_csv(self.firs_name, index=False, compression="gzip")

        with ZipFile(archive_name, "w") as zip_obj:
            zip_obj.write(self.flights_name)
            zip_obj.write(self.flight_points_filed_name)
            zip_obj.write(self.flight_points_actual_name)
            zip_obj.write(self.flight_firs_filed_name)
            zip_obj.write(self.flight_firs_actual_name)
            zip_obj.write(self.flight_auas_filed_name)
            zip_obj.write(self.flight_auas_actual_name)
            zip_obj.write(self.routes_name)
            zip_obj.write(self.firs_name)

        zip_obj.close()

        # Automatic removal of temp gz files created to generate the zip file
        # for gz_file in self.name_df_map.keys():
        #    file_to_rem = Path(gz_file)
        #    file_to_rem.unlink()

        print(f"File {archive_name} with {self.flights_df.shape[0]} flights saved")

    def __repr__(self):
        return self.archive_name




        
        