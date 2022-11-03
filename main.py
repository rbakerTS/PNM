import os
import time

import geopandas as gpd
import pandas as pd
import pickle
import json
from datetime import date, datetime
import tzlocal
from AGO_Manager import AGO_manager
from arcgis.features import GeoAccessor
from shapely.geometry import Point


# field_mapping = {
#     "": "OBJECTID",
#     "": "CREATIONUSER",
#     "": "DATECREATED",
#     "": "DATEMODIFIED",
#     "": "LASTUSER",
#     "": "COMMENTS",
#     "": "HYPERLINK",
#     "": "ATTACHMENTDATE",
#     "": "ATTACHMENTHEIGHT",
#     "": "ATTACHMENTTYPE",
#     "": "ATTACHMENTOWNER",
#     "": "GROUNDSPACEOWNER",
#     "": "ATTACHMENTCOUNT",
#     "": "SUPPORTSTRUCTUREOBJECTID",
#     "": "POLEFACILITYID",
#     "": "TRANSMISSIONTOWEROBJECTID",
#     "": "SUBTYPECD",
#     "": "FOOTAGE_OCCUPIED",
#     "": "PHOTO",
#     "": "NEEDSTOTRANSFER",
# }


class UpdateDashboard:
    def __init__(self, target, sql_select, sql_from, sql_where, poles, ago_table_id):
        self.sql_select = sql_select
        self.sql_from = sql_from
        self.sql_where = sql_where
        self.target = target
        self.poles = poles
        self.ago_table_id = ago_table_id
        self.sql_engine = 'mssql+pyodbc://TS-TYLER/PNM_2021_Pole_Attachment_Audit?trusted_connection=yes&driver=SQL+Server+Native+Client+11.0'
        with open('secrets.json') as file:
            x = json.load(file)

        username = x['username']
        password = x['password']

        self.manager = AGO_manager(username, password)
        self.local_timezone = tzlocal.get_localzone()
        self.date = date.today()
        self.now = datetime.strftime(datetime.now(), '%Y%m%d%H%M')
        self.results_folder = f'results/{self.now}'
        os.makedirs(self.results_folder, exist_ok=True)

    def get_qaqc(self):
        print("_______________________________________________________________________________________________________")
        print(datetime.strftime(datetime.now(), '%Y/%m/%d %H:%M:%S'))
        print("Getting PNM QAQC v2 data")
        if self.sql_where != '':
            sql_query = f"SELECT {self.sql_select} FROM {self.sql_from} WHERE {self.sql_where}"
            self.qaqc_df = pd.read_sql(sql_query, con=self.sql_engine)
        else:
            sql_query = f"SELECT {self.sql_select} FROM {self.sql_from}"
            self.qaqc_df = pd.read_sql(sql_query, con=self.sql_engine)
        self.qaqc_df['Status'] = 'QAQC'
        self.qaqc_df.columns = map(str.lower, self.qaqc_df.columns)
        self.qaqc_feeders = list(set(self.qaqc_df['feederid']))
        print(f"{len(self.qaqc_df)} QAQC'd poles")
        return self.qaqc_df

    def create_qaqc_sdf(self):
        print("_______________________________________________________________________________________________________")
        print(datetime.strftime(datetime.now(), '%Y/%m/%d %H:%M:%S'))
        print("Converting QAQC poles into sdf")
        self.qaqc_df['geometry'] = self.qaqc_df.apply(lambda x: Point((float(x.longitude), float(x.latitude))), axis=1)
        self.qaqc_sdf = gpd.GeoDataFrame(self.qaqc_df, geometry='geometry')
        # self.qaqc_sdf = GeoAccessor.from_xy(self.qaqc_df, 'longitude', 'latitude')
        # self.qaqc_df['SHAPE'] = '"spatialReference": {"wkid": 4326}, {"x":' + self.qaqc_df['Longitude'].astype(
        #     'str') + ', "y":' + self.qaqc_df['Latitude'].astype('str') + '}'
        print(f"{len(self.qaqc_sdf)} poles in QAQC sdf")
        # return self.qaqc_sdf

    def create_qaqc_csv(self):
        print("_______________________________________________________________________________________________________")
        print(datetime.strftime(datetime.now(), '%Y/%m/%d %H:%M:%S'))
        print("Converting QAQC poles sdf to csv")
        csv_name = f'{self.results_folder}/pnm_poles_qaqc_{self.now}.csv'
        self.qaqc_csv = self.qaqc_sdf.to_csv(csv_name, index=False)
        print(f"Generated {csv_name}")
        # return self.qaqc_csv
        pass

    def get_poles(self):
        print("_______________________________________________________________________________________________________")
        print(datetime.strftime(datetime.now(), '%Y/%m/%d %H:%M:%S'))
        print("Getting PNM poles from AGO")
        success = False
        while success == False:
            try:
                self.search_results = self.manager.content_search(title_search=self.target, max_items=10000,
                                                                  item_type='Feature Service')
                self.search_items_trimmed = []
                for item in self.search_results['items']:
                    if self.target == item['title']:
                        self.search_items_trimmed.append(item)
                self.search_count_trimmed = len(self.search_items_trimmed)
                self.search_items = self.search_items_trimmed
                self.search_count = len(self.search_items)
                self.target_item = self.search_items[0]
                if self.search_count == 0:
                    search = input(
                        f'Search parameter {self.target} returned 0 results. Input an alternative search term: ')
                    self.search_results = self.manager.content_search(title_search=search, max_items=10000,
                                                                      item_type='Feature Service')
                    self.search_items_trimmed = []
                    for item in self.search_results['items']:
                        if search == item['title']:
                            self.search_items_trimmed.append(item)
                    self.search_count_trimmed = len(self.search_items_trimmed)
                    self.search_items = self.search_items_trimmed
                    self.search_count = len(self.search_items)
                    self.target_item = self.search_items[0]
                self.poles_df = self.target_item.layers[0].query().sdf
                self.poles_df.columns = self.poles_df.columns.str.lower()
                # self.poles_feeders = list(set(self.poles_df['feederid']))
                print(f"{len(self.poles_df)} total poles")
                # return self.poles_df
                success = True
            except Exception as e:
                print(e)
                try_again = input("Try again? (Y/N):")
                if try_again.lower().strip() == 'y':
                    time.sleep(30)
                    print("Trying again after 30 seconds.")
                elif try_again.lower().strip() == 'n':
                    print("Exiting script.")
                    quit()
                else:
                    try_again = input("Try again? (Y/N):")
                    if try_again.lower().strip() == 'y':
                        time.sleep(30)
                        print("Trying again after 30 seconds.")
                    elif try_again.lower().strip() == 'n':
                        print("Exiting script.")
                        quit()
                    else:
                        print("Invalid response. Exiting script.")
                        quit()
        pass

    def create_backup(self):
        print("_______________________________________________________________________________________________________")
        print(datetime.strftime(datetime.now(), '%Y/%m/%d %H:%M:%S'))
        print("Creating backup of current PNM poles.")
        try:
            print(self.poles_df.head())
        except:
            self.get_poles()
        backup_name = f"backups/PNM_AGO_Poles_{self.now}.csv"
        self.poles_df.to_csv(backup_name, index=False)
        print(f"Created {backup_name}")

    def update_poles(self):
        print("_______________________________________________________________________________________________________")
        print(datetime.strftime(datetime.now(), '%Y/%m/%d %H:%M:%S'))
        print("Updating poles with QAQC'd data")
        self.raw_df = self.poles_df
        for feeder in self.qaqc_feeders:
            self.raw_df.drop(self.raw_df[self.raw_df['feederid'] == feeder].index, inplace=True)
        # self.no_qaqc_feeders = list(set(self.poles_df['feederid']))
        self.poles_updated_df = pd.concat([self.raw_df, self.qaqc_df], ignore_index=True, join='inner')
        # self.poles_updated_df = self.poles_updated_df.drop(columns='SHAPE')
        # self.poles_updated_feeders = list(set(self.poles_updated_df['feederid']))
        print(f"{len(self.poles_updated_df)} poles after update")
        # return self.poles_updated_df

    def create_update_sdf(self):
        print("_______________________________________________________________________________________________________")
        print(datetime.strftime(datetime.now(), '%Y/%m/%d %H:%M:%S'))
        print("Converting poles into sdf")
        self.poles_updated_sdf = GeoAccessor.from_xy(self.poles_updated_df, 'longitude', 'latitude')
        print(f"{len(self.poles_updated_sdf)} poles in update sdf")
        # return self.poles_updated_sdf
        pass

    def upload_updated_poles(self):
        print("_______________________________________________________________________________________________________")
        print(datetime.strftime(datetime.now(), '%Y/%m/%d %H:%M:%S'))
        print("Uploading updated poles to AGO")

        poles_csv = f'{self.results_folder}/pnm_poles_for_dash_{self.now}.csv'
        self.poles_updated_df.to_csv(poles_csv, index=False)
        self.poles_name = f'PNM_Poles_v2_table_{self.now}'
        self.poles_table = self.manager.content.add(item_properties={'type': 'CSV', 'title': {self.poles_name}},
                                                    data=poles_csv)
        self.poles_layer = self.target_item.layers[0]
        self.poles_layer.manager.truncate()
        param = self.manager.content.analyze(item=self.poles_table.id, file_type='csv')
        table_fields = []
        for field in param['publishParameters']['layerInfo']['fields']:
            table_fields.append(field['name'])
        layer_fields = []
        for field in self.poles_layer.properties.fields:
            layer_fields.append(field['name'])
        self.poles_layer.append(item_id=self.poles_table.id, upload_format='csv',
                                source_table_name='PNM_Poles_v2_table_202203020833',
                                source_info=param['publishParameters'])
        print(f"{len(self.poles_layer)} poles in fc")
        # return self.poles_layer

    def update_from_csv(self):
        csv_search = self.manager.content_search(title_search='PNM_Poles_v2_table', item_type='CSV')
        self.poles_table = None
        for item in csv_search['items']:
            # if '202203021222' in item.title:
            if str(self.date).replace("-", "") in item.title:
                self.poles_table = item
                break
        if self.poles_table == None:
            print("Did not find today's update PNM poles csv. Using AGO itemID variable.")
            self.poles_table = self.manager.content.get(self.ago_table_id)
        self.get_poles()
        self.poles_layer = self.target_item.layers[0]
        self.poles_layer.manager.truncate()
        param = self.manager.content.analyze(item=self.poles_table.id, file_type='csv')
        table_fields = []
        for field in param['publishParameters']['layerInfo']['fields']:
            table_fields.append(field['name'])
        layer_fields = []
        for field in self.poles_layer.properties.fields:
            layer_fields.append(field['name'])
        self.poles_layer.append(item_id=self.poles_table.id, upload_format='csv',
                                source_info=param['publishParameters'])
        print(f"{len(self.poles_layer)} poles in fc")
        pass

    def insert_poles(self):
        print("_______________________________________________________________________________________________________")
        print(datetime.strftime(datetime.now(), '%Y/%m/%d %H:%M:%S'))
        print("Getting PNM poles from csv")

        self.search_results = self.manager.content_search(title_search=self.target, max_items=10000,
                                                          item_type='Feature Service')
        self.search_items_trimmed = []
        for item in self.search_results['items']:
            if self.target == item['title']:
                self.search_items_trimmed.append(item)
        self.search_count_trimmed = len(self.search_items_trimmed)
        self.search_items = self.search_items_trimmed
        self.search_count = len(self.search_items)
        self.target_item = self.search_items[0]
        if self.search_count == 0:
            search = input(
                f'Search parameter {self.target} returned 0 results. Input an alternative search term: ')
            self.search_results = self.manager.content_search(title_search=search, max_items=10000,
                                                              item_type='Feature Service')
            self.search_items_trimmed = []
            for item in self.search_results['items']:
                if search == item['title']:
                    self.search_items_trimmed.append(item)
            self.search_count_trimmed = len(self.search_items_trimmed)
            self.search_items = self.search_items_trimmed
            self.search_count = len(self.search_items)
            self.target_item = self.search_items[0]
        self.poles_df = pd.read_csv(self.poles)
        self.poles_df.columns = self.poles_df.columns.str.lower()
        print(f"{len(self.poles_df)} total poles")


class PNM_Attachers:
    def __init__(self, pnm_attachers, pnm_supports, pnm_transmissions):
        self.pnm_attachers = pnm_attachers
        self.pnm_supports = pnm_supports
        self.pnm_transmissions = pnm_transmissions
        self.pnm_attachers_df = pd.read_pickle(self.pnm_attachers)
        self.pnm_supports_df = pd.read_pickle(self.pnm_supports)
        self.pnm_transmissions_df = pd.read_pickle(self.pnm_transmissions)
        pass

    def join_poles_to_attachers(self):
        self.pnm_attachers_supports_df = pd.merge(self.pnm_attachers_df, self.pnm_supports_df, on="LEGACYID")
        self.pnm_attachers_poles_df = pd.merge(self.pnm_attachers_supports_df, self.pnm_transmissions_df, on="LEGACYID")
        pass

    def rename_fields(self):
        pass


if __name__ == '__main__':
    # pnm = PNM_Attachers(
    #     pnm_attachers='attachers_pk.pickle',
    #     pnm_supports='support_pk.pickle',
    #     pnm_transmissions='transmission_pk.pickle'
    # )
    # pnm.join_poles_to_attachers()
    # pnm.rename_fields()

    dash = UpdateDashboard(
        target='PNM_Poles_v2_Dashboard',
        sql_select='[Region],[Division],[Facilityid],[Height],[Class],[Usagetype],[Feederid],[Owner],[Latitude],[Longitude],[Feeder],[PNM_Attachment],[Attacher_1],[Attach_Type_1],[Attach_Count_1],[Attach_Tagged_1],[Attacher_2],[Attach_Type_2],[Attach_Count_2],[Attach_Tagged_2],[Attacher_3],[Attach_Type_3],[Attach_Count_3],[Attach_Tagged_3],[Attacher_4],[Attach_Type_4],[Attach_Count_4],[Attach_Tagged_4],[Attacher_5],[Attach_Type_5],[Attach_Count_5],[Attach_Tagged_5],[Attacher_6],[Attach_Type_6],[Attach_Count_6],[Attach_Tagged_6],[Attacher_7],[Attach_Type_7],[Attach_Count_7],[Attach_Tagged_7],[Attacher_8],[Attach_Type_8],[Attach_Count_8],[Attach_Tagged_8],[Attacher_9],[Attach_Type_9],[Attach_Count_9],[Attach_Tagged_9],[Attacher_10],[Attach_Type_10],[Attach_Count_10],[Attach_Tagged_10],[Customer_Hardware],[Pole_not_in_Field],[Double Wood],[Maintenance_Items],[Comments],[Field Name],[Image_1],[Date Collected],[Kat_Job_Link]',
        sql_from='[PNM_2021_Pole_Attachment_Audit].[dbo].[tblKatapultPoleQAQCData_v2]',
        sql_where="Feederid = 'HONDALE_11' or Feederid = 'HONDALE_12' or Feederid = 'HONDALE_12_B' or Feederid = 'GOLD_11' or Feederid = 'GOLD_11_B' or Feederid = 'GOLD_12' or Feederid = 'GOLD_13' or Feederid = 'HERMANAS_11' or Feederid = 'HERMANAS_12' or Feederid = 'HERMANAS_13' or Feederid = 'HERMANAS_14' or Feederid = 'DEMING EAST_12' or Feederid = 'DEMING WEST_11' or Feederid = 'DEMING WEST_12' or Feederid = 'DEMING WEST_13' or Feederid = 'SILVER CITY_10' or Feederid = 'SILVER CITY_10_B' or Feederid = 'SILVER CITY_16' or Feederid = 'SILVER CITY_17' or Feederid = 'SILVER CITY_17_B' or Feederid = 'SILVER CITY_18' or Feederid = 'SILVER CITY_18_B' or Feederid = 'TYRONE_17' or Feederid = 'TYRONE_95' or Feederid = 'BURRO MOUNTAIN_60' or Feederid = 'BURRO MOUNTAIN_87' or Feederid = 'LORDSBURG_0' or Feederid = 'LORDSBURG_18' or Feederid = 'LORDSBURG_18_B' or Feederid = 'CLIFF_32' or Feederid = 'CLIFF_33' or Feederid = 'NORTH SILVER CITY_61' or Feederid = 'NORTH SILVER CITY_61_B' or Feederid = 'NORTH SILVER CITY_65' or Feederid = 'MD #1_11' or Feederid = 'MD #1_11_B' or Feederid = 'MD #1_12' or Feederid = 'MD #1_12_B' or Feederid = 'MD #1_12_C' or Feederid = 'MD #1_13' or Feederid = 'MD #1_14' or Feederid = 'MD #1_26' or Feederid = 'MD #1_30' or Feederid = 'ALAMOGORDO 1_1' or Feederid = 'ALAMOGORDO 1_11' or Feederid = 'ALAMOGORDO 1_11_B' or Feederid = 'ALAMOGORDO 1_12' or Feederid = 'ALAMOGORDO 1_13' or Feederid = 'ALAMOGORDO 1_2' or Feederid = 'ALAMOGORDO 1_3' or Feederid = 'ALAMOGORDO 1_83' or Feederid = 'ALAMOGORDO 1_83_B' or Feederid = 'ALAMOGORDO 1_89' or Feederid = 'ALAMOGORDO 2_10' or Feederid = 'TULAROSA_12' or Feederid = 'TULAROSA_13' or Feederid = 'TULAROSA_14' or Feederid = 'TULAROSA_73' or Feederid = 'TULAROSA_9' or Feederid = 'COCHITI_11' or Feederid = 'COCHITI_12' or Feederid = 'KEWA_11' or Feederid = 'KAISER_11' or Feederid = 'LA BAJADA_11' or Feederid = 'CAMEL TRACKS_11' or Feederid = 'STATE PEN_11' or Feederid = 'STATE PEN_12' or Feederid = 'STATE PEN_13' or Feederid = 'STATE PEN_13_B' or Feederid = 'BECKNER_11' or Feederid = 'BECKNER_12' or Feederid = 'BECKNER_13' or Feederid = 'RODEO_12' or Feederid = 'RODEO_13' or Feederid = 'RODEO_14' or Feederid = 'ZAFARANO_11' or Feederid = 'ZAFARANO_12' or Feederid = 'ZAFARANO_13' or Feederid = 'ZAFARANO_14' or Feederid = 'SANTO DOMINGO_11'",
        poles=r'C:\Users\TechServPC\PycharmProjects\PNMAttachers\backups/PNM_AGO_Poles_202203011619.csv',
        ago_table_id='515e0e8917d3444b9439133ed9d16f89'
    )
    dash.get_qaqc()
    dash.create_qaqc_sdf()
    dash.create_qaqc_csv()
    dash.get_poles()
    # dash.create_backup()
    dash.insert_poles()
    dash.update_poles()
    dash.create_update_sdf()
    dash.upload_updated_poles()

    # dash.update_from_csv()

    # dash.get_poles()
    # dash.create_backup()
    pass
