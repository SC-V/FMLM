import streamlit as st
import pydeck as pdk
import pandas
import psycopg2
import io
import datetime
import dateutil.parser
import time
from pytz import timezone
from google.oauth2 import service_account

st.set_page_config(page_title="First Mile Monitor", layout="wide")

gbq_credentials = service_account.Credentials.from_service_account_info(st.secrets["gcp_service_account"])

FILE_BUFFER_REPORT = io.BytesIO()


@st.cache_resource
def init_connection():
    return psycopg2.connect(**st.secrets["postgres"])


connection = init_connection()


@st.cache_data(ttl=600)
def get_historical_orders(query):
    with connection.cursor() as cursor:
        cursor.execute(query)
        return cursor.fetchall()


def refactor_lo_code(row):
    if not pandas.isna(row["lo_code"]):
        row["lo_code"] = "LO-" + str(row["lo_code"])
    return row


def normalize_coordinates(row):
    row["lat"] = float(row["lat"])
    row["lon"] = float(row["lon"])
    return row


def normalize_tariffs(row):
    if pandas.isna(row["tariff"]):
        row["tariff"] = "Unknown"
    elif row["tariff"] == 0:
        row["tariff"] = "SDD"
    else:
        row["tariff"] = "NDD"
    return row


def set_barcode_image(row):
    barcode = row["scanned_barcode_value"]
    row["scannable_qr"] = rf"http://qrcoder.ru/code/?{barcode}&4&0"
    return row


def set_status(row):
    if pandas.isna(row["barcode"]):
        row["pick_status"] = "Missing"
    elif pandas.isna(row["claim_id"]):
        row["pick_status"] = "Picked"
    else:
        row["pick_status"] = "Received"
    return row


@st.cache_data(ttl=600)
def get_scan_frame(date_limit):

    if date_limit:
        try:
            date_from, date_to = date_limit
        except:
            date_from = date_limit[0]
            date_to = None
        if date_to:
            query_date_filter = rf"WHERE DATETIME(timestamp, 'America/Santiago') BETWEEN '{date_from}' AND '{date_to}'"
        else:
            query_date_filter = rf"WHERE DATETIME(timestamp, 'America/Santiago') >= '{date_from}'"
    else:
        query_date_filter = ""  # ._seconds
    query = rf"""
        SELECT
            DATETIME(timestamp, "America/Santiago") scan_dttm,
            json_value(data, '$.corp_client_id') corp_client_id,
            json_value(data, '$.courier_uuid') courier_uuid,
            json_value(data, '$.scanned_barcode') scanned_barcode_value,
            json_value(data, '$.store_name') store_name,
            json_value(data, '$.scan_location._latitude') lat,
            json_value(data, '$.scan_location._longitude') lon
        FROM `yango-pick-mvp.scan_events_firestore_export.scan_events_raw_latest`
        {query_date_filter}
        ORDER BY timestamp DESC
        ;
        """
    scan_frame_ = pandas.read_gbq(query, credentials=gbq_credentials)
    return scan_frame_


st.markdown(f"# First Mile Monitor")

# Get historical orders
proxy_orders = get_historical_orders(rf"""
    SELECT 
        client_order_number,
        routing_order_number,
        market_order_id,
        request_id,
        claim_id,
        tariff,
        logistic_status,
        claim_status,
        created_at AT TIME ZONE 'America/Santiago',
        client_id
    FROM orders
    WHERE is_redelivery = FALSE AND created_at BETWEEN '2023-08-01' AND '2023-08-31';
""")

proxy_frame = pandas.DataFrame(proxy_orders,
                               columns=["barcode", "external_id", "lo_code", "request_id", "claim_id",
                                        "tariff", "platform_status", "cargo_status", "created_at", "proxy_client_id"])
proxy_frame = proxy_frame[~proxy_frame.external_id.str.contains("_RETRY_")]

col_date, col_time, _, col_scanned, col_errors = st.columns([1, 1, 0.3, 0.3, 0.3])
col_courier, col_client, col_tariff, col_status = st.columns(4)

with col_date:
    date_limit = st.date_input("Search date >= left date, < right date\n(offset +1 day to include the date)",
                               value=(datetime.date.today(), datetime.date.today() + datetime.timedelta(days=1)),
                               format="YYYY-MM-DD")

with col_time:
    interval_start, interval_end = st.select_slider(
        'Search time [between] – disabled',
        options=['00', '01', '02', '03', '04', '05', '06', '07', '08', '09', '10',
                 '11', '12', '13', '14', '15', '16', '17', '18', '19', '20', '21',
                 '22', '23', '24'],
        value=('09', '20'),
        disabled=True)

scan_frame = get_scan_frame(date_limit)

merged_frame = pandas.merge(scan_frame, proxy_frame, how="left", left_on="scanned_barcode_value", right_on="barcode")
merged_frame = merged_frame.apply(lambda row: set_barcode_image(row), axis=1)
merged_frame = merged_frame.apply(lambda row: normalize_coordinates(row), axis=1)
merged_frame = merged_frame.apply(lambda row: normalize_tariffs(row), axis=1)
merged_frame = merged_frame.apply(lambda row: refactor_lo_code(row), axis=1)
merged_frame = merged_frame.apply(lambda row: set_status(row), axis=1)

with col_courier:
    couriers = st.multiselect("Courier =", merged_frame["courier_uuid"].unique())

with col_client:
    clients = st.multiselect("Client =", merged_frame["corp_client_id"].unique())

with col_tariff:
    tariffs = st.multiselect("Tariff =", merged_frame["tariff"].unique())

with col_status:
    filtered_statuses = st.multiselect("Status =", merged_frame["pick_status"].unique())

with col_scanned:
    scan_events = st.metric("Scan events #", len(merged_frame))

with col_errors:
    scan_errors = st.metric("Match errors #", len(merged_frame[merged_frame["pick_status"] == "Missing"]))

if couriers:
    merged_frame = merged_frame[merged_frame['courier_uuid'].isin(couriers)]

if clients:
    merged_frame = merged_frame[merged_frame['corp_client_id'].isin(clients)]

if tariffs:
    merged_frame = merged_frame[merged_frame['tariff'].isin(tariffs)]

if filtered_statuses:
    merged_frame = merged_frame[merged_frame['pick_status'].isin(filtered_statuses)]

visible_frame = merged_frame[["scan_dttm", "created_at", "scanned_barcode_value", "pick_status", "tariff",
                              "corp_client_id", "store_name", "courier_uuid", "external_id", "lo_code", "claim_id",
                              "scannable_qr",]]
st.dataframe(visible_frame,
             use_container_width=True,
             column_config={
                 "scannable_qr": st.column_config.ImageColumn(
                     "scannable_qr", help="Barcode generator", width="medium"
                 )
             })

if st.button("Reload data"):
    st.cache_data.clear()

with pandas.ExcelWriter(FILE_BUFFER_REPORT, engine='xlsxwriter') as writer:
    visible_frame.to_excel(writer, sheet_name='pick_report')
    writer.close()

    TODAY = datetime.datetime.now(timezone("America/Santiago")).strftime("%Y-%m-%d")
    st.download_button(
        label="Download log",
        data=FILE_BUFFER_REPORT,
        file_name=f"status_orders_{TODAY}.xlsx",
        mime="application/vnd.ms-excel"
    )

# st.pydeck_chart(pdk.Deck(
#     map_style=None,
#     # height=500,
#     initial_view_state=pdk.ViewState(
#         latitude=-33.368855,
#         longitude=-70.695044,
#         zoom=10,
#         pitch=0,
#     ),
#     layers=[
#         # geojson_delivery_area,
#         pdk.Layer(
#             'ScatterplotLayer',
#             data=merged_frame,
#             get_position='[lon, lat]',
#             get_color='[88, 24, 69]',
#             get_radius=300,

#             pickable=False
#         ),
#         pdk.Layer(
#             'ScatterplotLayer',
#             get_position=[-70.6945098, -33.3688048],
#             get_color='[0, 128, 255, 160]',
#             get_radius=1000,
#             pickable=True
#         )
#     ],
# ))
