import datetime
import requests
from dataclasses import dataclass, asdict
from enum import Enum


CoercedDicts = list[dict[str, str | int | float | bool | datetime.date | datetime.time]]


class ResponseError(Exception):
    pass


@dataclass
class DailySummary:
    date: datetime.date = None
    station: str = None
    avg_temp: int = None
    min_temp: int = None
    max_temp: int = None
    precipitation: float = None
    snowfall: float = None
    snow_depth: float = None
    cloudiness_midnight_to_midnight: float = None
    cloudiness_sunrise_to_sunset: float = None
    percent_possible_sunshine: int = None
    total_sunshine: int = None
    frozen_ground_layer: int = None
    water_equivalent_snow_on_ground: float = None
    fog: bool = False
    heavy_fog: bool = False
    thunder: bool = False
    sleet: bool = False
    hail: bool = False
    glaze: bool = False
    dust: bool = False
    smoke: bool = False
    blowing_snow: bool = False
    high_wind: bool = False
    mist: bool = False
    drizzle: bool = False
    freezing_drizzle: bool = False
    rain: bool = False
    freezing_rain: bool = False
    snow: bool = False
    other_precipitation: bool = False
    ground_fog: bool = False
    ice_fog: bool = False
    avg_wind: float = None
    fog_in_area: bool = False
    thunder_in_area: bool = False
    rain_or_snow_in_area: bool = False
    time_fastest_mile_or_fastest_1_minute_wind: datetime.time = None
    peak_gust_time: datetime.time = None
    direction_fastest_1_minute_wind: int = None
    direction_fastest_2_minute_wind: int = None
    direction_fastest_5_second_wind: int = None
    direction_peak_gust: int = None
    direction_fastest_mile_wind: int = None
    fastest_1_minute_wind: float = None
    fastest_2_minute_wind: float = None
    fastest_5_second_wind: float = None
    peak_gust: float = None
    fastest_mile_wind: float = None
    avg_relative_humidity: int = None
    min_relative_humidity: int = None
    max_relative_humidity: int = None
    avg_sea_level_pressure: float = None
    avg_station_pressure: float = None
    avg_dew_point_temperature: int = None
    avg_wet_bulb_temperature: int = None


class ConversionEnum(Enum):
    DATE = "date"
    STATION = "station"
    TAVG = "avg_temp"
    TMIN = "min_temp"
    TMAX = "max_temp"
    PRCP = "precipitation"
    SNOW = "snowfall"
    SNWD = "snow_depth"
    ACMH = "cloudiness_midnight_to_midnight"
    ACSH = "cloudiness_sunrise_to_sunset"
    PSUN = "percent_possible_sunshine"
    TSUN = "total_sunshine"
    FRGT = "frozen_ground_layer"
    WESD = "water_equivalent_snow_on_ground"
    WT01 = "fog"
    WT02 = "heavy_fog"
    WT03 = "thunder"
    WT04 = "sleet"
    WT05 = "hail"
    WT06 = "glaze"
    WT07 = "dust"
    WT08 = "smoke"
    WT09 = "blowing_snow"
    WT11 = "high_wind"
    WT13 = "mist"
    WT14 = "drizzle"
    WT15 = "freezing_drizzle"
    WT16 = "rain"
    WT17 = "freezing_rain"
    WT18 = "snow"
    WT19 = "other_precipitation"
    WT21 = "ground_fog"
    WT22 = "ice_fog"
    WV01 = "fog_in_area"
    WV03 = "thunder_in_area"
    WV20 = "rain_or_snow_in_area"
    AWND = "avg_wind"
    FMTM = "time_fastest_mile_or_fastest_1_minute_wind"
    PGTM = "peak_gust_time"
    WDF1 = "direction_fastest_1_minute_wind"
    WDF2 = "direction_fastest_2_minute_wind"
    WDF5 = "direction_fastest_5_second_wind"
    WDFG = "direction_peak_gust"
    WDFM = "direction_fastest_mile_wind"
    WSF1 = "fastest_1_minute_wind"
    WSF2 = "fastest_2_minute_wind"
    WSF5 = "fastest_5_second_wind"
    WSFG = "peak_gust"
    WSFM = "fastest_mile_wind"
    RHAV = "avg_relative_humidity"
    RHMN = "min_relative_humidity"
    RHMX = "max_relative_humidity"
    ASLP = "avg_sea_level_pressure"
    ASTP = "avg_station_pressure"
    ADPT = "avg_dew_point_temperature"
    AWBT = "avg_wet_bulb_temperature"


class DailySummaries:
    """
    Wrapper for access to NOAA's daily summary data API via the National Centers for
    Environmental Information (NCEI). For more inforamtion on the API and data, go to
    https://www.ncei.noaa.gov/access.
    """

    def __init__(self) -> None:
        self._base_url = (
            "https://www.ncei.noaa.gov/access/services/data/v1?dataset=daily-summaries"
        )
        self._format_string = "&format=json"

    def get_daily_summaries(
        self,
        station_id: str,
        start_date: str,
        end_date: str,
        units: str = "standard",
        return_raw: bool = False,
    ) -> list[DailySummary] | list[dict[str, str]]:
        """
        Interface to get daily summary data for a weather station, with data category
        codes formated into readable category/column titles, and data transformed from
        string format to an appropriate Python data type. Returns a list of dataclass
        objects, each holding a row, which can easily be converted to dataframe,
        dictionary or other format.

        Parameters:

        station_id: str - ex: "USW00024233" - The station ID to get data for. You
        can find stations at https://www.ncdc.noaa.gov/cdo-web/datatools/findstation.

        start_date: str, datetime.date, or datetime.datetime - String dates must be in
        the form "YYYY-MM-DD".

        end_date: str, datetime.date, or datetime.datetime - String dates must be in
        the form "YYYY-MM-DD".

        units: str - "standard" or "metric". Default value "None" returns data in
        standard format.

        return_raw: bool - If True, returns the raw JSON data from the API call,
        unformatted. Default value is False.
        """

        start_date, end_date = self._validate_dates(start_date, end_date)

        raw_data = self._make_api_call(start_date, end_date, station_id, units)

        if return_raw:
            return raw_data

        data = self._convert_keys(raw_data)

        data = self._coerce_types(data)

        data = self._remap_dictionaries(data)

        return [DailySummary(**i) for i in data]

    def _remap_dictionaries(self, data: CoercedDicts) -> CoercedDicts:
        template_dict = asdict(DailySummary())
        re_mapped_dicts = []
        for i in data:
            new_dict = {}
            for key in template_dict.keys():
                if key in i.keys():
                    new_dict[key] = i[key]
                else:
                    new_dict[key] = template_dict[key]
            re_mapped_dicts.append(new_dict)

        return re_mapped_dicts

    def _validate_dates(
        self,
        start_date: str | datetime.date | datetime.datetime,
        end_date: str | datetime.date | datetime.datetime,
    ) -> tuple[str, str]:
        if isinstance(start_date, datetime.date):
            start_date = start_date.isoformat()
        elif isinstance(start_date, datetime.datetime):
            start_date = start_date.date().isoformat()
        elif isinstance(start_date, str):
            if len(start_date) != 10:
                raise ValueError("Start date must be in the form YYYY-MM-DD.")
        else:
            raise TypeError("Start date must be a string or datetime object.")

        if isinstance(end_date, datetime.date):
            end_date = end_date.isoformat()
        elif isinstance(end_date, datetime.datetime):
            end_date = end_date.date().isoformat()
        elif isinstance(end_date, str):
            if len(end_date) != 10:
                raise ValueError("End date must be in the form YYYY-MM-DD.")
        else:
            raise TypeError("End date must be a string or datetime object.")

        if start_date > end_date:
            raise ValueError("Start date must be before end date.")

        return (start_date, end_date)

    def _construct_request_url(
        self, start_date: str, end_date: str, station_id: str, units: str
    ) -> str:
        """
        Constructs the request URL with the given parameters.
        """
        return (
            f"{self._base_url}&startDate={start_date}&endDate={end_date}&stations="
            f"{station_id}&units={units}{self._format_string}"
        )

    def _convert_keys(self, data: list[dict[str, str]]) -> list[dict[str, str]]:
        converted_results = []
        for i in data:
            converted_results.append({ConversionEnum[k].value: v for k, v in i.items()})

        return converted_results

    def _convert_time_string(self, time_string: str) -> datetime.time:
        return datetime.time(hour=int(time_string[:3]), minute=int(time_string[-2:]))

    def _coerce_types(self, data: list[dict[str, str]]) -> CoercedDicts:
        dtype_dict = {
            k: v
            for k, v in zip(
                [k for k in DailySummary.__annotations__.keys()],
                [v.__name__ for v in DailySummary.__annotations__.values()],
            )
        }

        for i in data:
            for k, v in i.items():
                if dtype_dict[k] == "int":
                    i[k] = int(v)
                elif dtype_dict[k] == "float":
                    i[k] = float(v)
                elif dtype_dict[k] == "bool":
                    i[k] = True if v.strip() == "1" else False
                elif dtype_dict[k] == "date":
                    i[k] = datetime.date.fromisoformat(v)
                elif dtype_dict[k] == "time":
                    i[k] = self._convert_time_string(v)

        return data

    def _make_api_call(
        self,
        start_date: str,
        end_date: str,
        station_id: str,
        units: str,
    ) -> list[dict[str, str]]:
        """
        Makes an API call to NCEI, and returns the response in JSON format.
        """
        if not units:
            units = "standard"

        url = self._construct_request_url(start_date, end_date, station_id, units)
        response = requests.get(url).json()

        if isinstance(response, dict):
            raise ResponseError(f"API returned the following error: {response}")

        return response
