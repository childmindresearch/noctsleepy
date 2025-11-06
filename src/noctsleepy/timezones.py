"""Map and Enum class to convert from user-defined location to IANA timezone."""

from enum import Enum

TIMEZONE_MAP = {
    # North America - United States
    "us_eastern": "America/New_York",
    "us_central": "America/Chicago",
    "us_mountain": "America/Denver",
    "us_pacific": "America/Los_Angeles",
    "us_alaska": "America/Anchorage",
    "us_hawaii": "Pacific/Honolulu",
    "us_arizona": "America/Phoenix",
    # North America - Canada
    "canada_atlantic": "America/Halifax",
    "canada_eastern": "America/Toronto",
    "canada_central": "America/Winnipeg",
    "canada_mountain": "America/Edmonton",
    "canada_pacific": "America/Vancouver",
    "canada_newfoundland": "America/St_Johns",
    # North America - Mexico
    "mexico_central": "America/Mexico_City",
    "mexico_pacific": "America/Mazatlan",
    # South America
    "brazil_eastern": "America/Sao_Paulo",
    "brazil_western": "America/Manaus",
    "argentina": "America/Argentina/Buenos_Aires",
    "chile": "America/Santiago",
    "colombia": "America/Bogota",
    "peru": "America/Lima",
    "venezuela": "America/Caracas",
    # Europe - Western
    "europe_london": "Europe/London",
    "europe_dublin": "Europe/Dublin",
    "europe_lisbon": "Europe/Lisbon",
    "europe_reykjavik": "Atlantic/Reykjavik",
    # Europe - Central
    "europe_paris": "Europe/Paris",
    "europe_berlin": "Europe/Berlin",
    "europe_rome": "Europe/Rome",
    "europe_madrid": "Europe/Madrid",
    "europe_amsterdam": "Europe/Amsterdam",
    "europe_brussels": "Europe/Brussels",
    "europe_vienna": "Europe/Vienna",
    "europe_zurich": "Europe/Zurich",
    "europe_stockholm": "Europe/Stockholm",
    "europe_oslo": "Europe/Oslo",
    "europe_copenhagen": "Europe/Copenhagen",
    "europe_warsaw": "Europe/Warsaw",
    "europe_prague": "Europe/Prague",
    # Europe - Eastern
    "europe_athens": "Europe/Athens",
    "europe_bucharest": "Europe/Bucharest",
    "europe_helsinki": "Europe/Helsinki",
    "europe_istanbul": "Europe/Istanbul",
    "europe_kyiv": "Europe/Kiev",
    "europe_moscow": "Europe/Moscow",
    # Middle East
    "middle_east_dubai": "Asia/Dubai",
    "middle_east_riyadh": "Asia/Riyadh",
    "middle_east_jerusalem": "Asia/Jerusalem",
    "middle_east_beirut": "Asia/Beirut",
    "middle_east_tehran": "Asia/Tehran",
    # Africa
    "africa_cairo": "Africa/Cairo",
    "africa_johannesburg": "Africa/Johannesburg",
    "africa_nairobi": "Africa/Nairobi",
    "africa_lagos": "Africa/Lagos",
    "africa_casablanca": "Africa/Casablanca",
    "africa_algiers": "Africa/Algiers",
    "africa_addis_ababa": "Africa/Addis_Ababa",
    # Asia - East
    "asia_tokyo": "Asia/Tokyo",
    "asia_seoul": "Asia/Seoul",
    "asia_beijing": "Asia/Shanghai",
    "asia_shanghai": "Asia/Shanghai",
    "asia_hong_kong": "Asia/Hong_Kong",
    "asia_taipei": "Asia/Taipei",
    # Asia - Southeast
    "asia_singapore": "Asia/Singapore",
    "asia_bangkok": "Asia/Bangkok",
    "asia_jakarta": "Asia/Jakarta",
    "asia_manila": "Asia/Manila",
    "asia_kuala_lumpur": "Asia/Kuala_Lumpur",
    "asia_hanoi": "Asia/Ho_Chi_Minh",
    # Asia - South
    "asia_kolkata": "Asia/Kolkata",
    "asia_dhaka": "Asia/Dhaka",
    "asia_karachi": "Asia/Karachi",
    "asia_kathmandu": "Asia/Kathmandu",
    "asia_colombo": "Asia/Colombo",
    # Asia - Central
    "asia_almaty": "Asia/Almaty",
    "asia_tashkent": "Asia/Tashkent",
    # Oceania
    "australia_sydney": "Australia/Sydney",
    "australia_melbourne": "Australia/Melbourne",
    "australia_brisbane": "Australia/Brisbane",
    "australia_perth": "Australia/Perth",
    "australia_adelaide": "Australia/Adelaide",
    "australia_darwin": "Australia/Darwin",
    "new_zealand": "Pacific/Auckland",
    "pacific_fiji": "Pacific/Fiji",
    "pacific_tahiti": "Pacific/Tahiti",
    "pacific_guam": "Pacific/Guam",
}


class CommonTimezones(str, Enum):
    """Common timezone choices."""

    # North America - United States
    us_eastern = "us_eastern"
    us_central = "us_central"
    us_mountain = "us_mountain"
    us_pacific = "us_pacific"
    us_alaska = "us_alaska"
    us_hawaii = "us_hawaii"
    us_arizona = "us_arizona"

    # North America - Canada
    canada_atlantic = "canada_atlantic"
    canada_eastern = "canada_eastern"
    canada_central = "canada_central"
    canada_mountain = "canada_mountain"
    canada_pacific = "canada_pacific"
    canada_newfoundland = "canada_newfoundland"

    # North America - Mexico
    mexico_central = "mexico_central"
    mexico_pacific = "mexico_pacific"

    # South America
    brazil_eastern = "brazil_eastern"
    brazil_western = "brazil_western"
    argentina = "argentina"
    chile = "chile"
    colombia = "colombia"
    peru = "peru"
    venezuela = "venezuela"

    # Europe - Western
    europe_london = "europe_london"
    europe_dublin = "europe_dublin"
    europe_lisbon = "europe_lisbon"
    europe_reykjavik = "europe_reykjavik"

    # Europe - Central
    europe_paris = "europe_paris"
    europe_berlin = "europe_berlin"
    europe_rome = "europe_rome"
    europe_madrid = "europe_madrid"
    europe_amsterdam = "europe_amsterdam"
    europe_brussels = "europe_brussels"
    europe_vienna = "europe_vienna"
    europe_zurich = "europe_zurich"
    europe_stockholm = "europe_stockholm"
    europe_oslo = "europe_oslo"
    europe_copenhagen = "europe_copenhagen"
    europe_warsaw = "europe_warsaw"
    europe_prague = "europe_prague"

    # Europe - Eastern
    europe_athens = "europe_athens"
    europe_bucharest = "europe_bucharest"
    europe_helsinki = "europe_helsinki"
    europe_istanbul = "europe_istanbul"
    europe_kyiv = "europe_kyiv"
    europe_moscow = "europe_moscow"

    # Middle East
    middle_east_dubai = "middle_east_dubai"
    middle_east_riyadh = "middle_east_riyadh"
    middle_east_jerusalem = "middle_east_jerusalem"
    middle_east_beirut = "middle_east_beirut"
    middle_east_tehran = "middle_east_tehran"

    # Africa
    africa_cairo = "africa_cairo"
    africa_johannesburg = "africa_johannesburg"
    africa_nairobi = "africa_nairobi"
    africa_lagos = "africa_lagos"
    africa_casablanca = "africa_casablanca"
    africa_algiers = "africa_algiers"
    africa_addis_ababa = "africa_addis_ababa"

    # Asia - East
    asia_tokyo = "asia_tokyo"
    asia_seoul = "asia_seoul"
    asia_beijing = "asia_beijing"
    asia_shanghai = "asia_shanghai"
    asia_hong_kong = "asia_hong_kong"
    asia_taipei = "asia_taipei"

    # Asia - Southeast
    asia_singapore = "asia_singapore"
    asia_bangkok = "asia_bangkok"
    asia_jakarta = "asia_jakarta"
    asia_manila = "asia_manila"
    asia_kuala_lumpur = "asia_kuala_lumpur"
    asia_hanoi = "asia_hanoi"

    # Asia - South
    asia_kolkata = "asia_kolkata"
    asia_dhaka = "asia_dhaka"
    asia_karachi = "asia_karachi"
    asia_kathmandu = "asia_kathmandu"
    asia_colombo = "asia_colombo"

    # Asia - Central
    asia_almaty = "asia_almaty"
    asia_tashkent = "asia_tashkent"

    # Oceania
    australia_sydney = "australia_sydney"
    australia_melbourne = "australia_melbourne"
    australia_brisbane = "australia_brisbane"
    australia_perth = "australia_perth"
    australia_adelaide = "australia_adelaide"
    australia_darwin = "australia_darwin"
    new_zealand = "new_zealand"
    pacific_fiji = "pacific_fiji"
    pacific_tahiti = "pacific_tahiti"
    pacific_guam = "pacific_guam"
