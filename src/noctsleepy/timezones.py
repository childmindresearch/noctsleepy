"""List of common IANA timezones to be used in `nocstsleepy`.

Users can can manually add to this list or open an issue on GitHub
if a commonly used timezone is missing.
"""

from typing import Literal

CommonTimezones = Literal[
    # Africa
    "Africa/Addis_Ababa",
    "Africa/Algiers",
    "Africa/Cairo",
    "Africa/Casablanca",
    "Africa/Johannesburg",
    "Africa/Lagos",
    "Africa/Nairobi",
    # America - Argentina
    "America/Argentina/Buenos_Aires",
    # America - Canada
    "America/Edmonton",
    "America/Halifax",
    "America/St_Johns",
    "America/Toronto",
    "America/Vancouver",
    "America/Winnipeg",
    # America - Mexico
    "America/Mazatlan",
    "America/Mexico_City",
    # America - South America
    "America/Bogota",
    "America/Caracas",
    "America/Lima",
    "America/Manaus",
    "America/Santiago",
    "America/Sao_Paulo",
    # America - United States
    "America/Anchorage",
    "America/Chicago",
    "America/Denver",
    "America/Los_Angeles",
    "America/New_York",
    "America/Phoenix",
    # Asia - Central
    "Asia/Almaty",
    "Asia/Tashkent",
    # Asia - East
    "Asia/Hong_Kong",
    "Asia/Shanghai",
    "Asia/Seoul",
    "Asia/Taipei",
    "Asia/Tokyo",
    # Asia - South
    "Asia/Colombo",
    "Asia/Dhaka",
    "Asia/Karachi",
    "Asia/Kathmandu",
    "Asia/Kolkata",
    # Asia - Southeast
    "Asia/Bangkok",
    "Asia/Ho_Chi_Minh",
    "Asia/Jakarta",
    "Asia/Kuala_Lumpur",
    "Asia/Manila",
    "Asia/Singapore",
    # Asia - West
    "Asia/Beirut",
    "Asia/Dubai",
    "Asia/Jerusalem",
    "Asia/Riyadh",
    "Asia/Tehran",
    # Atlantic
    "Atlantic/Reykjavik",
    # Australia
    "Australia/Adelaide",
    "Australia/Brisbane",
    "Australia/Darwin",
    "Australia/Melbourne",
    "Australia/Perth",
    "Australia/Sydney",
    # Europe - Central
    "Europe/Amsterdam",
    "Europe/Brussels",
    "Europe/Berlin",
    "Europe/Madrid",
    "Europe/Paris",
    "Europe/Rome",
    "Europe/Stockholm",
    "Europe/Vienna",
    "Europe/Warsaw",
    "Europe/Zurich",
    # Europe - Eastern
    "Europe/Athens",
    "Europe/Bucharest",
    "Europe/Helsinki",
    "Europe/Istanbul",
    "Europe/Kiev",
    "Europe/Moscow",
    # Europe - Western
    "Europe/Dublin",
    "Europe/Lisbon",
    "Europe/London",
    # Middle East
    "Asia/Tehran",
    # Oceania
    "Pacific/Auckland",
    "Pacific/Fiji",
    "Pacific/Guam",
    "Pacific/Tahiti",
    "Pacific/Honolulu",
]
