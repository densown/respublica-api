#!/usr/bin/env python3
"""
Phase D2.1 – Country Stammdaten füllen
Importiert ~195 Länder + Aggregate in data_countries

Quelle: ISO 3166-1 + Weltbank Region/Income Klassifikation + DE/EN Übersetzungen
"""
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from lib.db import get_db
from lib.env import load_env

# Format: (iso3, iso2, name_de, name_en, region_de, region_en, income, capital_de, capital_en)
# income: 'high', 'upper_middle', 'lower_middle', 'low', 'aggregate'
COUNTRIES = [
    # Europa
    ("DEU", "DE", "Deutschland", "Germany", "Europa", "Europe", "high", "Berlin", "Berlin"),
    ("FRA", "FR", "Frankreich", "France", "Europa", "Europe", "high", "Paris", "Paris"),
    ("GBR", "GB", "Vereinigtes Königreich", "United Kingdom", "Europa", "Europe", "high", "London", "London"),
    ("ITA", "IT", "Italien", "Italy", "Europa", "Europe", "high", "Rom", "Rome"),
    ("ESP", "ES", "Spanien", "Spain", "Europa", "Europe", "high", "Madrid", "Madrid"),
    ("POL", "PL", "Polen", "Poland", "Europa", "Europe", "high", "Warschau", "Warsaw"),
    ("ROU", "RO", "Rumänien", "Romania", "Europa", "Europe", "high", "Bukarest", "Bucharest"),
    ("NLD", "NL", "Niederlande", "Netherlands", "Europa", "Europe", "high", "Amsterdam", "Amsterdam"),
    ("BEL", "BE", "Belgien", "Belgium", "Europa", "Europe", "high", "Brüssel", "Brussels"),
    ("CZE", "CZ", "Tschechien", "Czech Republic", "Europa", "Europe", "high", "Prag", "Prague"),
    ("GRC", "GR", "Griechenland", "Greece", "Europa", "Europe", "high", "Athen", "Athens"),
    ("PRT", "PT", "Portugal", "Portugal", "Europa", "Europe", "high", "Lissabon", "Lisbon"),
    ("SWE", "SE", "Schweden", "Sweden", "Europa", "Europe", "high", "Stockholm", "Stockholm"),
    ("HUN", "HU", "Ungarn", "Hungary", "Europa", "Europe", "high", "Budapest", "Budapest"),
    ("AUT", "AT", "Österreich", "Austria", "Europa", "Europe", "high", "Wien", "Vienna"),
    ("CHE", "CH", "Schweiz", "Switzerland", "Europa", "Europe", "high", "Bern", "Bern"),
    ("BGR", "BG", "Bulgarien", "Bulgaria", "Europa", "Europe", "upper_middle", "Sofia", "Sofia"),
    ("DNK", "DK", "Dänemark", "Denmark", "Europa", "Europe", "high", "Kopenhagen", "Copenhagen"),
    ("FIN", "FI", "Finnland", "Finland", "Europa", "Europe", "high", "Helsinki", "Helsinki"),
    ("SVK", "SK", "Slowakei", "Slovakia", "Europa", "Europe", "high", "Bratislava", "Bratislava"),
    ("NOR", "NO", "Norwegen", "Norway", "Europa", "Europe", "high", "Oslo", "Oslo"),
    ("IRL", "IE", "Irland", "Ireland", "Europa", "Europe", "high", "Dublin", "Dublin"),
    ("HRV", "HR", "Kroatien", "Croatia", "Europa", "Europe", "high", "Zagreb", "Zagreb"),
    ("BIH", "BA", "Bosnien und Herzegowina", "Bosnia and Herzegovina", "Europa", "Europe", "upper_middle", "Sarajevo", "Sarajevo"),
    ("ALB", "AL", "Albanien", "Albania", "Europa", "Europe", "upper_middle", "Tirana", "Tirana"),
    ("LTU", "LT", "Litauen", "Lithuania", "Europa", "Europe", "high", "Vilnius", "Vilnius"),
    ("SVN", "SI", "Slowenien", "Slovenia", "Europa", "Europe", "high", "Ljubljana", "Ljubljana"),
    ("LVA", "LV", "Lettland", "Latvia", "Europa", "Europe", "high", "Riga", "Riga"),
    ("EST", "EE", "Estland", "Estonia", "Europa", "Europe", "high", "Tallinn", "Tallinn"),
    ("MKD", "MK", "Nordmazedonien", "North Macedonia", "Europa", "Europe", "upper_middle", "Skopje", "Skopje"),
    ("MDA", "MD", "Moldau", "Moldova", "Europa", "Europe", "upper_middle", "Chișinău", "Chișinău"),
    ("MLT", "MT", "Malta", "Malta", "Europa", "Europe", "high", "Valletta", "Valletta"),
    ("ISL", "IS", "Island", "Iceland", "Europa", "Europe", "high", "Reykjavík", "Reykjavík"),
    ("MNE", "ME", "Montenegro", "Montenegro", "Europa", "Europe", "upper_middle", "Podgorica", "Podgorica"),
    ("LUX", "LU", "Luxemburg", "Luxembourg", "Europa", "Europe", "high", "Luxemburg", "Luxembourg"),
    ("SRB", "RS", "Serbien", "Serbia", "Europa", "Europe", "upper_middle", "Belgrad", "Belgrade"),
    ("CYP", "CY", "Zypern", "Cyprus", "Europa", "Europe", "high", "Nikosia", "Nicosia"),
    ("UKR", "UA", "Ukraine", "Ukraine", "Europa", "Europe", "upper_middle", "Kiew", "Kyiv"),
    ("BLR", "BY", "Belarus", "Belarus", "Europa", "Europe", "upper_middle", "Minsk", "Minsk"),
    ("RUS", "RU", "Russland", "Russia", "Europa", "Europe", "high", "Moskau", "Moscow"),
    ("AND", "AD", "Andorra", "Andorra", "Europa", "Europe", "high", "Andorra la Vella", "Andorra la Vella"),
    ("MCO", "MC", "Monaco", "Monaco", "Europa", "Europe", "high", "Monaco", "Monaco"),
    ("LIE", "LI", "Liechtenstein", "Liechtenstein", "Europa", "Europe", "high", "Vaduz", "Vaduz"),
    ("SMR", "SM", "San Marino", "San Marino", "Europa", "Europe", "high", "San Marino", "San Marino"),
    ("VAT", "VA", "Vatikanstadt", "Vatican City", "Europa", "Europe", "high", "Vatikanstadt", "Vatican City"),
    ("XKX", "XK", "Kosovo", "Kosovo", "Europa", "Europe", "upper_middle", "Pristina", "Pristina"),

    # Nordamerika
    ("USA", "US", "Vereinigte Staaten", "United States", "Nordamerika", "North America", "high", "Washington, D.C.", "Washington, D.C."),
    ("CAN", "CA", "Kanada", "Canada", "Nordamerika", "North America", "high", "Ottawa", "Ottawa"),
    ("MEX", "MX", "Mexiko", "Mexico", "Nordamerika", "North America", "upper_middle", "Mexiko-Stadt", "Mexico City"),

    # Lateinamerika & Karibik
    ("BRA", "BR", "Brasilien", "Brazil", "Lateinamerika", "Latin America", "upper_middle", "Brasília", "Brasília"),
    ("ARG", "AR", "Argentinien", "Argentina", "Lateinamerika", "Latin America", "upper_middle", "Buenos Aires", "Buenos Aires"),
    ("COL", "CO", "Kolumbien", "Colombia", "Lateinamerika", "Latin America", "upper_middle", "Bogotá", "Bogotá"),
    ("CHL", "CL", "Chile", "Chile", "Lateinamerika", "Latin America", "high", "Santiago", "Santiago"),
    ("PER", "PE", "Peru", "Peru", "Lateinamerika", "Latin America", "upper_middle", "Lima", "Lima"),
    ("VEN", "VE", "Venezuela", "Venezuela", "Lateinamerika", "Latin America", "upper_middle", "Caracas", "Caracas"),
    ("ECU", "EC", "Ecuador", "Ecuador", "Lateinamerika", "Latin America", "upper_middle", "Quito", "Quito"),
    ("BOL", "BO", "Bolivien", "Bolivia", "Lateinamerika", "Latin America", "lower_middle", "Sucre", "Sucre"),
    ("PRY", "PY", "Paraguay", "Paraguay", "Lateinamerika", "Latin America", "upper_middle", "Asunción", "Asunción"),
    ("URY", "UY", "Uruguay", "Uruguay", "Lateinamerika", "Latin America", "high", "Montevideo", "Montevideo"),
    ("GUY", "GY", "Guyana", "Guyana", "Lateinamerika", "Latin America", "high", "Georgetown", "Georgetown"),
    ("SUR", "SR", "Suriname", "Suriname", "Lateinamerika", "Latin America", "upper_middle", "Paramaribo", "Paramaribo"),
    ("CUB", "CU", "Kuba", "Cuba", "Lateinamerika", "Latin America", "upper_middle", "Havanna", "Havana"),
    ("DOM", "DO", "Dominikanische Republik", "Dominican Republic", "Lateinamerika", "Latin America", "upper_middle", "Santo Domingo", "Santo Domingo"),
    ("HTI", "HT", "Haiti", "Haiti", "Lateinamerika", "Latin America", "low", "Port-au-Prince", "Port-au-Prince"),
    ("JAM", "JM", "Jamaika", "Jamaica", "Lateinamerika", "Latin America", "upper_middle", "Kingston", "Kingston"),
    ("TTO", "TT", "Trinidad und Tobago", "Trinidad and Tobago", "Lateinamerika", "Latin America", "high", "Port of Spain", "Port of Spain"),
    ("BHS", "BS", "Bahamas", "Bahamas", "Lateinamerika", "Latin America", "high", "Nassau", "Nassau"),
    ("BRB", "BB", "Barbados", "Barbados", "Lateinamerika", "Latin America", "high", "Bridgetown", "Bridgetown"),
    ("BLZ", "BZ", "Belize", "Belize", "Lateinamerika", "Latin America", "upper_middle", "Belmopan", "Belmopan"),
    ("CRI", "CR", "Costa Rica", "Costa Rica", "Lateinamerika", "Latin America", "high", "San José", "San José"),
    ("SLV", "SV", "El Salvador", "El Salvador", "Lateinamerika", "Latin America", "upper_middle", "San Salvador", "San Salvador"),
    ("GTM", "GT", "Guatemala", "Guatemala", "Lateinamerika", "Latin America", "upper_middle", "Guatemala-Stadt", "Guatemala City"),
    ("HND", "HN", "Honduras", "Honduras", "Lateinamerika", "Latin America", "lower_middle", "Tegucigalpa", "Tegucigalpa"),
    ("NIC", "NI", "Nicaragua", "Nicaragua", "Lateinamerika", "Latin America", "lower_middle", "Managua", "Managua"),
    ("PAN", "PA", "Panama", "Panama", "Lateinamerika", "Latin America", "high", "Panama-Stadt", "Panama City"),
    ("ATG", "AG", "Antigua und Barbuda", "Antigua and Barbuda", "Lateinamerika", "Latin America", "high", "Saint John's", "Saint John's"),
    ("DMA", "DM", "Dominica", "Dominica", "Lateinamerika", "Latin America", "upper_middle", "Roseau", "Roseau"),
    ("GRD", "GD", "Grenada", "Grenada", "Lateinamerika", "Latin America", "upper_middle", "Saint George's", "Saint George's"),
    ("KNA", "KN", "St. Kitts und Nevis", "Saint Kitts and Nevis", "Lateinamerika", "Latin America", "high", "Basseterre", "Basseterre"),
    ("LCA", "LC", "St. Lucia", "Saint Lucia", "Lateinamerika", "Latin America", "upper_middle", "Castries", "Castries"),
    ("VCT", "VC", "St. Vincent und die Grenadinen", "Saint Vincent and the Grenadines", "Lateinamerika", "Latin America", "upper_middle", "Kingstown", "Kingstown"),

    # Asien
    ("CHN", "CN", "China", "China", "Asien", "Asia", "upper_middle", "Peking", "Beijing"),
    ("JPN", "JP", "Japan", "Japan", "Asien", "Asia", "high", "Tokio", "Tokyo"),
    ("IND", "IN", "Indien", "India", "Asien", "Asia", "lower_middle", "Neu-Delhi", "New Delhi"),
    ("KOR", "KR", "Südkorea", "South Korea", "Asien", "Asia", "high", "Seoul", "Seoul"),
    ("PRK", "KP", "Nordkorea", "North Korea", "Asien", "Asia", "low", "Pjöngjang", "Pyongyang"),
    ("IDN", "ID", "Indonesien", "Indonesia", "Asien", "Asia", "upper_middle", "Jakarta", "Jakarta"),
    ("PAK", "PK", "Pakistan", "Pakistan", "Asien", "Asia", "lower_middle", "Islamabad", "Islamabad"),
    ("BGD", "BD", "Bangladesch", "Bangladesh", "Asien", "Asia", "lower_middle", "Dhaka", "Dhaka"),
    ("VNM", "VN", "Vietnam", "Vietnam", "Asien", "Asia", "lower_middle", "Hanoi", "Hanoi"),
    ("THA", "TH", "Thailand", "Thailand", "Asien", "Asia", "upper_middle", "Bangkok", "Bangkok"),
    ("PHL", "PH", "Philippinen", "Philippines", "Asien", "Asia", "lower_middle", "Manila", "Manila"),
    ("MYS", "MY", "Malaysia", "Malaysia", "Asien", "Asia", "upper_middle", "Kuala Lumpur", "Kuala Lumpur"),
    ("SGP", "SG", "Singapur", "Singapore", "Asien", "Asia", "high", "Singapur", "Singapore"),
    ("MMR", "MM", "Myanmar", "Myanmar", "Asien", "Asia", "lower_middle", "Naypyidaw", "Naypyidaw"),
    ("KHM", "KH", "Kambodscha", "Cambodia", "Asien", "Asia", "lower_middle", "Phnom Penh", "Phnom Penh"),
    ("LAO", "LA", "Laos", "Laos", "Asien", "Asia", "lower_middle", "Vientiane", "Vientiane"),
    ("LKA", "LK", "Sri Lanka", "Sri Lanka", "Asien", "Asia", "lower_middle", "Sri Jayawardenepura", "Sri Jayawardenepura"),
    ("NPL", "NP", "Nepal", "Nepal", "Asien", "Asia", "lower_middle", "Kathmandu", "Kathmandu"),
    ("BTN", "BT", "Bhutan", "Bhutan", "Asien", "Asia", "lower_middle", "Thimphu", "Thimphu"),
    ("MNG", "MN", "Mongolei", "Mongolia", "Asien", "Asia", "upper_middle", "Ulaanbaatar", "Ulaanbaatar"),
    ("AFG", "AF", "Afghanistan", "Afghanistan", "Asien", "Asia", "low", "Kabul", "Kabul"),
    ("KAZ", "KZ", "Kasachstan", "Kazakhstan", "Asien", "Asia", "upper_middle", "Astana", "Astana"),
    ("UZB", "UZ", "Usbekistan", "Uzbekistan", "Asien", "Asia", "lower_middle", "Taschkent", "Tashkent"),
    ("TKM", "TM", "Turkmenistan", "Turkmenistan", "Asien", "Asia", "upper_middle", "Aşgabat", "Ashgabat"),
    ("KGZ", "KG", "Kirgisistan", "Kyrgyzstan", "Asien", "Asia", "lower_middle", "Bischkek", "Bishkek"),
    ("TJK", "TJ", "Tadschikistan", "Tajikistan", "Asien", "Asia", "lower_middle", "Duschanbe", "Dushanbe"),
    ("MDV", "MV", "Malediven", "Maldives", "Asien", "Asia", "upper_middle", "Malé", "Malé"),
    ("BRN", "BN", "Brunei", "Brunei", "Asien", "Asia", "high", "Bandar Seri Begawan", "Bandar Seri Begawan"),
    ("TLS", "TL", "Osttimor", "Timor-Leste", "Asien", "Asia", "lower_middle", "Dili", "Dili"),
    ("TWN", "TW", "Taiwan", "Taiwan", "Asien", "Asia", "high", "Taipeh", "Taipei"),
    ("HKG", "HK", "Hongkong", "Hong Kong", "Asien", "Asia", "high", "Hongkong", "Hong Kong"),
    ("MAC", "MO", "Macao", "Macao", "Asien", "Asia", "high", "Macao", "Macao"),

    # Naher Osten
    ("SAU", "SA", "Saudi-Arabien", "Saudi Arabia", "Naher Osten", "Middle East", "high", "Riad", "Riyadh"),
    ("ARE", "AE", "Vereinigte Arabische Emirate", "United Arab Emirates", "Naher Osten", "Middle East", "high", "Abu Dhabi", "Abu Dhabi"),
    ("ISR", "IL", "Israel", "Israel", "Naher Osten", "Middle East", "high", "Jerusalem", "Jerusalem"),
    ("TUR", "TR", "Türkei", "Türkiye", "Naher Osten", "Middle East", "upper_middle", "Ankara", "Ankara"),
    ("IRN", "IR", "Iran", "Iran", "Naher Osten", "Middle East", "lower_middle", "Teheran", "Tehran"),
    ("IRQ", "IQ", "Irak", "Iraq", "Naher Osten", "Middle East", "upper_middle", "Bagdad", "Baghdad"),
    ("SYR", "SY", "Syrien", "Syria", "Naher Osten", "Middle East", "low", "Damaskus", "Damascus"),
    ("LBN", "LB", "Libanon", "Lebanon", "Naher Osten", "Middle East", "lower_middle", "Beirut", "Beirut"),
    ("JOR", "JO", "Jordanien", "Jordan", "Naher Osten", "Middle East", "upper_middle", "Amman", "Amman"),
    ("KWT", "KW", "Kuwait", "Kuwait", "Naher Osten", "Middle East", "high", "Kuwait-Stadt", "Kuwait City"),
    ("QAT", "QA", "Katar", "Qatar", "Naher Osten", "Middle East", "high", "Doha", "Doha"),
    ("BHR", "BH", "Bahrain", "Bahrain", "Naher Osten", "Middle East", "high", "Manama", "Manama"),
    ("OMN", "OM", "Oman", "Oman", "Naher Osten", "Middle East", "high", "Maskat", "Muscat"),
    ("YEM", "YE", "Jemen", "Yemen", "Naher Osten", "Middle East", "low", "Sanaa", "Sanaa"),
    ("PSE", "PS", "Palästina", "Palestine", "Naher Osten", "Middle East", "lower_middle", "Ostjerusalem", "East Jerusalem"),
    ("ARM", "AM", "Armenien", "Armenia", "Naher Osten", "Middle East", "upper_middle", "Eriwan", "Yerevan"),
    ("AZE", "AZ", "Aserbaidschan", "Azerbaijan", "Naher Osten", "Middle East", "upper_middle", "Baku", "Baku"),
    ("GEO", "GE", "Georgien", "Georgia", "Naher Osten", "Middle East", "upper_middle", "Tiflis", "Tbilisi"),

    # Afrika
    ("ZAF", "ZA", "Südafrika", "South Africa", "Afrika", "Africa", "upper_middle", "Pretoria", "Pretoria"),
    ("EGY", "EG", "Ägypten", "Egypt", "Afrika", "Africa", "lower_middle", "Kairo", "Cairo"),
    ("NGA", "NG", "Nigeria", "Nigeria", "Afrika", "Africa", "lower_middle", "Abuja", "Abuja"),
    ("ETH", "ET", "Äthiopien", "Ethiopia", "Afrika", "Africa", "low", "Addis Abeba", "Addis Ababa"),
    ("KEN", "KE", "Kenia", "Kenya", "Afrika", "Africa", "lower_middle", "Nairobi", "Nairobi"),
    ("DZA", "DZ", "Algerien", "Algeria", "Afrika", "Africa", "lower_middle", "Algier", "Algiers"),
    ("MAR", "MA", "Marokko", "Morocco", "Afrika", "Africa", "lower_middle", "Rabat", "Rabat"),
    ("TUN", "TN", "Tunesien", "Tunisia", "Afrika", "Africa", "lower_middle", "Tunis", "Tunis"),
    ("LBY", "LY", "Libyen", "Libya", "Afrika", "Africa", "upper_middle", "Tripolis", "Tripoli"),
    ("SDN", "SD", "Sudan", "Sudan", "Afrika", "Africa", "low", "Khartum", "Khartoum"),
    ("SSD", "SS", "Südsudan", "South Sudan", "Afrika", "Africa", "low", "Juba", "Juba"),
    ("GHA", "GH", "Ghana", "Ghana", "Afrika", "Africa", "lower_middle", "Accra", "Accra"),
    ("CIV", "CI", "Elfenbeinküste", "Côte d'Ivoire", "Afrika", "Africa", "lower_middle", "Yamoussoukro", "Yamoussoukro"),
    ("SEN", "SN", "Senegal", "Senegal", "Afrika", "Africa", "lower_middle", "Dakar", "Dakar"),
    ("CMR", "CM", "Kamerun", "Cameroon", "Afrika", "Africa", "lower_middle", "Yaoundé", "Yaoundé"),
    ("AGO", "AO", "Angola", "Angola", "Afrika", "Africa", "lower_middle", "Luanda", "Luanda"),
    ("MOZ", "MZ", "Mosambik", "Mozambique", "Afrika", "Africa", "low", "Maputo", "Maputo"),
    ("UGA", "UG", "Uganda", "Uganda", "Afrika", "Africa", "low", "Kampala", "Kampala"),
    ("TZA", "TZ", "Tansania", "Tanzania", "Afrika", "Africa", "lower_middle", "Dodoma", "Dodoma"),
    ("MDG", "MG", "Madagaskar", "Madagascar", "Afrika", "Africa", "low", "Antananarivo", "Antananarivo"),
    ("MLI", "ML", "Mali", "Mali", "Afrika", "Africa", "low", "Bamako", "Bamako"),
    ("BFA", "BF", "Burkina Faso", "Burkina Faso", "Afrika", "Africa", "low", "Ouagadougou", "Ouagadougou"),
    ("NER", "NE", "Niger", "Niger", "Afrika", "Africa", "low", "Niamey", "Niamey"),
    ("TCD", "TD", "Tschad", "Chad", "Afrika", "Africa", "low", "N'Djamena", "N'Djamena"),
    ("ZWE", "ZW", "Simbabwe", "Zimbabwe", "Afrika", "Africa", "lower_middle", "Harare", "Harare"),
    ("ZMB", "ZM", "Sambia", "Zambia", "Afrika", "Africa", "lower_middle", "Lusaka", "Lusaka"),
    ("MWI", "MW", "Malawi", "Malawi", "Afrika", "Africa", "low", "Lilongwe", "Lilongwe"),
    ("RWA", "RW", "Ruanda", "Rwanda", "Afrika", "Africa", "low", "Kigali", "Kigali"),
    ("BDI", "BI", "Burundi", "Burundi", "Afrika", "Africa", "low", "Gitega", "Gitega"),
    ("BEN", "BJ", "Benin", "Benin", "Afrika", "Africa", "lower_middle", "Porto-Novo", "Porto-Novo"),
    ("TGO", "TG", "Togo", "Togo", "Afrika", "Africa", "low", "Lomé", "Lomé"),
    ("LBR", "LR", "Liberia", "Liberia", "Afrika", "Africa", "low", "Monrovia", "Monrovia"),
    ("SLE", "SL", "Sierra Leone", "Sierra Leone", "Afrika", "Africa", "low", "Freetown", "Freetown"),
    ("GIN", "GN", "Guinea", "Guinea", "Afrika", "Africa", "lower_middle", "Conakry", "Conakry"),
    ("GNB", "GW", "Guinea-Bissau", "Guinea-Bissau", "Afrika", "Africa", "low", "Bissau", "Bissau"),
    ("MRT", "MR", "Mauretanien", "Mauritania", "Afrika", "Africa", "lower_middle", "Nouakchott", "Nouakchott"),
    ("GMB", "GM", "Gambia", "Gambia", "Afrika", "Africa", "low", "Banjul", "Banjul"),
    ("CPV", "CV", "Kap Verde", "Cape Verde", "Afrika", "Africa", "lower_middle", "Praia", "Praia"),
    ("CAF", "CF", "Zentralafrikanische Republik", "Central African Republic", "Afrika", "Africa", "low", "Bangui", "Bangui"),
    ("COG", "CG", "Republik Kongo", "Republic of the Congo", "Afrika", "Africa", "lower_middle", "Brazzaville", "Brazzaville"),
    ("COD", "CD", "Demokratische Republik Kongo", "Democratic Republic of the Congo", "Afrika", "Africa", "low", "Kinshasa", "Kinshasa"),
    ("GAB", "GA", "Gabun", "Gabon", "Afrika", "Africa", "upper_middle", "Libreville", "Libreville"),
    ("GNQ", "GQ", "Äquatorialguinea", "Equatorial Guinea", "Afrika", "Africa", "upper_middle", "Malabo", "Malabo"),
    ("STP", "ST", "São Tomé und Príncipe", "São Tomé and Príncipe", "Afrika", "Africa", "lower_middle", "São Tomé", "São Tomé"),
    ("ERI", "ER", "Eritrea", "Eritrea", "Afrika", "Africa", "low", "Asmara", "Asmara"),
    ("DJI", "DJ", "Dschibuti", "Djibouti", "Afrika", "Africa", "lower_middle", "Dschibuti", "Djibouti"),
    ("SOM", "SO", "Somalia", "Somalia", "Afrika", "Africa", "low", "Mogadischu", "Mogadishu"),
    ("BWA", "BW", "Botswana", "Botswana", "Afrika", "Africa", "upper_middle", "Gaborone", "Gaborone"),
    ("NAM", "NA", "Namibia", "Namibia", "Afrika", "Africa", "upper_middle", "Windhoek", "Windhoek"),
    ("LSO", "LS", "Lesotho", "Lesotho", "Afrika", "Africa", "lower_middle", "Maseru", "Maseru"),
    ("SWZ", "SZ", "Eswatini", "Eswatini", "Afrika", "Africa", "lower_middle", "Mbabane", "Mbabane"),
    ("MUS", "MU", "Mauritius", "Mauritius", "Afrika", "Africa", "high", "Port Louis", "Port Louis"),
    ("SYC", "SC", "Seychellen", "Seychelles", "Afrika", "Africa", "high", "Victoria", "Victoria"),
    ("COM", "KM", "Komoren", "Comoros", "Afrika", "Africa", "lower_middle", "Moroni", "Moroni"),

    # Ozeanien
    ("AUS", "AU", "Australien", "Australia", "Ozeanien", "Oceania", "high", "Canberra", "Canberra"),
    ("NZL", "NZ", "Neuseeland", "New Zealand", "Ozeanien", "Oceania", "high", "Wellington", "Wellington"),
    ("PNG", "PG", "Papua-Neuguinea", "Papua New Guinea", "Ozeanien", "Oceania", "lower_middle", "Port Moresby", "Port Moresby"),
    ("FJI", "FJ", "Fidschi", "Fiji", "Ozeanien", "Oceania", "upper_middle", "Suva", "Suva"),
    ("SLB", "SB", "Salomonen", "Solomon Islands", "Ozeanien", "Oceania", "lower_middle", "Honiara", "Honiara"),
    ("VUT", "VU", "Vanuatu", "Vanuatu", "Ozeanien", "Oceania", "lower_middle", "Port Vila", "Port Vila"),
    ("WSM", "WS", "Samoa", "Samoa", "Ozeanien", "Oceania", "lower_middle", "Apia", "Apia"),
    ("KIR", "KI", "Kiribati", "Kiribati", "Ozeanien", "Oceania", "lower_middle", "South Tarawa", "South Tarawa"),
    ("FSM", "FM", "Mikronesien", "Micronesia", "Ozeanien", "Oceania", "lower_middle", "Palikir", "Palikir"),
    ("MHL", "MH", "Marshallinseln", "Marshall Islands", "Ozeanien", "Oceania", "upper_middle", "Majuro", "Majuro"),
    ("PLW", "PW", "Palau", "Palau", "Ozeanien", "Oceania", "high", "Ngerulmud", "Ngerulmud"),
    ("TON", "TO", "Tonga", "Tonga", "Ozeanien", "Oceania", "upper_middle", "Nukuʻalofa", "Nukuʻalofa"),
    ("TUV", "TV", "Tuvalu", "Tuvalu", "Ozeanien", "Oceania", "upper_middle", "Funafuti", "Funafuti"),
    ("NRU", "NR", "Nauru", "Nauru", "Ozeanien", "Oceania", "high", "Yaren", "Yaren"),

    # Aggregate (Weltbank-Klassifikationen) - is_aggregate=True
    ("WLD", None, "Welt", "World", None, None, "aggregate", None, None),
    ("EUU", None, "Europäische Union", "European Union", "Europa", "Europe", "aggregate", "Brüssel", "Brussels"),
    ("OED", None, "OECD-Mitglieder", "OECD Members", None, None, "aggregate", None, None),
]


def main():
    load_env()
    conn = get_db(autocommit=False)
    cur = conn.cursor()

    print(f"Importiere {len(COUNTRIES)} Länder...")
    inserted, updated = 0, 0

    for row in COUNTRIES:
        iso3, iso2, name_de, name_en, region_de, region_en, income, capital_de, capital_en = row
        is_aggregate = (income == "aggregate")
        is_un_member = not is_aggregate and iso3 not in ("TWN", "HKG", "MAC", "PSE", "VAT", "XKX")

        cur.execute("""
            INSERT INTO data_countries
              (iso3, iso2, name_de, name_en, region_de, region_en,
               income_level, is_aggregate, is_un_member, capital_de, capital_en)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON DUPLICATE KEY UPDATE
              iso2=VALUES(iso2),
              name_de=VALUES(name_de),
              name_en=VALUES(name_en),
              region_de=VALUES(region_de),
              region_en=VALUES(region_en),
              income_level=VALUES(income_level),
              is_aggregate=VALUES(is_aggregate),
              is_un_member=VALUES(is_un_member),
              capital_de=VALUES(capital_de),
              capital_en=VALUES(capital_en)
        """, (iso3, iso2, name_de, name_en, region_de, region_en, income,
              is_aggregate, is_un_member, capital_de, capital_en))

        if cur.rowcount == 1:
            inserted += 1
        else:
            updated += 1

    conn.commit()
    cur.close()
    conn.close()

    print(f"Fertig. Eingefügt: {inserted}, Aktualisiert: {updated}")


if __name__ == "__main__":
    main()
