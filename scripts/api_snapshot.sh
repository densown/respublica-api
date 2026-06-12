#!/bin/bash
# API-Snapshot für Refactoring-Verifikation
# Curlt alle API-Routen mit repräsentativen Parametern und legt die
# Responses (Body + HTTP-Code) als Dateien ab. Zwei Snapshots vor/nach
# einem Refactoring lassen sich per `diff -r` vergleichen.
#
# Usage: ./scripts/api_snapshot.sh <output-dir>
set -e
OUT="${1:-/tmp/api-snapshot}"
BASE="http://localhost:3002"
mkdir -p "$OUT"

snap() {
  local name="$1"; local path="$2"
  curl -s -w "\n%{http_code}" "$BASE$path" > "$OUT/$name.txt"
  echo "✓ $name"
}

# Repräsentative IDs (Stand 2026-06-12, per mysql aus respublica_gesetze):
AID=1               # aenderungen.id
AWID=68376          # abgeordnete.aw_id
POLL=6551           # abstimmungen.poll_id
URTEIL=1            # urteile.id
EURECHT=1           # eu_rechtsakte.id
EUURTEIL=1          # eu_urteile.id (quality_ok=1)
RN=R000001          # lobbyregister.register_number
LOBBYGESETZ=15      # lobby_gesetze.gesetz_id
AGS=01001           # wahlen.ags (federal)
AGS2=16077          # zweites AGS für compare
IND=AG.LND.FRST.ZS  # data_indicators.code
IND2=VC.IHR.PSRC.P5 # zweiter Indikator für scatter

# --- Gesetze ---
snap "gesetze_list" "/api/gesetze?limit=5"
snap "gesetze_stats" "/api/gesetze/stats"
snap "gesetze_detail" "/api/gesetze/$AID"
snap "gesetze_invalid" "/api/gesetze/999999999"
snap "gesetze_badid" "/api/gesetze/abc"

# --- Abstimmungen / Bundestag ---
snap "abstimmungen_latest" "/api/abstimmungen/latest?limit=3"
snap "bundestag_sitzverteilung" "/api/bundestag/sitzverteilung"
snap "bundestag_abgeordnete" "/api/bundestag/abgeordnete"
snap "bundestag_abgeordneter" "/api/bundestag/abgeordnete/$AWID"
snap "bundestag_abgeordneter_invalid" "/api/bundestag/abgeordnete/999999999"
snap "bundestag_abgeordneter_badid" "/api/bundestag/abgeordnete/abc"
snap "abgeordnete" "/api/abgeordnete"
snap "abgeordnete_votes" "/api/abgeordnete/$AWID/votes"
snap "abgeordnete_votes_badid" "/api/abgeordnete/abc/votes"
snap "bundestag_abstimmungen" "/api/bundestag/abstimmungen"
snap "bundestag_abstimmung" "/api/bundestag/abstimmungen/$POLL"
snap "bundestag_abstimmung_invalid" "/api/bundestag/abstimmungen/999999999"
snap "poll_votes" "/api/bundestag/poll-votes/$POLL"
snap "poll_votes_badid" "/api/bundestag/poll-votes/abc"
snap "abstimmung_flach" "/api/abstimmungen/$POLL"
snap "abstimmung_flach_badid" "/api/abstimmungen/abc"

# --- Urteile ---
snap "urteile_list" "/api/urteile"
snap "urteile_filtered" "/api/urteile?gericht=BGH"
snap "urteile_detail" "/api/urteile/$URTEIL"
snap "urteile_invalid" "/api/urteile/999999999"
snap "urteile_badid" "/api/urteile/abc"

# --- EU-Recht ---
snap "eu_recht_stats" "/api/eu-recht/stats"
snap "eu_recht_list" "/api/eu-recht?limit=5"
snap "eu_recht_detail" "/api/eu-recht/$EURECHT"
snap "eu_recht_invalid" "/api/eu-recht/999999999"

# --- EU-Urteile ---
snap "eu_urteile_stats" "/api/eu-urteile/stats"
snap "eu_urteile_list" "/api/eu-urteile?limit=5"
snap "eu_urteile_detail" "/api/eu-urteile/$EUURTEIL"
snap "eu_urteile_invalid" "/api/eu-urteile/999999999"

# --- Lobbyregister ---
snap "lobby_list" "/api/lobbyregister?limit=5"
snap "lobby_stats" "/api/lobbyregister/stats"
snap "lobby_by_field" "/api/lobbyregister/by-field"
snap "lobby_by_city" "/api/lobbyregister/by-city"
snap "lobby_by_time" "/api/lobbyregister/by-time"
snap "lobby_projects" "/api/lobbyregister/$RN/projects"
snap "lobby_by_gesetz_id" "/api/lobby-projects/by-gesetz-id?gesetz_id=$LOBBYGESETZ"
snap "lobby_by_gesetz_id_invalid" "/api/lobby-projects/by-gesetz-id?gesetz_id=abc"
snap "lobby_gesetze" "/api/lobbyregister/$RN/gesetze"
snap "lobby_by_law" "/api/lobby-projects/by-law?q=BGB"
snap "lobby_projects_stats" "/api/lobby-projects/stats"
snap "lobby_detail" "/api/lobbyregister/$RN"
snap "lobby_detail_invalid" "/api/lobbyregister/R9999999"

# --- Wahlen ---
snap "wahlen_types" "/api/wahlen/types"
snap "wahlen_years" "/api/wahlen/years?typ=federal"
snap "wahlen_years_invalid" "/api/wahlen/years?typ=foo"
snap "wahlen_states" "/api/wahlen/states"
snap "wahlen_map" "/api/wahlen/map?typ=federal&year=2025&metric=spd"
snap "wahlen_map_invalid" "/api/wahlen/map"
snap "wahlen_timeseries" "/api/wahlen/timeseries?typ=federal&party=spd&ags=$AGS"
snap "wahlen_compare" "/api/wahlen/compare?typ=federal&party=spd&ags=$AGS,$AGS2"
snap "wahlen_scatter" "/api/wahlen/scatter?typ=federal&year=2025&x=spd&y=turnout"
snap "wahlen_ranking" "/api/wahlen/ranking?typ=federal&year=2025&party=spd&limit=10"
snap "wahlen_change" "/api/wahlen/change?typ=federal&party=spd&from=2021&to=2025"
snap "wahlen_national_average" "/api/wahlen/national-average?typ=federal&party=spd"
snap "wahlen_stats" "/api/wahlen/stats"
snap "wahlen_region" "/api/wahlen/region/$AGS"
snap "wahlen_region_invalid" "/api/wahlen/region/99999"

# --- World ---
snap "world_categories" "/api/world/categories"
snap "world_indicators" "/api/world/indicators"
snap "world_map" "/api/world/map?indicator=$IND&year=2020"
snap "world_map_invalid" "/api/world/map"
snap "world_country" "/api/world/country/DEU"
snap "world_country_invalid" "/api/world/country/XXX"
snap "world_country_badcode" "/api/world/country/XY"
snap "world_climate" "/api/world/climate/DEU"
snap "world_climate_invalid" "/api/world/climate/XXX"
snap "world_timeseries" "/api/world/timeseries?country=DEU&indicator=$IND"
snap "world_compare" "/api/world/compare?countries=DEU,FRA&indicator=$IND"
snap "world_ranking" "/api/world/ranking?indicator=$IND&year=2020&limit=10"
snap "world_scatter" "/api/world/scatter?x=$IND&y=$IND2&year=2020"
snap "world_sources" "/api/world/sources"
snap "world_stats" "/api/world/stats"
snap "world_trade" "/api/world/trade/DEU"
snap "world_trade_badcode" "/api/world/trade/XY"
snap "world_trade_timeseries" "/api/world/trade/DEU/timeseries?yearMin=2018&yearMax=2023"

# --- 404-Fallback (unbekannte Route) ---
snap "unknown_route" "/api/gibtsnicht"

echo ""
echo "Snapshot: $(ls "$OUT" | wc -l) Dateien in $OUT"
