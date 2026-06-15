/*M!999999\- enable the sandbox mode */ 

/*!40101 SET @OLD_CHARACTER_SET_CLIENT=@@CHARACTER_SET_CLIENT */;
/*!40101 SET @OLD_CHARACTER_SET_RESULTS=@@CHARACTER_SET_RESULTS */;
/*!40101 SET @OLD_COLLATION_CONNECTION=@@COLLATION_CONNECTION */;
/*!40101 SET NAMES utf8mb4 */;
/*!40103 SET @OLD_TIME_ZONE=@@TIME_ZONE */;
/*!40103 SET TIME_ZONE='+00:00' */;
/*!40014 SET @OLD_UNIQUE_CHECKS=@@UNIQUE_CHECKS, UNIQUE_CHECKS=0 */;
/*!40014 SET @OLD_FOREIGN_KEY_CHECKS=@@FOREIGN_KEY_CHECKS, FOREIGN_KEY_CHECKS=0 */;
/*!40101 SET @OLD_SQL_MODE=@@SQL_MODE, SQL_MODE='NO_AUTO_VALUE_ON_ZERO' */;
/*!40111 SET @OLD_SQL_NOTES=@@SQL_NOTES, SQL_NOTES=0 */;
DROP TABLE IF EXISTS `abgeordnete`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `abgeordnete` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `aw_id` int(11) DEFAULT NULL,
  `politiker_id` int(11) DEFAULT NULL,
  `vorname` varchar(100) DEFAULT NULL,
  `nachname` varchar(100) DEFAULT NULL,
  `name` varchar(200) DEFAULT NULL,
  `fraktion` varchar(100) DEFAULT NULL,
  `wahlkreis` varchar(200) DEFAULT NULL,
  `wahlkreis_nr` int(11) DEFAULT NULL,
  `listenplatz` int(11) DEFAULT NULL,
  `profil_url` varchar(500) DEFAULT NULL,
  `foto_url` varchar(500) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `aw_id` (`aw_id`)
) ENGINE=InnoDB AUTO_INCREMENT=630 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `abstimmungen`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `abstimmungen` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `aenderung_id` int(11) DEFAULT NULL,
  `partei` varchar(50) NOT NULL,
  `ja` int(11) DEFAULT 0,
  `nein` int(11) DEFAULT 0,
  `enthalten` int(11) DEFAULT 0,
  `abwesend` int(11) DEFAULT 0,
  `created_at` datetime DEFAULT current_timestamp(),
  `poll_id` int(11) DEFAULT NULL,
  `poll_titel` text DEFAULT NULL,
  `poll_datum` date DEFAULT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_abstimmung_poll_partei` (`poll_id`,`partei`),
  KEY `aenderung_id` (`aenderung_id`),
  KEY `idx_poll_id` (`poll_id`),
  CONSTRAINT `abstimmungen_ibfk_1` FOREIGN KEY (`aenderung_id`) REFERENCES `aenderungen` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=25945 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `aenderungen`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `aenderungen` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `gesetz_id` int(11) NOT NULL,
  `datum` date NOT NULL,
  `diff` mediumtext DEFAULT NULL,
  `zusammenfassung` text DEFAULT NULL,
  `kontext` text DEFAULT NULL,
  `bgbl_referenz` varchar(100) DEFAULT NULL,
  `created_at` datetime DEFAULT current_timestamp(),
  `poll_id` int(11) DEFAULT NULL,
  `zusammenfassung_en` text DEFAULT NULL,
  `quality_ok` tinyint(1) NOT NULL DEFAULT 0,
  PRIMARY KEY (`id`),
  KEY `gesetz_id` (`gesetz_id`),
  CONSTRAINT `aenderungen_ibfk_1` FOREIGN KEY (`gesetz_id`) REFERENCES `gesetze` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=8053 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `climate_koeppen_classes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `climate_koeppen_classes` (
  `class_code` tinyint(3) unsigned NOT NULL,
  `symbol` varchar(4) NOT NULL,
  `major_group` char(1) NOT NULL COMMENT 'A,B,C,D,E nach Köppen-Hauptklassen',
  `name_de` varchar(120) NOT NULL,
  `name_en` varchar(120) NOT NULL,
  `short_name_de` varchar(40) NOT NULL DEFAULT '',
  `short_name_en` varchar(40) NOT NULL DEFAULT '',
  `color_rgb` char(7) NOT NULL COMMENT '#RRGGBB nach Peel et al. 2007',
  `description_de` text DEFAULT NULL,
  `description_en` text DEFAULT NULL,
  PRIMARY KEY (`class_code`),
  UNIQUE KEY `uk_symbol` (`symbol`),
  KEY `idx_major` (`major_group`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `climate_koeppen_distribution`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `climate_koeppen_distribution` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `country_code` varchar(3) NOT NULL,
  `scenario` varchar(16) NOT NULL COMMENT 'historical / ssp119 / ssp126 / ssp245 / ssp370 / ssp434 / ssp460 / ssp585',
  `period` varchar(9) NOT NULL COMMENT 'z.B. 1991_2020, 2041_2070, 2071_2099',
  `class_code` tinyint(3) unsigned NOT NULL,
  `share` decimal(7,4) NOT NULL COMMENT 'Anteil 0..100 (%) der Landfläche dieser Klasse',
  `pixel_count` int(10) unsigned NOT NULL COMMENT 'Pixel dieser Klasse im Landausschnitt',
  `raster_file` varchar(200) DEFAULT NULL COMMENT 'Quelldatei aus dem Beck-Archiv',
  `fetched_at` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_dist` (`country_code`,`scenario`,`period`,`class_code`),
  KEY `idx_country_period` (`country_code`,`period`),
  KEY `idx_scenario_period` (`scenario`,`period`),
  KEY `fk_dist_class` (`class_code`),
  CONSTRAINT `fk_dist_class` FOREIGN KEY (`class_code`) REFERENCES `climate_koeppen_classes` (`class_code`)
) ENGINE=InnoDB AUTO_INCREMENT=6462 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `data_countries`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `data_countries` (
  `iso3` varchar(3) NOT NULL,
  `iso2` varchar(2) DEFAULT NULL,
  `name_de` varchar(200) NOT NULL,
  `name_en` varchar(200) NOT NULL,
  `official_de` varchar(300) DEFAULT NULL,
  `official_en` varchar(300) DEFAULT NULL,
  `region_de` varchar(100) DEFAULT NULL,
  `region_en` varchar(100) DEFAULT NULL,
  `subregion_de` varchar(100) DEFAULT NULL,
  `subregion_en` varchar(100) DEFAULT NULL,
  `income_level` enum('high','upper_middle','lower_middle','low','aggregate') DEFAULT NULL,
  `is_aggregate` tinyint(1) DEFAULT 0,
  `is_un_member` tinyint(1) DEFAULT 1,
  `capital_de` varchar(100) DEFAULT NULL,
  `capital_en` varchar(100) DEFAULT NULL,
  `population` bigint(20) DEFAULT NULL,
  `gdp_total` bigint(20) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`iso3`),
  KEY `idx_region` (`region_en`),
  KEY `idx_income` (`income_level`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `data_indicators`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `data_indicators` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `code` varchar(80) NOT NULL,
  `source_id` int(11) NOT NULL,
  `category` varchar(50) NOT NULL,
  `subcategory` varchar(50) DEFAULT NULL,
  `name_de` varchar(300) NOT NULL,
  `name_en` varchar(300) NOT NULL,
  `description_de` text DEFAULT NULL,
  `description_en` text DEFAULT NULL,
  `unit_de` varchar(100) DEFAULT NULL,
  `unit_en` varchar(100) DEFAULT NULL,
  `value_type` enum('absolute','percent','index','rate','count','currency','ratio','categorical') DEFAULT 'absolute',
  `lower_is_better` tinyint(1) DEFAULT 0,
  `display_decimals` tinyint(4) DEFAULT 2,
  `year_min` int(11) DEFAULT NULL,
  `year_max` int(11) DEFAULT NULL,
  `country_count` int(11) DEFAULT 0,
  `is_active` tinyint(1) DEFAULT 1,
  `priority` int(11) DEFAULT 50,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `code` (`code`),
  KEY `source_id` (`source_id`),
  KEY `idx_category` (`category`),
  KEY `idx_active_priority` (`is_active`,`priority`),
  CONSTRAINT `data_indicators_ibfk_1` FOREIGN KEY (`source_id`) REFERENCES `data_sources` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=107 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `data_sources`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `data_sources` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `slug` varchar(50) NOT NULL,
  `name` varchar(200) NOT NULL,
  `provider` varchar(100) NOT NULL,
  `url` varchar(500) DEFAULT NULL,
  `api_endpoint` varchar(500) DEFAULT NULL,
  `license` varchar(100) DEFAULT NULL,
  `update_freq` enum('daily','weekly','monthly','quarterly','yearly','manual') DEFAULT 'weekly',
  `last_fetched` timestamp NULL DEFAULT NULL,
  `last_success` timestamp NULL DEFAULT NULL,
  `last_error` text DEFAULT NULL,
  `is_active` tinyint(1) DEFAULT 1,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `slug` (`slug`)
) ENGINE=InnoDB AUTO_INCREMENT=13 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `data_update_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `data_update_log` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `source_id` int(11) DEFAULT NULL,
  `indicator_id` int(11) DEFAULT NULL,
  `status` enum('started','success','partial','failed') NOT NULL,
  `records_added` int(11) DEFAULT 0,
  `records_updated` int(11) DEFAULT 0,
  `records_skipped` int(11) DEFAULT 0,
  `duration_seconds` int(11) DEFAULT NULL,
  `error_message` text DEFAULT NULL,
  `context` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`context`)),
  `started_at` timestamp NULL DEFAULT current_timestamp(),
  `finished_at` timestamp NULL DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `source_id` (`source_id`),
  KEY `indicator_id` (`indicator_id`),
  KEY `idx_started` (`started_at`),
  KEY `idx_status` (`status`),
  CONSTRAINT `data_update_log_ibfk_1` FOREIGN KEY (`source_id`) REFERENCES `data_sources` (`id`) ON DELETE SET NULL,
  CONSTRAINT `data_update_log_ibfk_2` FOREIGN KEY (`indicator_id`) REFERENCES `data_indicators` (`id`) ON DELETE SET NULL
) ENGINE=InnoDB AUTO_INCREMENT=16 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `data_values`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `data_values` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `indicator_id` int(11) NOT NULL,
  `country_code` varchar(3) NOT NULL,
  `year` int(11) NOT NULL,
  `value` decimal(20,6) DEFAULT NULL,
  `quality` enum('measured','estimated','projected','interpolated','provisional') DEFAULT 'measured',
  `note` varchar(200) DEFAULT NULL,
  `fetched_at` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_value` (`indicator_id`,`country_code`,`year`),
  KEY `idx_country_year` (`country_code`,`year`),
  KEY `idx_indicator_year` (`indicator_id`,`year`),
  CONSTRAINT `data_values_ibfk_1` FOREIGN KEY (`indicator_id`) REFERENCES `data_indicators` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=351972 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `eu_rechtsakt_gesetze`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `eu_rechtsakt_gesetze` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `eu_rechtsakt_id` int(11) NOT NULL,
  `gesetz_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_link` (`eu_rechtsakt_id`,`gesetz_id`),
  KEY `gesetz_id` (`gesetz_id`),
  CONSTRAINT `eu_rechtsakt_gesetze_ibfk_1` FOREIGN KEY (`eu_rechtsakt_id`) REFERENCES `eu_rechtsakte` (`id`) ON DELETE CASCADE,
  CONSTRAINT `eu_rechtsakt_gesetze_ibfk_2` FOREIGN KEY (`gesetz_id`) REFERENCES `gesetze` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=8 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `eu_rechtsakte`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `eu_rechtsakte` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `celex` varchar(50) NOT NULL,
  `titel_de` text DEFAULT NULL,
  `titel_en` text DEFAULT NULL,
  `typ` enum('REG','DIR','DEC','REC','OTHER') NOT NULL DEFAULT 'OTHER',
  `typ_label` varchar(100) DEFAULT NULL,
  `datum` date DEFAULT NULL,
  `in_kraft` varchar(20) DEFAULT NULL,
  `eurovoc_tags` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`eurovoc_tags`)),
  `zusammenfassung` text DEFAULT NULL,
  `zusammenfassung_de` text DEFAULT NULL,
  `zusammenfassung_en` text DEFAULT NULL,
  `rechtsgebiet` varchar(100) DEFAULT NULL,
  `eurlex_url` varchar(500) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `celex` (`celex`)
) ENGINE=InnoDB AUTO_INCREMENT=66529 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `eu_urteil_rechtsakte`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `eu_urteil_rechtsakte` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `eu_urteil_id` int(11) NOT NULL,
  `eu_rechtsakt_id` int(11) DEFAULT NULL,
  `rechtsakt_celex` varchar(50) DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_urteil` (`eu_urteil_id`),
  KEY `idx_rechtsakt` (`eu_rechtsakt_id`),
  CONSTRAINT `eu_urteil_rechtsakte_ibfk_1` FOREIGN KEY (`eu_urteil_id`) REFERENCES `eu_urteile` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `eu_urteile`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `eu_urteile` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `celex` varchar(50) NOT NULL,
  `ecli` varchar(100) DEFAULT NULL,
  `gericht` enum('EuGH','EuG') NOT NULL,
  `typ` varchar(100) DEFAULT NULL,
  `datum` date DEFAULT NULL,
  `parteien` text DEFAULT NULL,
  `betreff` text DEFAULT NULL,
  `keywords` text DEFAULT NULL,
  `leitsatz` text DEFAULT NULL,
  `zusammenfassung_de` text DEFAULT NULL,
  `zusammenfassung_en` text DEFAULT NULL,
  `auswirkung_de` text DEFAULT NULL,
  `auswirkung_en` text DEFAULT NULL,
  `rechtsgebiet` varchar(100) DEFAULT NULL,
  `eurlex_url` varchar(500) DEFAULT NULL,
  `curia_url` varchar(500) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `quality_ok` tinyint(1) NOT NULL DEFAULT 1,
  PRIMARY KEY (`id`),
  UNIQUE KEY `celex` (`celex`)
) ENGINE=InnoDB AUTO_INCREMENT=567 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_general_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `gesetze`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `gesetze` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `kuerzel` varchar(50) NOT NULL,
  `name` varchar(255) NOT NULL,
  `titel_offiziell` text DEFAULT NULL,
  `amtliche_abkuerzung` varchar(50) DEFAULT NULL,
  `pfad` varchar(255) NOT NULL,
  `zuletzt_geaendert` datetime DEFAULT NULL,
  `created_at` datetime DEFAULT current_timestamp(),
  `ausfertigung_datum` date DEFAULT NULL,
  `fundstelle_periodikum` varchar(64) DEFAULT NULL,
  `fundstelle_zitstelle` varchar(512) DEFAULT NULL,
  `letzter_stand` text DEFAULT NULL,
  `gii_slug` varchar(100) DEFAULT NULL,
  `gii_doknr` varchar(50) DEFAULT NULL,
  `gii_builddate` varchar(20) DEFAULT NULL,
  `gii_last_synced` datetime DEFAULT NULL,
  `status` enum('aktiv','aufgehoben','unbekannt') NOT NULL DEFAULT 'unbekannt',
  PRIMARY KEY (`id`),
  UNIQUE KEY `pfad` (`pfad`),
  KEY `idx_gii_doknr` (`gii_doknr`),
  KEY `idx_gii_slug` (`gii_slug`),
  KEY `idx_titel` (`titel_offiziell`(255))
) ENGINE=InnoDB AUTO_INCREMENT=8125 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `gesetze_sync_log`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `gesetze_sync_log` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `run_started_at` datetime NOT NULL,
  `run_ended_at` datetime DEFAULT NULL,
  `status` enum('running','success','failed') NOT NULL,
  `gesetze_total` int(11) DEFAULT 0,
  `gesetze_neu` int(11) DEFAULT 0,
  `gesetze_geaendert` int(11) DEFAULT 0,
  `gesetze_fehler` int(11) DEFAULT 0,
  `error_message` text DEFAULT NULL,
  PRIMARY KEY (`id`),
  KEY `idx_run_started` (`run_started_at`)
) ENGINE=InnoDB AUTO_INCREMENT=40 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `lobby_gesetze`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `lobby_gesetze` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `project_id` int(11) NOT NULL,
  `gesetz_id` int(11) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uniq_link` (`project_id`,`gesetz_id`),
  KEY `idx_project` (`project_id`),
  KEY `idx_gesetz` (`gesetz_id`)
) ENGINE=InnoDB AUTO_INCREMENT=25076 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `lobby_regulatory_projects`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `lobby_regulatory_projects` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `lobby_register_number` varchar(20) DEFAULT NULL,
  `project_number` varchar(20) DEFAULT NULL,
  `title` text DEFAULT NULL,
  `printing_number` varchar(50) DEFAULT NULL,
  `issuer` varchar(10) DEFAULT NULL,
  `document_url` text DEFAULT NULL,
  `project_url` text DEFAULT NULL,
  `description` text DEFAULT NULL,
  `affected_laws` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`affected_laws`)),
  `leading_ministries` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`leading_ministries`)),
  `fields_of_interest` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`fields_of_interest`)),
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_lobby_project` (`lobby_register_number`,`project_number`)
) ENGINE=InnoDB AUTO_INCREMENT=1323221 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `lobbyregister`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `lobbyregister` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `register_number` varchar(20) DEFAULT NULL,
  `name` text DEFAULT NULL,
  `legal_form` text DEFAULT NULL,
  `city` varchar(100) DEFAULT NULL,
  `country` varchar(100) DEFAULT NULL,
  `active` tinyint(1) DEFAULT NULL,
  `members_count` int(11) DEFAULT NULL,
  `employee_fte` decimal(10,2) DEFAULT NULL,
  `financial_expenses_euro` bigint(20) DEFAULT NULL,
  `financial_year_start` date DEFAULT NULL,
  `financial_year_end` date DEFAULT NULL,
  `fields_of_interest` longtext CHARACTER SET utf8mb4 COLLATE utf8mb4_bin DEFAULT NULL CHECK (json_valid(`fields_of_interest`)),
  `activity_description` text DEFAULT NULL,
  `regulatory_projects_count` int(11) DEFAULT NULL,
  `statements_count` int(11) DEFAULT NULL,
  `details_url` text DEFAULT NULL,
  `first_publication` date DEFAULT NULL,
  `last_update` date DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  `updated_at` timestamp NULL DEFAULT current_timestamp() ON UPDATE current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `register_number` (`register_number`)
) ENGINE=InnoDB AUTO_INCREMENT=451791 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `trade_flows_v2`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `trade_flows_v2` (
  `id` bigint(20) NOT NULL AUTO_INCREMENT,
  `reporter_iso3` varchar(3) NOT NULL,
  `partner_iso3` varchar(3) NOT NULL,
  `flow` enum('export','import') NOT NULL,
  `year` int(11) NOT NULL,
  `value_usd` bigint(20) DEFAULT NULL,
  `hs_section` varchar(20) DEFAULT 'TOTAL',
  `source_id` int(11) NOT NULL,
  `fetched_at` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `uk_trade` (`reporter_iso3`,`partner_iso3`,`flow`,`year`,`hs_section`),
  KEY `source_id` (`source_id`),
  KEY `idx_reporter_year` (`reporter_iso3`,`year`),
  KEY `idx_partner_year` (`partner_iso3`,`year`),
  CONSTRAINT `trade_flows_v2_ibfk_1` FOREIGN KEY (`source_id`) REFERENCES `data_sources` (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=5570477 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `urteil_gesetze`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `urteil_gesetze` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `urteil_id` int(11) NOT NULL,
  `gesetz_kuerzel` varchar(50) NOT NULL,
  PRIMARY KEY (`id`),
  UNIQUE KEY `uq_urteil_gesetz` (`urteil_id`,`gesetz_kuerzel`),
  CONSTRAINT `urteil_gesetze_ibfk_1` FOREIGN KEY (`urteil_id`) REFERENCES `urteile` (`id`) ON DELETE CASCADE
) ENGINE=InnoDB AUTO_INCREMENT=2189 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `urteile`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `urteile` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `doc_id` varchar(50) NOT NULL,
  `gericht` varchar(50) NOT NULL,
  `senat` varchar(100) DEFAULT NULL,
  `typ` varchar(100) DEFAULT NULL,
  `datum` date DEFAULT NULL,
  `aktenzeichen` varchar(100) DEFAULT NULL,
  `ecli` varchar(150) DEFAULT NULL,
  `leitsatz` text DEFAULT NULL,
  `tenor` text DEFAULT NULL,
  `zusammenfassung` text DEFAULT NULL,
  `auswirkung` text DEFAULT NULL,
  `rechtsgebiet` varchar(100) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `doc_id` (`doc_id`)
) ENGINE=InnoDB AUTO_INCREMENT=1474 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `votes`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `votes` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `vote_id` int(11) DEFAULT NULL,
  `poll_id` int(11) DEFAULT NULL,
  `mandate_id` int(11) DEFAULT NULL,
  `abgeordneter_name` varchar(200) DEFAULT NULL,
  `vote` enum('yes','no','abstain','no_show') DEFAULT NULL,
  `fraction_id` int(11) DEFAULT NULL,
  `fraction_label` varchar(200) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `vote_id` (`vote_id`),
  KEY `idx_poll_id` (`poll_id`),
  KEY `idx_mandate_id` (`mandate_id`)
) ENGINE=InnoDB AUTO_INCREMENT=260164 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `wahlen`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `wahlen` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `typ` enum('federal','state','municipal','european','mayoral') NOT NULL,
  `ags` varchar(20) NOT NULL,
  `ags_name` varchar(200) DEFAULT NULL,
  `election_year` int(11) NOT NULL,
  `election_date` date DEFAULT NULL,
  `state` varchar(5) DEFAULT NULL,
  `state_name` varchar(100) DEFAULT NULL,
  `county` varchar(10) DEFAULT NULL,
  `eligible_voters` int(11) DEFAULT NULL,
  `number_voters` int(11) DEFAULT NULL,
  `valid_votes` int(11) DEFAULT NULL,
  `invalid_votes` int(11) DEFAULT NULL,
  `turnout` decimal(8,6) DEFAULT NULL,
  `cdu_csu` decimal(8,6) DEFAULT NULL,
  `spd` decimal(8,6) DEFAULT NULL,
  `gruene` decimal(8,6) DEFAULT NULL,
  `fdp` decimal(8,6) DEFAULT NULL,
  `linke_pds` decimal(8,6) DEFAULT NULL,
  `afd` decimal(8,6) DEFAULT NULL,
  `bsw` decimal(8,6) DEFAULT NULL,
  `npd` decimal(8,6) DEFAULT NULL,
  `freie_waehler` decimal(8,6) DEFAULT NULL,
  `piraten` decimal(8,6) DEFAULT NULL,
  `die_partei` decimal(8,6) DEFAULT NULL,
  `other` decimal(8,6) DEFAULT NULL,
  `far_right` decimal(8,6) DEFAULT NULL,
  `far_left` decimal(8,6) DEFAULT NULL,
  `winning_party` varchar(50) DEFAULT NULL,
  `winner_party` varchar(50) DEFAULT NULL,
  `winner_voteshare` decimal(8,6) DEFAULT NULL,
  `election_type` varchar(50) DEFAULT NULL,
  `round` int(11) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_entry` (`typ`,`ags`,`election_year`,`election_type`,`round`),
  KEY `idx_typ_year` (`typ`,`election_year`),
  KEY `idx_ags` (`ags`),
  KEY `idx_state` (`state`)
) ENGINE=InnoDB AUTO_INCREMENT=54070 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `world_indicator_meta`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `world_indicator_meta` (
  `indicator_code` varchar(50) NOT NULL,
  `indicator_name` varchar(300) DEFAULT NULL,
  `category` varchar(50) DEFAULT NULL,
  `unit` varchar(100) DEFAULT NULL,
  `description_de` text DEFAULT NULL,
  `description_en` text DEFAULT NULL,
  `source` varchar(200) DEFAULT NULL,
  `source_url` varchar(500) DEFAULT NULL,
  PRIMARY KEY (`indicator_code`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
DROP TABLE IF EXISTS `world_indicators`;
/*!40101 SET @saved_cs_client     = @@character_set_client */;
/*!40101 SET character_set_client = utf8mb4 */;
CREATE TABLE `world_indicators` (
  `id` int(11) NOT NULL AUTO_INCREMENT,
  `country_code` varchar(3) NOT NULL,
  `country_name` varchar(200) DEFAULT NULL,
  `region` varchar(100) DEFAULT NULL,
  `income_level` varchar(50) DEFAULT NULL,
  `indicator_code` varchar(50) NOT NULL,
  `indicator_name` varchar(300) DEFAULT NULL,
  `year` int(11) NOT NULL,
  `value` decimal(20,6) DEFAULT NULL,
  `created_at` timestamp NULL DEFAULT current_timestamp(),
  PRIMARY KEY (`id`),
  UNIQUE KEY `unique_entry` (`country_code`,`indicator_code`,`year`),
  KEY `idx_indicator_year` (`indicator_code`,`year`),
  KEY `idx_country` (`country_code`),
  KEY `idx_region` (`region`)
) ENGINE=InnoDB AUTO_INCREMENT=1098274 DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;
/*!40101 SET character_set_client = @saved_cs_client */;
/*!40103 SET TIME_ZONE=@OLD_TIME_ZONE */;

/*!40101 SET SQL_MODE=@OLD_SQL_MODE */;
/*!40014 SET FOREIGN_KEY_CHECKS=@OLD_FOREIGN_KEY_CHECKS */;
/*!40014 SET UNIQUE_CHECKS=@OLD_UNIQUE_CHECKS */;
/*!40101 SET CHARACTER_SET_CLIENT=@OLD_CHARACTER_SET_CLIENT */;
/*!40101 SET CHARACTER_SET_RESULTS=@OLD_CHARACTER_SET_RESULTS */;
/*!40101 SET COLLATION_CONNECTION=@OLD_COLLATION_CONNECTION */;
/*!40111 SET SQL_NOTES=@OLD_SQL_NOTES */;

