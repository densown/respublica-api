<?php
/**
 * Template Name: Gesetze
 *
 * Gesetzesübersicht mit Filter, Pagination und Sortierung (clientseitig).
 * Diese Datei ins Verzeichnis des aktiven Themes legen (oder Theme-Child).
 *
 * API-URL für die Liste: Umgebungsvariable GESETZE_API_URL oder Filter
 * `gesetze_api_list_url` (Standard: /api/gesetze, z. B. per Reverse-Proxy).
 *
 * @package WordPress
 */

if ( ! defined( 'ABSPATH' ) ) {
	exit;
}

$gesetze_api_list_url = apply_filters(
	'gesetze_api_list_url',
	( getenv( 'GESETZE_API_URL' ) !== false && getenv( 'GESETZE_API_URL' ) !== '' )
		? getenv( 'GESETZE_API_URL' )
		: '/api/gesetze'
);

get_header();
?>

<main id="primary" class="site-main">
	<?php
	while ( have_posts() ) :
		the_post();
		?>
		<article id="post-<?php the_ID(); ?>" <?php post_class(); ?>>
			<header class="entry-header">
				<?php the_title( '<h1 class="entry-title">', '</h1>' ); ?>
			</header>

			<div class="entry-content">
				<?php the_content(); ?>

				<div class="gesetze-page-wrap">
					<p class="gesetze-sub"><?php esc_html_e( 'Überarbeitungen und Aktualisierungen im Überblick', 'gesetze' ); ?></p>

					<div id="gesetze-app-error" class="gesetze-err" hidden></div>

					<div class="gesetze-toolbar" id="gesetze-app-toolbar" hidden>
						<div class="gesetze-field">
							<label for="gesetze-filter-domain"><?php esc_html_e( 'Rechtsgebiet', 'gesetze' ); ?></label>
							<select id="gesetze-filter-domain" aria-label="<?php esc_attr_e( 'Rechtsgebiet filtern', 'gesetze' ); ?>">
								<option value="Alle"><?php esc_html_e( 'Alle', 'gesetze' ); ?></option>
								<option value="Zivilrecht"><?php esc_html_e( 'Zivilrecht', 'gesetze' ); ?></option>
								<option value="Sozialrecht"><?php esc_html_e( 'Sozialrecht', 'gesetze' ); ?></option>
								<option value="Steuerrecht"><?php esc_html_e( 'Steuerrecht', 'gesetze' ); ?></option>
								<option value="Strafrecht"><?php esc_html_e( 'Strafrecht', 'gesetze' ); ?></option>
								<option value="Verfassungsrecht"><?php esc_html_e( 'Verfassungsrecht', 'gesetze' ); ?></option>
								<option value="Bundesrecht"><?php esc_html_e( 'Bundesrecht', 'gesetze' ); ?></option>
							</select>
						</div>
						<div class="gesetze-field">
							<label for="gesetze-filter-search"><?php esc_html_e( 'Suche (Kürzel)', 'gesetze' ); ?></label>
							<input type="search" id="gesetze-filter-search" placeholder="<?php esc_attr_e( 'z. B. BGB, StGB …', 'gesetze' ); ?>" autocomplete="off" />
						</div>
						<div class="gesetze-field gesetze-sort-group">
							<span class="gesetze-sort-label"><?php esc_html_e( 'Sortierung', 'gesetze' ); ?></span>
							<button type="button" class="gesetze-btn-sort gesetze-btn-sort-active" id="gesetze-sort-new" data-desc="true"><?php esc_html_e( 'Neueste zuerst', 'gesetze' ); ?></button>
							<button type="button" class="gesetze-btn-sort" id="gesetze-sort-old" data-desc="false"><?php esc_html_e( 'Älteste zuerst', 'gesetze' ); ?></button>
						</div>
					</div>

					<div id="gesetze-app-loading" class="gesetze-loading"><?php esc_html_e( 'Lade Daten …', 'gesetze' ); ?></div>
					<div id="gesetze-app-cards" class="gesetze-cards" hidden></div>

					<div class="gesetze-pager" id="gesetze-app-pager" hidden>
						<span class="gesetze-pager-meta" id="gesetze-pager-meta"></span>
						<div class="gesetze-pager-nav">
							<button type="button" class="gesetze-btn-page" id="gesetze-btn-prev"><?php esc_html_e( '← Zurück', 'gesetze' ); ?></button>
							<button type="button" class="gesetze-btn-page" id="gesetze-btn-next"><?php esc_html_e( 'Weiter →', 'gesetze' ); ?></button>
						</div>
					</div>
				</div>
			</div>
		</article>
		<?php
	endwhile;
	?>
</main>

<style>
	.gesetze-page-wrap {
		--gesetze-bg: #0c0e12;
		--gesetze-surface: #151922;
		--gesetze-surface-hover: #1c2230;
		--gesetze-border: #2a3344;
		--gesetze-text: #e6e9ef;
		--gesetze-muted: #8b95a8;
		--gesetze-accent: #c4a035;
		--gesetze-accent-dim: #8a7330;
		--gesetze-radius: 10px;
		max-width: 960px;
		margin: 0 auto;
		padding: 0 0 2rem;
		font-family: "Segoe UI", system-ui, -apple-system, sans-serif;
		color: var(--gesetze-text);
		background: var(--gesetze-bg);
		margin-top: 1rem;
		padding: 1.25rem 1rem 2rem;
		border-radius: var(--gesetze-radius);
		border: 1px solid var(--gesetze-border);
	}

	.gesetze-sub {
		color: var(--gesetze-muted);
		font-size: 0.95rem;
		margin: 0 0 1.25rem;
	}

	.gesetze-toolbar {
		display: flex;
		flex-wrap: wrap;
		gap: 0.75rem 1rem;
		align-items: flex-end;
		padding: 1rem 1.1rem;
		background: var(--gesetze-surface);
		border: 1px solid var(--gesetze-border);
		border-radius: var(--gesetze-radius);
		margin-bottom: 1.25rem;
	}

	.gesetze-field {
		display: flex;
		flex-direction: column;
		gap: 0.35rem;
		min-width: 0;
	}

	.gesetze-field label,
	.gesetze-sort-label {
		font-size: 0.75rem;
		text-transform: uppercase;
		letter-spacing: 0.06em;
		color: var(--gesetze-muted);
	}

	.gesetze-sort-group {
		flex-direction: row;
		flex-wrap: wrap;
		align-items: center;
		gap: 0.5rem;
	}

	.gesetze-sort-label {
		text-transform: none;
		letter-spacing: 0;
		font-size: 0.85rem;
		margin-right: 0.25rem;
	}

	.gesetze-page-wrap select,
	.gesetze-page-wrap input[type="search"] {
		font: inherit;
		color: var(--gesetze-text);
		background: var(--gesetze-bg);
		border: 1px solid var(--gesetze-border);
		border-radius: 6px;
		padding: 0.5rem 0.65rem;
		min-width: 12rem;
	}

	.gesetze-page-wrap input[type="search"] {
		min-width: 14rem;
	}

	.gesetze-page-wrap select:focus,
	.gesetze-page-wrap input[type="search"]:focus {
		outline: none;
		border-color: var(--gesetze-accent-dim);
		box-shadow: 0 0 0 2px rgba(196, 160, 53, 0.2);
	}

	.gesetze-btn-sort {
		font: inherit;
		cursor: pointer;
		padding: 0.45rem 0.75rem;
		border-radius: 6px;
		border: 1px solid var(--gesetze-border);
		background: var(--gesetze-bg);
		color: var(--gesetze-muted);
		transition: background 0.15s, color 0.15s, border-color 0.15s;
	}

	.gesetze-btn-sort:hover {
		color: var(--gesetze-text);
		background: var(--gesetze-surface-hover);
	}

	.gesetze-btn-sort-active {
		border-color: var(--gesetze-accent-dim);
		color: var(--gesetze-accent);
		background: rgba(196, 160, 53, 0.12);
	}

	.gesetze-cards {
		display: flex;
		flex-direction: column;
		gap: 0.75rem;
	}

	.gesetze-card {
		background: var(--gesetze-surface);
		border: 1px solid var(--gesetze-border);
		border-radius: var(--gesetze-radius);
		padding: 1rem 1.15rem;
		transition: border-color 0.15s, background 0.15s;
	}

	.gesetze-card:hover {
		border-color: #3d4a5f;
		background: var(--gesetze-surface-hover);
	}

	.gesetze-card-head {
		display: flex;
		flex-wrap: wrap;
		align-items: baseline;
		justify-content: space-between;
		gap: 0.5rem;
		margin-bottom: 0.5rem;
	}

	.gesetze-card-kuerzel {
		font-weight: 600;
		font-size: 1.05rem;
		color: var(--gesetze-accent);
	}

	.gesetze-card-date {
		font-size: 0.85rem;
		color: var(--gesetze-muted);
		font-variant-numeric: tabular-nums;
	}

	.gesetze-card-domain {
		display: inline-block;
		font-size: 0.7rem;
		text-transform: uppercase;
		letter-spacing: 0.05em;
		color: var(--gesetze-accent-dim);
		margin-bottom: 0.35rem;
	}

	.gesetze-card-body {
		font-size: 0.92rem;
		color: var(--gesetze-muted);
	}

	.gesetze-card-body p {
		margin: 0;
		display: -webkit-box;
		-webkit-line-clamp: 3;
		-webkit-box-orient: vertical;
		overflow: hidden;
	}

	.gesetze-pager {
		display: flex;
		flex-wrap: wrap;
		align-items: center;
		justify-content: space-between;
		gap: 1rem;
		margin-top: 1.5rem;
		padding-top: 1.25rem;
		border-top: 1px solid var(--gesetze-border);
	}

	.gesetze-pager-meta {
		font-size: 0.9rem;
		color: var(--gesetze-muted);
	}

	.gesetze-pager-nav {
		display: flex;
		gap: 0.5rem;
	}

	.gesetze-btn-page {
		font: inherit;
		cursor: pointer;
		padding: 0.5rem 1rem;
		border-radius: 6px;
		border: 1px solid var(--gesetze-border);
		background: var(--gesetze-surface);
		color: var(--gesetze-text);
		transition: background 0.15s, opacity 0.15s;
	}

	.gesetze-btn-page:hover:not(:disabled) {
		background: var(--gesetze-surface-hover);
		border-color: #3d4a5f;
	}

	.gesetze-btn-page:disabled {
		opacity: 0.35;
		cursor: not-allowed;
	}

	.gesetze-empty {
		text-align: center;
		padding: 2.5rem 1rem;
		color: var(--gesetze-muted);
		border: 1px dashed var(--gesetze-border);
		border-radius: var(--gesetze-radius);
	}

	.gesetze-err {
		padding: 1rem;
		background: rgba(180, 60, 60, 0.15);
		border: 1px solid rgba(180, 60, 60, 0.45);
		border-radius: var(--gesetze-radius);
		color: #f0b0b0;
		margin-bottom: 1rem;
	}

	.gesetze-loading {
		color: var(--gesetze-muted);
		padding: 2rem;
		text-align: center;
	}
</style>

<script>
	window.gesetzeApiListUrl = <?php echo wp_json_encode( $gesetze_api_list_url ); ?>;
</script>
<script>
(function () {
	var PAGE_SIZE = 20;
	var API = window.gesetzeApiListUrl || "/api/gesetze";

	function inferRechtsgebiet(kuerzel, name) {
		var t = (kuerzel || "") + " " + (name || "");
		t = t.toLowerCase();
		if (/\bgg\b|grundgesetz|verfassung|bverf|emrk|grundrechte/.test(t)) {
			return "Verfassungsrecht";
		}
		if (/\bstgb\b|\bstpo\b|straf|owig|\bjgg\b|jugendgericht|waffg|btmg|strafrecht/.test(t)) {
			return "Strafrecht";
		}
		if (/\bsgb\b|sozial|renten|krankenvers|arbeitnehmer|grundsicherung|bürgergeld|arbeitsförderung/.test(t)) {
			return "Sozialrecht";
		}
		if (/\bao\b|\bestg\b|\bustg\b|steuer|abgaben|finanzamt|gewstg|köstg|einkommensteuer/.test(t)) {
			return "Steuerrecht";
		}
		if (/\bbgb\b|\bzpo\b|\bhgb\b|zivil|miet|pacht|famfg|schuldrecht|handelsrecht/.test(t)) {
			return "Zivilrecht";
		}
		return "Bundesrecht";
	}

	var rawData = [];
	var sortDesc = true;
	var filterDomain = "Alle";
	var searchQuery = "";
	var page = 1;

	var el = {
		loading: document.getElementById("gesetze-app-loading"),
		error: document.getElementById("gesetze-app-error"),
		toolbar: document.getElementById("gesetze-app-toolbar"),
		cards: document.getElementById("gesetze-app-cards"),
		pager: document.getElementById("gesetze-app-pager"),
		pagerMeta: document.getElementById("gesetze-pager-meta"),
		filterDomain: document.getElementById("gesetze-filter-domain"),
		filterSearch: document.getElementById("gesetze-filter-search"),
		sortNew: document.getElementById("gesetze-sort-new"),
		sortOld: document.getElementById("gesetze-sort-old"),
		btnPrev: document.getElementById("gesetze-btn-prev"),
		btnNext: document.getElementById("gesetze-btn-next"),
	};

	function showError(msg) {
		el.error.textContent = msg;
		el.error.hidden = false;
	}

	function getFilteredSorted() {
		var rows = rawData.map(function (r) {
			return Object.assign({}, r, {
				rechtsgebiet: inferRechtsgebiet(r.kuerzel, r.name),
			});
		});
		if (filterDomain !== "Alle") {
			rows = rows.filter(function (r) {
				return r.rechtsgebiet === filterDomain;
			});
		}
		var q = searchQuery.trim().toLowerCase();
		if (q) {
			rows = rows.filter(function (r) {
				var k = (r.kuerzel || "").toLowerCase();
				var n = (r.name || "").toLowerCase();
				return k.indexOf(q) !== -1 || n.indexOf(q) !== -1;
			});
		}
		rows.sort(function (a, b) {
			var da = a.datum || "";
			var db = b.datum || "";
			var cmp = sortDesc ? db.localeCompare(da) : da.localeCompare(db);
			if (cmp !== 0) {
				return cmp;
			}
			return (b.id || 0) - (a.id || 0);
		});
		return rows;
	}

	function escapeHtml(s) {
		if (s == null || s === "") {
			return "";
		}
		var d = document.createElement("div");
		d.textContent = s;
		return d.innerHTML;
	}

	function excerpt(z) {
		if (!z || typeof z !== "string") {
			return "Keine Kurzbeschreibung.";
		}
		var t = z.replace(/\s+/g, " ").trim();
		return t.length > 220 ? t.slice(0, 217) + "…" : t;
	}

	function render() {
		var all = getFilteredSorted();
		var total = all.length;
		var totalPages = Math.max(1, Math.ceil(total / PAGE_SIZE));
		if (page > totalPages) {
			page = totalPages;
		}
		if (page < 1) {
			page = 1;
		}
		var start = (page - 1) * PAGE_SIZE;
		var slice = all.slice(start, start + PAGE_SIZE);

		el.cards.innerHTML = "";
		if (slice.length === 0) {
			el.cards.innerHTML =
				'<div class="gesetze-empty">Keine Einträge für die aktuelle Auswahl.</div>';
		} else {
			slice.forEach(function (r) {
				var article = document.createElement("article");
				article.className = "gesetze-card";
				article.innerHTML =
					'<div class="gesetze-card-domain">' +
					escapeHtml(r.rechtsgebiet) +
					"</div>" +
					'<div class="gesetze-card-head">' +
					'<span class="gesetze-card-kuerzel">' +
					escapeHtml(r.kuerzel || "—") +
					"</span>" +
					'<span class="gesetze-card-date">' +
					escapeHtml(r.datum || "") +
					"</span>" +
					"</div>" +
					'<div class="gesetze-card-body"><p>' +
					escapeHtml(excerpt(r.zusammenfassung)) +
					"</p></div>";
				el.cards.appendChild(article);
			});
		}

		el.pagerMeta.textContent =
			"Seite " +
			page +
			" von " +
			totalPages +
			" · " +
			total +
			" Einträge gesamt";

		el.btnPrev.disabled = page <= 1;
		el.btnNext.disabled = page >= totalPages || total === 0;
	}

	function onFilterChange() {
		filterDomain = el.filterDomain.value;
		searchQuery = el.filterSearch.value;
		page = 1;
		render();
	}

	el.filterDomain.addEventListener("change", onFilterChange);
	el.filterSearch.addEventListener("input", onFilterChange);

	el.sortNew.addEventListener("click", function () {
		sortDesc = true;
		el.sortNew.classList.add("gesetze-btn-sort-active");
		el.sortOld.classList.remove("gesetze-btn-sort-active");
		page = 1;
		render();
	});

	el.sortOld.addEventListener("click", function () {
		sortDesc = false;
		el.sortOld.classList.add("gesetze-btn-sort-active");
		el.sortNew.classList.remove("gesetze-btn-sort-active");
		page = 1;
		render();
	});

	el.btnPrev.addEventListener("click", function () {
		if (page > 1) {
			page -= 1;
			render();
		}
	});

	el.btnNext.addEventListener("click", function () {
		var all = getFilteredSorted();
		var totalPages = Math.max(1, Math.ceil(all.length / PAGE_SIZE));
		if (page < totalPages) {
			page += 1;
			render();
		}
	});

	fetch(API, { credentials: "omit" })
		.then(function (res) {
			if (!res.ok) {
				throw new Error("API-Fehler: " + res.status);
			}
			return res.json();
		})
		.then(function (data) {
			if (!Array.isArray(data)) {
				throw new Error("Ungültige Antwort");
			}
			rawData = data;
			el.loading.hidden = true;
			el.toolbar.hidden = false;
			el.cards.hidden = false;
			el.pager.hidden = false;
			page = 1;
			render();
		})
		.catch(function (e) {
			el.loading.hidden = true;
			showError(e.message || "Daten konnten nicht geladen werden.");
		});
})();
</script>

<?php
get_footer();
