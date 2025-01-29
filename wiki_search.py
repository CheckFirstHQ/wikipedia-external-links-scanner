#!/usr/bin/env python3
import os
import csv
import time
import requests
import pandas as pd
from concurrent.futures import ThreadPoolExecutor, as_completed

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

WIKI_VERSIONS_FILE      = 'sources/wiki_versions.csv'   # Must have columns: [Wikipedia Name, Language Code]
DOMAINS_FILE            = 'sources/domains.csv'         # Single column of domains (no header)

MAIN_RESULTS_OUTPUT     = 'results/results.csv'
USER_INFO_OUTPUT        = 'results/user_info_all.csv'
USER_CONTRIBS_OUTPUT    = 'results/user_contributions_all.csv'

# Temporary files for storing user pairs and deduplication
TMP_USER_PAIRS_FILE     = 'lang_user_temp.txt'
UNIQUE_USER_PAIRS_FILE  = 'lang_user_unique.txt'

# Max concurrency to avoid overwhelming the Wikipedia API
MAX_WORKERS             = 5

# Courtesy sleep between requests
SLEEP_BETWEEN_REQUESTS  = 1

# Number of user contributions to fetch
USER_CONTRIB_LIMIT      = 10

# Global Requests session
S = requests.Session()

# ----------------------------------------------------------------------------
# Step A: Read Input
# ----------------------------------------------------------------------------

def read_language_codes(file_path):
	"""
	Reads wiki_versions.csv to extract valid language codes.
	Expected columns: "Wikipedia Name", "Language Code".
	"""
	print(f"[DEBUG] Reading language codes from: {file_path}")
	df = pd.read_csv(file_path)
	lang_codes = df['Language Code'].dropna().unique().tolist()
	print(f"[DEBUG] Found {len(lang_codes)} language codes.")
	return lang_codes

def read_domains(file_path):
	"""
	Reads domains.csv (single column, no header) to get a list of domains.
	"""
	print(f"[DEBUG] Reading domains from: {file_path}")
	df = pd.read_csv(file_path, header=None, names=['domain'])
	domains_list = df['domain'].dropna().unique().tolist()
	print(f"[DEBUG] Found {len(domains_list)} domains.")
	return domains_list

# ----------------------------------------------------------------------------
# Step B: exturlusage + introduction user
# ----------------------------------------------------------------------------

def fetch_exturlusage(lang, domain):
	"""
	Generator that yields external link usage from lang.wikipedia.org for a given domain.
	Yields dicts: { lang, domain, url, page_title, wiki_link }.
	"""
	base_api_url = f"https://{lang}.wikipedia.org/w/api.php"
	params = {
		"action": "query",
		"format": "json",
		"list": "exturlusage",
		"euquery": domain,
		"eulimit": "500"  # batch size
	}

	while True:
		r = S.get(url=base_api_url, params=params)
		if r.status_code != 200:
			print(f"[ERROR] fetch_exturlusage: HTTP {r.status_code} for {lang}:{domain}")
			break

		data = r.json()
		if "query" not in data or "exturlusage" not in data["query"]:
			# No more data found
			break

		exturl_list = data["query"]["exturlusage"]
		for item in exturl_list:
			ext_url = item.get("url", "")
			page_title = item.get("title", "")
			wiki_link = f"https://{lang}.wikipedia.org/wiki/{page_title.replace(' ', '_')}"
			yield {
				'lang': lang,
				'domain': domain,
				'url': ext_url,
				'page_title': page_title,
				'wiki_link': wiki_link
			}

		# Check for continuation
		if "continue" in data and "euecontinue" in data["continue"]:
			params["euecontinue"] = data["continue"]["euecontinue"]
		else:
			break

		time.sleep(SLEEP_BETWEEN_REQUESTS)  # be nice to Wikipedia

def fetch_all_revisions(lang, page_title):
	"""
	Fetches all revisions from earliest to latest for a page in 'lang'. 
	Returns a list of {timestamp, user, content}.
	"""
	base_url = f"https://{lang}.wikipedia.org/w/api.php"
	params = {
		"action": "query",
		"prop": "revisions",
		"titles": page_title,
		"rvslots": "main",
		"rvprop": "timestamp|user|content",
		"rvdir": "newer",
		"rvlimit": "max",
		"format": "json"
	}

	revisions = []
	while True:
		r = S.get(base_url, params=params)
		if r.status_code != 200:
			print(f"[ERROR] fetch_all_revisions: HTTP {r.status_code} for {lang}:{page_title}")
			break

		data = r.json()
		pages = data.get("query", {}).get("pages", {})
		for _, page_data in pages.items():
			if "revisions" in page_data:
				for rev in page_data["revisions"]:
					rev_info = {
						"timestamp": rev["timestamp"],
						"user": rev.get("user", ""),
						"content": rev["slots"]["main"].get("*", "")
					}
					revisions.append(rev_info)

		# Check continuation
		if "continue" in data and "rvcontinue" in data["continue"]:
			params["rvcontinue"] = data["continue"]["rvcontinue"]
		else:
			break

		time.sleep(SLEEP_BETWEEN_REQUESTS)

	return revisions

def find_introduction_user(url, lang, page_title):
	"""
	Scans revision history from earliest to latest to find who introduced 'url'.
	Returns (user, timestamp) or (None, None).
	"""
	revisions = fetch_all_revisions(lang, page_title)
	if not revisions:
		return None, None

	prev_present = False
	for rev in revisions:
		content = rev["content"] or ""
		present = (url in content)
		if present and not prev_present:
			# Found the introduction
			return rev["user"], rev["timestamp"]
		prev_present = present

	return None, None

def process_lang_domain_pair(lang, domain, writer, tmp_file_handle):
	"""
	Streams through all exturlusage for (lang, domain). For each link:
	  - find introduction user
	  - write row to 'results_with_user.csv'
	  - write (lang, user) to 'lang_user_temp.txt' (one line)
	"""
	print(f"[DEBUG] Starting exturlusage for {lang}:{domain}...")
	count = 0
	for record in fetch_exturlusage(lang, domain):
		# For each usage, find introduction user
		user, ts = find_introduction_user(record['url'], lang, record['page_title'])
		record['user'] = user if user else ''
		record['timestamp'] = ts if ts else ''

		# Write row to main CSV
		writer.writerow(record)

		# Write (lang, user) to temp file if user is not empty
		if record['user']:
			line = f"{lang},{record['user']}\n"
			tmp_file_handle.write(line)

		count += 1

	print(f"[DEBUG] Completed {lang}:{domain} => {count} URLs processed.")

# ----------------------------------------------------------------------------
# Step C: User Info & Contributions
# ----------------------------------------------------------------------------

def fetch_user_info(lang, user):
	"""
	Fetches user info from the API for (lang, user).
	Returns a dict (may be empty if not found).
	"""
	base_url = f"https://{lang}.wikipedia.org/w/api.php"
	params = {
		"action": "query",
		"format": "json",
		"list": "users",
		"ususers": user,
		"usprop": "blockinfo|cancreate|centralids|editcount|groupmemberships|groups|implicitgroups|registration|rights|emailable"
	}
	try:
		r = S.get(base_url, params=params)
		if r.status_code != 200:
			print(f"[ERROR] fetch_user_info: HTTP {r.status_code} for {lang}:{user}")
			return {}
		data = r.json()
		users_data = data.get("query", {}).get("users", [])
		if not users_data:
			return {}
		return users_data[0]
	except Exception as e:
		print(f"[ERROR] fetch_user_info: {lang}:{user} => {e}")
		return {}

def fetch_user_contributions(lang, user, limit=10):
	"""
	Fetches up to 'limit' contributions for (lang, user).
	Returns a list of dicts.
	"""
	base_url = f"https://{lang}.wikipedia.org/w/api.php"
	params = {
		"action": "query",
		"format": "json",
		"list": "usercontribs",
		"ucuser": user,
		"ucprop": "ids|title|timestamp|comment|size|sizediff|tags|flags",
		"uclimit": min(limit, 500)
	}
	contributions = []
	try:
		while True:
			r = S.get(base_url, params=params)
			if r.status_code != 200:
				print(f"[ERROR] fetch_user_contributions: HTTP {r.status_code} for {lang}:{user}")
				break
			data = r.json()
			ucs = data.get("query", {}).get("usercontribs", [])
			contributions.extend(ucs)
			
			if "continue" in data and "uccontinue" in data["continue"]:
				params["uccontinue"] = data["continue"]["uccontinue"]
				if len(contributions) >= limit:
					break
				time.sleep(SLEEP_BETWEEN_REQUESTS)
			else:
				break

		# Return at most 'limit'
		return contributions[:limit]
	except Exception as e:
		print(f"[ERROR] fetch_user_contributions: {lang}:{user} => {e}")
		return []

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def main():
	# Step 1: Open the main CSV for writing (append if it already exists)
	write_header = not os.path.exists(MAIN_RESULTS_OUTPUT)
	main_fields = ['lang', 'domain', 'url', 'page_title', 'wiki_link', 'user', 'timestamp']
	with open(MAIN_RESULTS_OUTPUT, 'a', newline='', encoding='utf-8') as main_out:
		writer = csv.DictWriter(main_out, fieldnames=main_fields)
		if write_header:
			print(f"[DEBUG] Writing header to {MAIN_RESULTS_OUTPUT}")
			writer.writeheader()

		# Step 2: Also open the temp file for user pairs
		with open(TMP_USER_PAIRS_FILE, 'a', encoding='utf-8') as tmp_out:
			
			# Step A: Read language codes & domains
			language_codes = read_language_codes(WIKI_VERSIONS_FILE)
			domains = read_domains(DOMAINS_FILE)

			# Step B: Launch concurrency for all (lang, domain)
			futures = []
			print("[DEBUG] Starting concurrency for (lang, domain) pairs...")
			with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
				for lang in language_codes:
					for domain in domains:
						future = executor.submit(
							process_lang_domain_pair,
							lang, domain,
							writer,
							tmp_out
						)
						futures.append(future)

				# Wait for all tasks to finish
				for fut in as_completed(futures):
					try:
						fut.result()
					except Exception as e:
						print("[ERROR] Worker error in process_lang_domain_pair:", e)

	print(f"[DEBUG] Done collecting main results in {MAIN_RESULTS_OUTPUT}")
	print(f"[DEBUG] All (lang, domain) pairs have been processed.")

	# Step C: Sort & deduplicate (lang, user) on disk
	print("[DEBUG] De-duplicating (lang, user) pairs using sort|uniq...")
	# Make sure the environment has 'sort' and 'uniq'
	cmd = f"sort {TMP_USER_PAIRS_FILE} | uniq > {UNIQUE_USER_PAIRS_FILE}"
	os.system(cmd)
	print(f"[DEBUG] Created {UNIQUE_USER_PAIRS_FILE}")

	# Step D: Read unique (lang, user) from that file
	unique_pairs = []
	with open(UNIQUE_USER_PAIRS_FILE, 'r', encoding='utf-8') as f:
		for line in f:
			line = line.strip()
			if not line:
				continue
			lang, user = line.split(',', 1)
			unique_pairs.append((lang, user))
	print(f"[DEBUG] Found {len(unique_pairs)} unique (lang, user) pairs.")

	if not unique_pairs:
		print("[DEBUG] No users found at all. Exiting.")
		return

	# Step E: Fetch user info + contributions, storing results in memory
	user_info_rows = []
	user_contrib_rows = []

	def process_user_pair(pair):
		lang, user = pair
		print(f"[DEBUG] Processing user '{user}' in {lang}.wikipedia.org")
		info = fetch_user_info(lang, user) or {}
		info['_Lang'] = lang
		info['_User'] = user
		user_info_rows.append(info)

		contribs = fetch_user_contributions(lang, user, limit=USER_CONTRIB_LIMIT)
		for c in contribs:
			c['_Lang'] = lang
			c['_User'] = user
			user_contrib_rows.append(c)

	# Concurrency for user data
	print("[DEBUG] Fetching user info & contributions concurrently...")
	futures = []
	with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
		for pair in unique_pairs:
			fut = executor.submit(process_user_pair, pair)
			futures.append(fut)

		for fut in as_completed(futures):
			try:
				fut.result()
			except Exception as e:
				print("[ERROR] Worker error in process_user_pair:", e)

	# Step F: Write user info
	print(f"[DEBUG] Writing user info to {USER_INFO_OUTPUT}")
	if user_info_rows:
		df_info = pd.DataFrame(user_info_rows)
		if not df_info.empty:
			# Reorder columns so _Lang, _User come first
			cols = list(df_info.columns)
			for c in ('_Lang','_User'):
				if c in cols: cols.remove(c)
			cols = ['_Lang','_User'] + sorted(cols)
			df_info = df_info.reindex(columns=cols)
		df_info.to_csv(USER_INFO_OUTPUT, index=False, encoding='utf-8')
	else:
		pd.DataFrame().to_csv(USER_INFO_OUTPUT, index=False, encoding='utf-8')

	# Step G: Write user contributions
	print(f"[DEBUG] Writing user contributions to {USER_CONTRIBS_OUTPUT}")
	if user_contrib_rows:
		df_contribs = pd.DataFrame(user_contrib_rows)
		if not df_contribs.empty:
			cols = list(df_contribs.columns)
			for c in ('_Lang','_User'):
				if c in cols: cols.remove(c)
			cols = ['_Lang','_User'] + sorted(cols)
			df_contribs = df_contribs.reindex(columns=cols)
		df_contribs.to_csv(USER_CONTRIBS_OUTPUT, index=False, encoding='utf-8')
	else:
		pd.DataFrame().to_csv(USER_CONTRIBS_OUTPUT, index=False, encoding='utf-8')

	print("[DEBUG] Done! Final outputs:")
	print(f" - {MAIN_RESULTS_OUTPUT} (URL usage + introduction user)")
	print(f" - {USER_INFO_OUTPUT} (user info)")
	print(f" - {USER_CONTRIBS_OUTPUT} (user contributions)")
	print(f"Temporary files used:")
	print(f" - {TMP_USER_PAIRS_FILE}")
	print(f" - {UNIQUE_USER_PAIRS_FILE}")

if __name__ == "__main__":
	main()
