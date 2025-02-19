#!/usr/bin/env python3
import os
import csv
import time
import requests
import pandas as pd
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import functools

# ----------------------------------------------------------------------------
# Configuration
# ----------------------------------------------------------------------------

WIKI_VERSIONS_FILE      = 'sources/wiki_versions.csv'   # Must have columns: [Wikipedia Name, Language Code]
DOMAINS_FILE            = 'sources/domains.csv'         # Single column of domains (no header)

MAIN_RESULTS_OUTPUT     = 'results/results.csv'
USER_INFO_OUTPUT        = 'results/user_info_all.csv'
# User contributions will be written one file per user in the "results" directory.

TMP_USER_PAIRS_FILE     = 'lang_user_temp.txt'
UNIQUE_USER_PAIRS_FILE  = 'lang_user_unique.txt'

MAX_WORKERS             = 5
SLEEP_BETWEEN_REQUESTS  = 1
USER_CONTRIB_LIMIT      = 10

# ----------------------------------------------------------------------------
# Global Locks & Thread‑Local Session
# ----------------------------------------------------------------------------

# Locks for writing to shared files.
main_writer_lock = threading.Lock()
tmp_file_lock = threading.Lock()
user_info_lock = threading.Lock()

# Use a thread‑local object so that each thread has its own session.
thread_local = threading.local()
def get_session():
	if not hasattr(thread_local, "session"):
		thread_local.session = requests.Session()
	return thread_local.session

# ----------------------------------------------------------------------------
# Ensure Directories Exist
# ----------------------------------------------------------------------------

for d in ['results', 'sources']:
	if not os.path.exists(d):
		os.makedirs(d)

# ----------------------------------------------------------------------------
# Step A: Read Input
# ----------------------------------------------------------------------------

def read_language_codes(file_path):
	print(f"[DEBUG] Reading language codes from: {file_path}")
	df = pd.read_csv(file_path)
	lang_codes = df['Language Code'].dropna().unique().tolist()
	print(f"[DEBUG] Found {len(lang_codes)} language codes.")
	return lang_codes

def read_domains(file_path):
	print(f"[DEBUG] Reading domains from: {file_path}")
	df = pd.read_csv(file_path, header=None, names=['domain'])
	domains_list = df['domain'].dropna().unique().tolist()
	print(f"[DEBUG] Found {len(domains_list)} domains.")
	return domains_list

# ----------------------------------------------------------------------------
# Step B: exturlusage + Introduction User
# ----------------------------------------------------------------------------

def fetch_exturlusage(lang, domain):
	session = get_session()
	base_api_url = f"https://{lang}.wikipedia.org/w/api.php"
	params = {
		"action": "query",
		"format": "json",
		"list": "exturlusage",
		"euquery": domain,
		"eulimit": "500"  # batch size
	}
	while True:
		try:
			r = session.get(url=base_api_url, params=params, timeout=10)
		except Exception as e:
			print(f"[ERROR] fetch_exturlusage: Exception for {lang}:{domain} => {e}")
			break

		if r.status_code != 200:
			print(f"[ERROR] fetch_exturlusage: HTTP {r.status_code} for {lang}:{domain}")
			break

		data = r.json()
		if "query" not in data or "exturlusage" not in data["query"]:
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

		if "continue" in data and "euecontinue" in data["continue"]:
			params["euecontinue"] = data["continue"]["euecontinue"]
		else:
			break

		time.sleep(SLEEP_BETWEEN_REQUESTS)

def fetch_all_revisions(lang, page_title):
	"""
	Generator that streams revisions (avoiding a full in‑memory list).
	"""
	session = get_session()
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

	while True:
		try:
			r = session.get(base_url, params=params, timeout=10)
		except Exception as e:
			print(f"[ERROR] fetch_all_revisions: Exception for {lang}:{page_title} => {e}")
			break

		if r.status_code != 200:
			print(f"[ERROR] fetch_all_revisions: HTTP {r.status_code} for {lang}:{page_title}")
			break

		data = r.json()
		pages = data.get("query", {}).get("pages", {})
		for page_data in pages.values():
			for rev in page_data.get("revisions", []):
				yield {
					"timestamp": rev["timestamp"],
					"user": rev.get("user", ""),
					"content": rev["slots"]["main"].get("*", "")
				}

		if "continue" in data and "rvcontinue" in data["continue"]:
			params["rvcontinue"] = data["continue"]["rvcontinue"]
		else:
			break

		time.sleep(SLEEP_BETWEEN_REQUESTS)

@functools.lru_cache(maxsize=1024)
def find_introduction_user_cached(lang, page_title, url):
	"""
	Cached version to avoid re‑fetching revision history for the same page.
	"""
	return find_introduction_user(url, lang, page_title)

def find_introduction_user(url, lang, page_title):
	"""
	Scans revision history (from earliest to latest) to find who introduced the URL.
	"""
	prev_present = False
	for rev in fetch_all_revisions(lang, page_title):
		content = rev["content"] or ""
		present = (url in content)
		if present and not prev_present:
			return rev["user"], rev["timestamp"]
		prev_present = present
	return None, None

def process_lang_domain_pair(lang, domain, writer, tmp_file_handle):
	print(f"[DEBUG] Starting exturlusage for {lang}:{domain}...")
	count = 0
	for record in fetch_exturlusage(lang, domain):
		# Use the cached function to avoid duplicate work
		user, ts = find_introduction_user_cached(lang, record['page_title'], record['url'])
		record['user'] = user if user else ''
		record['timestamp'] = ts if ts else ''

		with main_writer_lock:
			writer.writerow(record)

		if record['user']:
			with tmp_file_lock:
				tmp_file_handle.write(f"{lang},{record['user']}\n")

		count += 1

	print(f"[DEBUG] Completed {lang}:{domain} => {count} URLs processed.")

# ----------------------------------------------------------------------------
# Step C: User Info & Contributions - Directly Writing to Disk
# ----------------------------------------------------------------------------

USER_INFO_FIELDS = [
	'_Lang', '_User', 'userid', 'name', 'editcount', 'registration',
	'blockinfo', 'cancreate', 'centralids', 'groupmemberships', 'groups',
	'implicitgroups', 'rights', 'emailable'
]

def write_user_info(info):
	row = {field: info.get(field, "") for field in USER_INFO_FIELDS}
	file_exists = os.path.exists(USER_INFO_OUTPUT)
	with open(USER_INFO_OUTPUT, 'a', newline='', encoding='utf-8') as f:
		writer = csv.DictWriter(f, fieldnames=USER_INFO_FIELDS)
		if not file_exists:
			print(f"[DEBUG] Writing header to {USER_INFO_OUTPUT}")
			writer.writeheader()
		writer.writerow(row)

def fetch_user_info(lang, user):
	session = get_session()
	base_url = f"https://{lang}.wikipedia.org/w/api.php"
	params = {
		"action": "query",
		"format": "json",
		"list": "users",
		"ususers": user,
		"usprop": "blockinfo|cancreate|centralids|editcount|groupmemberships|groups|implicitgroups|registration|rights|emailable"
	}
	try:
		r = session.get(base_url, params=params, timeout=10)
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
	session = get_session()
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
			r = session.get(base_url, params=params, timeout=10)
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

		return contributions[:limit]
	except Exception as e:
		print(f"[ERROR] fetch_user_contributions: {lang}:{user} => {e}")
		return []

def process_user_pair(pair):
	lang, user = pair
	print(f"[DEBUG] Processing user '{user}' in {lang}.wikipedia.org")
	
	# Fetch and write user info.
	info = fetch_user_info(lang, user) or {}
	info['_Lang'] = lang
	info['_User'] = user
	with user_info_lock:
		write_user_info(info)

	# Fetch user contributions and write them to a per‑user file.
	contribs = fetch_user_contributions(lang, user, limit=USER_CONTRIB_LIMIT)
	if contribs:
		# Sanitize the username for a safe filename.
		safe_user = "".join(c if c.isalnum() or c in (' ', '.', '_') else '_' for c in user).rstrip()
		filename = f"results/user_contributions_{lang}_{safe_user}.csv"
		# Use the keys from the first contribution; ensure _Lang and _User come first.
		contrib_fields = list(contribs[0].keys())
		if '_Lang' not in contrib_fields:
			contrib_fields.insert(0, '_Lang')
		if '_User' not in contrib_fields:
			contrib_fields.insert(1, '_User')
		with open(filename, 'w', newline='', encoding='utf-8') as f:
			writer = csv.DictWriter(f, fieldnames=contrib_fields)
			writer.writeheader()
			for c in contribs:
				c['_Lang'] = lang
				c['_User'] = user
				writer.writerow(c)
		print(f"[DEBUG] Wrote {len(contribs)} contributions to {filename}")
	else:
		print(f"[DEBUG] No contributions found for {user} on {lang}.wikipedia.org")

# ----------------------------------------------------------------------------
# Main
# ----------------------------------------------------------------------------

def main():
	# Open the main CSV for writing (append mode)
	write_header = not os.path.exists(MAIN_RESULTS_OUTPUT)
	main_fields = ['lang', 'domain', 'url', 'page_title', 'wiki_link', 'user', 'timestamp']
	with open(MAIN_RESULTS_OUTPUT, 'a', newline='', encoding='utf-8') as main_out, \
		 open(TMP_USER_PAIRS_FILE, 'a', encoding='utf-8') as tmp_out:
		
		writer = csv.DictWriter(main_out, fieldnames=main_fields)
		if write_header:
			print(f"[DEBUG] Writing header to {MAIN_RESULTS_OUTPUT}")
			writer.writeheader()

		# Read language codes & domains.
		language_codes = read_language_codes(WIKI_VERSIONS_FILE)
		domains = read_domains(DOMAINS_FILE)

		# Process each (lang, domain) pair concurrently.
		futures = []
		print("[DEBUG] Starting concurrency for (lang, domain) pairs...")
		with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
			for lang in language_codes:
				for domain in domains:
					futures.append(executor.submit(process_lang_domain_pair, lang, domain, writer, tmp_out))
			for fut in as_completed(futures):
				try:
					fut.result()
				except Exception as e:
					print("[ERROR] Worker error in process_lang_domain_pair:", e)

	print(f"[DEBUG] Done collecting main results in {MAIN_RESULTS_OUTPUT}")
	print("[DEBUG] All (lang, domain) pairs have been processed.")

	# De-duplicate (lang, user) pairs using system sort/uniq.
	print("[DEBUG] De-duplicating (lang, user) pairs using sort|uniq...")
	cmd = f"sort {TMP_USER_PAIRS_FILE} | uniq > {UNIQUE_USER_PAIRS_FILE}"
	os.system(cmd)
	print(f"[DEBUG] Created {UNIQUE_USER_PAIRS_FILE}")

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

	# Process user info & contributions concurrently.
	print("[DEBUG] Fetching user info & contributions concurrently...")
	with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
		futures = []
		for pair in unique_pairs:
			futures.append(executor.submit(process_user_pair, pair))
		for fut in as_completed(futures):
			try:
				fut.result()
			except Exception as e:
				print("[ERROR] Worker error in process_user_pair:", e)

	# Final status messages.
	print("[DEBUG] Done! Final outputs:")
	print(f" - {MAIN_RESULTS_OUTPUT} (URL usage + introduction user)")
	print(f" - {USER_INFO_OUTPUT} (user info)")
	print(f" - Per-user contributions files in the 'results' folder")
	print("Temporary files used:")
	print(f" - {TMP_USER_PAIRS_FILE}")
	print(f" - {UNIQUE_USER_PAIRS_FILE}")

if __name__ == "__main__":
	main()
