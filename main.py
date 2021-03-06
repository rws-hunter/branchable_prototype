import os
import sqlite3
from dataclasses import dataclass

# Data class for a site option entity
@dataclass
class SiteOption:
	site_id: int
	version_id: int
	brand: str
	pn: str
	dp_id: int
	on_site: bool

# Create schema if not already active
def create_tables(con):
	# Create a table to store site data
	con.execute('''
	CREATE TABLE IF NOT EXISTS sites(
		-- Lookup columns
		-- ROWID is automatic in SQLite
		site_id integer not null,
		-- Trunk version number
		-- This is the "published" version of data
		trunk_version_id integer default 0,
		-- Branch version number
		-- This is the "pending" version of data
		branch_version_id integer default 1);
	''')
	# Create a unique index for sites on site_id
	con.execute('''
	CREATE UNIQUE INDEX IF NOT EXISTS idx_sites ON sites(site_id);
	''')
	# Create a table to store changelog entries
	con.execute('''
	CREATE TABLE IF NOT EXISTS site_changes(
		site_id integer not null,
		version_id integer not null,
		description text not null,
		is_publish integer not null default false,
		created_at timestamp default current_timestamp);
	''')
	# Create a index for site_changes on site_id and version_id
	con.execute('''
	CREATE INDEX IF NOT EXISTS idx_changelogs ON site_changes(site_id, version_id);
	''')
	# Create a table to store item option data and fills
	con.execute('''
	CREATE TABLE IF NOT EXISTS site_options(
		-- Lookup columns
		-- ROWID is automatic in SQLite
		site_id integer not null,    -- Not fillable
		version_id integer not null, -- Not fillable
		brand text not null,         -- Fill value = *
		pn text not null,            -- Fill value = *
		dp_id integer not null,      -- Fill value = 0
		-- Data columns, these need to be nullable for our fill scheme to work
		-- SQLite doesn't have boolean types
		on_site integer);
	''')
	# Create a unique index over the lookup values. 
	# This allows the insert-or-replace commands to work and ensure fills don't overlap.
	con.execute('''
	CREATE UNIQUE INDEX IF NOT EXISTS idx_site_options ON site_options(site_id, version_id, brand, pn, dp_id);
	''')
	con.commit()

# Helper method to get a human-readable description for a change to a site option
def get_change_desc_for_site_option(brand, pn, dp_id, on_site):
	is_fill = brand is None or pn is None or dp_id is None
	action = "Fill" if is_fill else "Set"

	desc = f"{action}"
	if brand:
		desc = desc + f" {brand}"
	if pn:
		desc = desc + f":{pn}"
	if dp_id:
		desc = desc + f" option {dp_id}"
	desc = desc + f" on_site={on_site}"
	return desc

# Store a site option or fill into the database
def store_site_option(con, site_id, brand, pn, dp_id, on_site):
	# First, check that the site_id is valid
	assert site_id is not None
	# First, get the branch version. Ideally this would be cached before hand to eliminate the extra query.
	# NOTE: We always store to the branch (cms) version. 
	branch_version_id = get_site_branch_version(con, site_id)
	# Get the description before we mess with the parameters
	desc = get_change_desc_for_site_option(brand, pn, dp_id, on_site)

	# If the row is a fill, first "delete" any rows that the fill would affect
	if brand is None or pn is None or dp_id is None :
		params = { "site_id": site_id, "branch_version_id": branch_version_id, "brand": brand, "pn": pn, "dp_id":dp_id }

		# Build a string to match predicates specified in the parameters
		predicates = "site_id=:site_id AND version_id<=:branch_version_id"
		if brand:
			predicates = predicates + " AND brand=:brand"
		if pn:
			predicates = predicates + " AND pn=:pn"
		if dp_id:
			predicates = predicates + " AND dp_id=:dp_id"

		con.execute(f'''
		INSERT OR REPLACE INTO site_options(version_id, site_id, brand, pn, dp_id, on_site) 
		SELECT :branch_version_id, a.site_id, a.brand, a.pn, a.dp_id, null
		FROM site_options a
		INNER JOIN (
			SELECT MAX(version_id) as version_id, site_id, brand, pn, dp_id
			FROM site_options
			WHERE {predicates}
			GROUP BY site_id, brand, pn, dp_id
		) b
		ON a.site_id=b.site_id AND a.version_id=b.version_id AND a.brand=b.brand AND a.pn=b.pn AND a.dp_id=a.dp_id;
		''', params)

	# Validate any fillable columns, swap them with their fill values if they are null (signaling a fill)
	brand = brand if brand is not None else '*'
	pn = pn if pn is not None else '*'
	dp_id = dp_id if dp_id is not None else 0
	# Insert the row
	params = { 
		"site_id": site_id, "version_id": branch_version_id,
		"brand": brand, "pn": pn, "dp_id": dp_id, 
		"on_site": on_site
	}
	con.execute('''
	INSERT OR REPLACE INTO site_options(site_id, version_id, brand, pn, dp_id, on_site) 
	VALUES (:site_id, :version_id, :brand, :pn, :dp_id, :on_site); 
	''', params)
	# Insert a changelog entry for the change
	con.execute('''
	INSERT INTO site_changes(site_id, version_id, description) 
	VALUES (:site_id, :version_id, :desc)
	''', {"site_id": site_id, "version_id": branch_version_id, "desc": desc})
	# Execute the transaction
	con.commit()

# Fetch a single site option from the database
def fetch_site_option(con, site_id, brand, pn, dp_id):
	# First, check that the site_id is valid
	assert site_id is not None
	# Get the trunk version. Ideally this would be cached before hand to eliminate the extra query.
	# NOTE: We always fetch from the trunk (live) version. 
	trunk_version_id = get_site_trunk_version(con, site_id)
	params = { "site_id": site_id, "version_id": trunk_version_id, "brand": brand, "pn": pn, "dp_id": dp_id }
	# Use a coalesce'd select query to get the data for the site option, taking into account fills
	# Q: Why use ORDER BY version_id DESC LIMIT 1 instead of MAX(version_id)? 
	# A: This is done because the trunk version won't actually be the highest version_id, the branch version will. 
	# And ORDER BY/LIMIT isn't actually that slow in my testing, provided the version_id is indexed
	query = con.execute('''
	WITH _fill_tbl(site_id, version_id, brand, pn, dp_id, on_site) AS (VALUES (
		:site_id, :version_id, :brand, :pn, :dp_id,
		-- Query most specific to least specific, looking for a result 
		COALESCE(
			-- Item specific check
			(SELECT on_site FROM site_options WHERE site_id=:site_id AND version_id<=:version_id AND brand=:brand AND pn=:pn   AND dp_id=:dp_id ORDER BY version_id DESC LIMIT 1),
			-- Fill for all options on item (site-specific)
			(SELECT on_site FROM site_options WHERE site_id=:site_id AND version_id<=:version_id AND brand=:brand AND pn=:pn   AND dp_id=0 ORDER BY version_id DESC LIMIT 1),
			-- FIll for all items in brand (site-specific)
			(SELECT on_site FROM site_options WHERE site_id=:site_id AND version_id<=:version_id AND brand=:brand AND pn=\'*\' AND dp_id=0 ORDER BY version_id DESC LIMIT 1),
			-- Default fill value for the site
			(SELECT on_site FROM site_options WHERE site_id=:site_id AND version_id<=:version_id AND brand=\'*\'  AND pn=\'*\' AND dp_id=0 ORDER BY version_id DESC LIMIT 1),
			-- Default fallback value
			TRUE)
	))
	SELECT * FROM _fill_tbl;
	''', params);
	# Fetch the result and convert
	result = query.fetchone()
	if result:
		return SiteOption(result[0], result[1], result[2], result[3], result[4], (result[5] == 1))
	return None

# Create a site entry with default values
def create_site(con, site_id):
	params = { "site_id": site_id }
	con.execute('''
	INSERT OR REPLACE INTO sites(site_id, trunk_version_id, branch_version_id) VALUES (:site_id, 0, 1); 
	''', params)
	con.commit()

# Get the trunk ID for the site
def get_site_trunk_version(con, site_id):
	query  = con.execute('''SELECT trunk_version_id FROM sites WHERE site_id=:site_id''', { "site_id": site_id })
	result = query.fetchone()
	assert(result is not None)
	return result[0]

# Get the branch ID for the site
def get_site_branch_version(con, site_id):
	query  = con.execute('''SELECT branch_version_id FROM sites WHERE site_id=:site_id''', { "site_id": site_id })
	result = query.fetchone()
	assert(result is not None)
	return result[0]

# Print the change log entries for a version to the std out
def print_changelog_for_version(con, site_id, version_id):
	# Get changelog entries for version
	params = { "site_id": site_id, "version_id": version_id }
	query = con.execute('''
	SELECT * FROM site_changes WHERE site_id=:site_id AND version_id=:version_id;
	''', params)
	rows = query.fetchall()
	# Pretty print
	print(f"Changelog for version #{version_id}:")
	if not rows:
		print("\tEmpty")
	for row in rows:
		print(f"[{row[4]}] {row[2]}")

# Publish the current branch changes to the trunk
def publish_site(con, site_id, desc=None):
	# First, get the branch version. Ideally this would be cached before hand to eliminate the extra query. 
	branch_version_id = get_site_branch_version(con, site_id)
	# Get the description for the publish if none was provided
	desc = desc if desc else f"Publish changes for version #{branch_version_id}"
	# Swap the trunk version for the branch version, increment the branch version
	params = { "site_id": site_id }
	con.execute('''
	UPDATE sites SET 
		trunk_version_id =branch_version_id, 
		branch_version_id=branch_version_id+1 
	WHERE site_id=:site_id;
	''', params)
	# Insert a changelog entry for the change
	con.execute('''
	INSERT INTO site_changes(site_id, version_id, description, is_publish) 
	VALUES (:site_id, :version_id, :desc, true)
	''', {"site_id": site_id, "version_id": branch_version_id, "desc": desc})
	con.commit()

# Rollback the site data to a prior version
def rollback_site(con, site_id, to_version_id):
	# First, get the branch version. Ideally this would be cached before hand to eliminate the extra query.
	# NOTE: We always store to the branch (cms) version. 
	branch_version_id = get_site_branch_version(con, site_id)
	params = { "site_id": site_id, "to_version_id": to_version_id, "branch_version_id": branch_version_id }
	# Clear out any pending changes. 
	# This needs to be done since all "rolled back" data is added to the branch version before being published.
	con.execute('''
	DELETE FROM site_options WHERE version_id=:branch_version_id;
	''', params)
	# Copy the rows that existed at the selected version, with the current branch version as the version_id
	# This makes a more recent copy of the data that existed at the point we're rolling back to
	con.execute('''
	INSERT INTO site_options(version_id, site_id, brand, pn, dp_id, on_site) 
	SELECT :branch_version_id, a.site_id, a.brand, a.pn, a.dp_id, a.on_site 
	FROM site_options a
	INNER JOIN (
		SELECT MAX(version_id) as version_id, site_id, brand, pn, dp_id 
		FROM site_options 
		WHERE version_id<=:to_version_id AND site_id=:site_id
		GROUP BY brand, pn, dp_id
	) b
	ON a.brand=b.brand AND a.pn=b.pn AND a.dp_id=b.dp_id AND a.version_id=b.version_id;
	''', params)
	# Get the items that don't exist in the new version, and insert them with the branch version_id and null data values
	# This effectively "deletes" any rows that shouldn't exist at the version we're rolling back to. 
	# We don't actually want to delete any data, since that would make it impossible to rollback to previous points after this rollback
	con.execute('''
	WITH _tmp_tble_items_to_delete(version_id, site_id, brand, pn, dp_id) AS
	(
		SELECT MAX(version_id) as version_id, site_id, brand, pn, dp_id
			FROM site_options 
			WHERE version_id<=:branch_version_id AND site_id=:site_id
			GROUP BY brand, pn, dp_id
		EXCEPT
		SELECT version_id, site_id, brand, pn, dp_id
			FROM site_options
			WHERE version_id=:branch_version_id AND site_id=:site_id
	)
	INSERT INTO site_options(version_id, site_id, brand, pn, dp_id, on_site) 
	SELECT :branch_version_id, site_id, brand, pn, dp_id, NULL FROM _tmp_tble_items_to_delete;
	''', params)
	# Create a description for the publish
	desc = f"Rolled back site settings to version #{to_version_id}"
	# Publish branch
	# NOTE: This is where the transaction is committed to keep database consistency
	publish_site(con, site_id, desc)

# Assertion helper for testing
def assert_match(site_option, version_id, on_site):
	assert site_option.version_id == version_id, f'version_id is {site_option.version_id}, should be {version_id}'
	assert site_option.on_site == on_site, f'on_site is {site_option.on_site}, should be {on_site}'
# Test cases to ensure correctness of algorithm
def run_tests(con):
	# Set up some test data
	site_id = 8080
	brand   = "ASHLEY"
	pns     = ["000111", "000112", "000113"]
	dp_ids  = [1000001, 1000002, 1000003]

	print("Create the site")
	create_site(con, site_id)

	print("Store version 1")
	store_site_option(con, site_id, brand, pns[0], dp_ids[0], True) # Store a specific value 
	store_site_option(con, site_id, brand, pns[0], dp_ids[1], True) # Store a specific value 
	store_site_option(con, site_id, brand, pns[0], dp_ids[2], True) # Store a specific value 
	publish_site(con, site_id)

	print_changelog_for_version(con, site_id, 1)

	print("Store version 2")
	store_site_option(con, site_id, brand, pns[0], dp_ids[1], False) # Store a specific value 
	publish_site(con, site_id)

	print_changelog_for_version(con, site_id, 2)

	print("Store version 3")
	store_site_option(con, site_id, brand, pns[0], dp_ids[0], False) # Store a specific value 
	store_site_option(con, site_id, brand, pns[1], None, False)      # Store a fill on_site=false over the item ASHLEY:000112
	publish_site(con, site_id)
	
	print_changelog_for_version(con, site_id, 3)

	print("Get the current version (version 3)")
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[0]), 3, False) # Version should be 3, on_site=False
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[1]), 3, False) # Version should be 3, on_site=False
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[2]), 3, True)  # Version should be 3, on_site=True
	assert_match(fetch_site_option(con, site_id, brand, pns[1], dp_ids[2]), 3, False) # Version should be 3, on_site=False, Value taken from fill over item
	assert_match(fetch_site_option(con, site_id, brand, pns[2], dp_ids[2]), 3, True)  # Version should be 3, on_site=True, Value taken from default

	print("Rollback to a prior version (version 2 as version 4)")
	rollback_site(con, site_id, 2)

	print_changelog_for_version(con, site_id, 4)

	print("Get the current version (version 4, but actually version 2)")
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[0]), 4, True)  # Version should be 4, on_site=True
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[1]), 4, False) # Version should be 4, on_site=False
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[2]), 4, True)  # Version should be 4, on_site=True
	assert_match(fetch_site_option(con, site_id, brand, pns[1], dp_ids[2]), 4, True)  # Version should be 4, on_site=True, Value take from default
	assert_match(fetch_site_option(con, site_id, brand, pns[2], dp_ids[2]), 4, True)  # Version should be 4, on_site=True, Value taken from default

	print("Store version 5")
	store_site_option(con, site_id, brand, pns[0], dp_ids[0], True) # Store a specific value 
	store_site_option(con, site_id, brand, pns[0], dp_ids[1], True) # Store a specific value 
	store_site_option(con, site_id, brand, pns[0], dp_ids[2], True) # Store a specific value 
	store_site_option(con, site_id, brand, pns[2], None, False)     # Store a fill on_site=false over the item ASHLEY:000112
	publish_site(con, site_id)

	print_changelog_for_version(con, site_id, 5)

	print("Get the current version (version 5)")
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[0]), 5, True)  # Version should be 5, on_site=True
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[1]), 5, True)  # Version should be 5, on_site=True
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[2]), 5, True)  # Version should be 5, on_site=True
	assert_match(fetch_site_option(con, site_id, brand, pns[1], dp_ids[2]), 5, True)  # Version should be 5, on_site=True, Value take from default
	assert_match(fetch_site_option(con, site_id, brand, pns[2], dp_ids[2]), 5, False) # Version should be 5, on_site=False, Value taken from fill

	print("Rollback to a prior version (version 3 as version 6)")
	rollback_site(con, site_id, 3)

	print_changelog_for_version(con, site_id, 6)

	print("Get the current version (version 6, but actually version 3)")
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[0]), 6, False) # Version should be 6, on_site=False
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[1]), 6, False) # Version should be 6, on_site=False
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[2]), 6, True)  # Version should be 6, on_site=True
	assert_match(fetch_site_option(con, site_id, brand, pns[1], dp_ids[2]), 6, False) # Version should be 6, on_site=False, Value take from fill over item
	assert_match(fetch_site_option(con, site_id, brand, pns[2], dp_ids[2]), 6, True)  # Version should be 6, on_site=True, Value taken from default

	print("Store version 7")
	store_site_option(con, site_id, brand, pns[0], None, False) # Store a fill on_site=false over the item ASHLEY:000112
	publish_site(con, site_id)

	print("Get the current version (version 7)")
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[0]), 7, False) # Version should be 7, on_site=False, value taken from fill over item
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[1]), 7, False) # Version should be 7, on_site=False, value taken from fill over item
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[2]), 7, False) # Version should be 7, on_site=False, value taken from fill over item
	assert_match(fetch_site_option(con, site_id, brand, pns[1], dp_ids[2]), 7, False) # Version should be 7, on_site=False, Value take from fill over item
	assert_match(fetch_site_option(con, site_id, brand, pns[2], dp_ids[2]), 7, True)  # Version should be 7, on_site=True, Value taken from default

	print("Store version 8")
	store_site_option(con, site_id, brand, pns[0], None, True) # Store a fill on_site=true over the item ASHLEY:000112
	publish_site(con, site_id)

	print_changelog_for_version(con, site_id, 8)

	print("Get the current version (version 8)")
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[0]), 8, True)  # Version should be 8, on_site=True, value taken from fill over item
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[1]), 8, True)  # Version should be 8, on_site=True, value taken from fill over item
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[2]), 8, True)  # Version should be 8, on_site=True, value taken from fill over item
	assert_match(fetch_site_option(con, site_id, brand, pns[1], dp_ids[2]), 8, False) # Version should be 8, on_site=False, Value take from fill over item
	assert_match(fetch_site_option(con, site_id, brand, pns[2], dp_ids[2]), 8, True)  # Version should be 8, on_site=True, Value taken from default
	
	print("Rollback to a prior version (version 7 as version 9)")
	rollback_site(con, site_id, 7)

	print("Get the current version (version 7, but actually version 9)")
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[0]), 9, False) # Version should be 9, on_site=False, value taken from fill over item
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[1]), 9, False) # Version should be 9, on_site=False, value taken from fill over item
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[2]), 9, False) # Version should be 9, on_site=False, value taken from fill over item
	assert_match(fetch_site_option(con, site_id, brand, pns[1], dp_ids[2]), 9, False) # Version should be 9, on_site=False, Value take from fill over item
	assert_match(fetch_site_option(con, site_id, brand, pns[2], dp_ids[2]), 9, True)  # Version should be 9, on_site=True, Value taken from default

	print("Rollback to a prior version (version 3 as version 10)")
	rollback_site(con, site_id, 3)

	print_changelog_for_version(con, site_id, 10)

	print("Get the current version (version 3 as version 10)")
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[0]), 10, False) # Version should be 10, on_site=False
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[1]), 10, False) # Version should be 10, on_site=False
	assert_match(fetch_site_option(con, site_id, brand, pns[0], dp_ids[2]), 10, True)  # Version should be 10, on_site=True
	assert_match(fetch_site_option(con, site_id, brand, pns[1], dp_ids[2]), 10, False) # Version should be 10, on_site=False, Value taken from fill over item
	assert_match(fetch_site_option(con, site_id, brand, pns[2], dp_ids[2]), 10, True)  # Version should be 10, on_site=True, Value taken from default

	# All done
	print("All tests passed!")

def main():
	# Delete the old database before creating a new one
	if os.path.exists('wf.db'):
		os.remove('wf.db')
	# Open the SQL database connection
	con = sqlite3.connect('wf.db')
	# Make sure we have the schema set up
	create_tables(con)
	# Run test cases
	run_tests(con)
	# Close the database connection
	con.close()

if __name__ == '__main__':
	main()
