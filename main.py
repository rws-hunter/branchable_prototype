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
	# Create a table to store item option data and fills
	con.execute('''
	CREATE TABLE IF NOT EXISTS site_options(
		-- Lookup columns
		-- ROWID is automatic in SQLite
		site_id integer not null,    -- Not fillable
		version_id integer not null, -- Not fillable
		brand string not null,       -- Fill value = *
		pn string not null,          -- Fill value = *
		dp_id integer not null,      -- Fill value = 0
		-- Data columns, these need to be nullable for our fill scheme to work
		-- SQLite doesn't have boolean types
		on_site integer default true);
	''')
	# Create a unique index over the lookup values. 
	# This allows the insert-or-replace commands to work and ensure fills don't overlap.
	con.execute('''
	CREATE UNIQUE INDEX IF NOT EXISTS idx_site_options ON site_options(site_id, version_id, brand, pn, dp_id);
	''')
	con.commit()

# Store a site option or fill into the database
def store_site_option(con, site_id, brand, pn, dp_id, on_site):
	params = { 
		"site_id": site_id, 
		"brand": brand, "pn": pn, "dp_id": dp_id, 
		"on_site": on_site
	}
	con.execute('''
	INSERT OR REPLACE INTO site_options(site_id, brand, pn, dp_id, on_site, version_id) VALUES (
		:site_id, :brand, :pn, :dp_id, 
		:on_site,
		(SELECT branch_version_id FROM sites where site_id=:site_id)
	); 
	''', params)
	con.commit()

# Fetch a single site option from the database
def fetch_site_option(con, site_id, brand, pn, dp_id):
	params = { "site_id": site_id, "brand": brand, "pn": pn, "dp_id": dp_id }
	query = con.execute('''
	SELECT * FROM site_options WHERE 
		site_id=:site_id AND brand=:brand AND pn=:pn AND dp_id=:dp_id AND
		version_id <= (SELECT trunk_version_id FROM sites WHERE site_id=:site_id) 
		ORDER BY version_id DESC
		LIMIT 1;
	''', params);
	# Fetch the result and convert
	result = query.fetchone()
	if result:
		return SiteOption(result[0], result[1], result[2], result[3], result[4], (result[5] == 1))
	return None

def create_site(con, site_id):
	params = { "site_id": site_id }
	con.execute('''
	INSERT OR REPLACE INTO sites(site_id, trunk_version_id, branch_version_id) VALUES (:site_id, 0, 1); 
	''', params)
	con.commit()

def publish_site(con, site_id):
	params = { "site_id": site_id }
	con.execute('''
	UPDATE sites SET 
		trunk_version_id=branch_version_id, 
		branch_version_id=branch_version_id+1 
		WHERE site_id=:site_id;
	''', params)
	con.commit()

def rollback_site(con, site_id, version_id):
	params = { "site_id": site_id, "version_id": version_id }
	# Clear out pending changes
	con.execute('''
	DELETE FROM site_options WHERE version_id=(SELECT branch_version_id FROM sites WHERE site_id=:site_id);
	''', params)
	# Copy the rows that existed at the selected version, with the current branch version
	con.execute('''
	INSERT INTO site_options(version_id, site_id, brand, pn, dp_id, on_site) 
	SELECT (SELECT branch_version_id FROM sites where site_id=:site_id), a.site_id, a.brand, a.pn, a.dp_id, a.on_site 
	FROM site_options a
	INNER JOIN (
		SELECT MAX(version_id) as version_id, site_id, brand, pn, dp_id 
		FROM site_options 
		WHERE version_id<=:version_id AND site_id=:site_id
		GROUP BY brand, pn, dp_id
	) b
	ON a.brand=b.brand AND a.pn=b.pn AND a.dp_id=b.dp_id AND a.version_id=b.version_id;
	''', params)
	con.commit()
	# Publish branch
	publish_site(con, site_id)

def assert_match(site_option, version_id, on_site):
	assert(site_option.version_id == version_id and site_option.on_site == on_site)

def main():
	# Delete the old database before creating a new one
	if os.path.exists('wf.db'):
		os.remove('wf.db')
	# Open the SQL database connection
	con = sqlite3.connect('wf.db')
	# Make sure we have the schema set up
	create_tables(con)

	print("Create the site")
	create_site(con, 8080)

	print("Store version 1")
	store_site_option(con, 8080, "ASHLEY", "000111", 1000001, True)
	store_site_option(con, 8080, "ASHLEY", "000111", 1000002, True)
	store_site_option(con, 8080, "ASHLEY", "000111", 1000003, True)
	publish_site(con, 8080)

	print("Store version 2")
	store_site_option(con, 8080, "ASHLEY", "000111", 1000002, False)
	publish_site(con, 8080)

	print("Store version 3")
	store_site_option(con, 8080, "ASHLEY", "000111", 1000001, False)
	publish_site(con, 8080)

	print("Get the current version (version 3)")
	assert_match(fetch_site_option(con, 8080, "ASHLEY", "000111", 1000001), 3, False) # Version should be 3, on_site=False
	assert_match(fetch_site_option(con, 8080, "ASHLEY", "000111", 1000002), 2, False) # Version should be 2, on_site=False
	assert_match(fetch_site_option(con, 8080, "ASHLEY", "000111", 1000003), 1, True) # Version should be 1, on_site=True

	print("Rollback to a prior version (version 2)")
	rollback_site(con, 8080, 2)

	print("Get the current version (version 4, but actually version 2)")
	assert_match(fetch_site_option(con, 8080, "ASHLEY", "000111", 1000001), 4, True) # Version should be 4, on_site=True
	assert_match(fetch_site_option(con, 8080, "ASHLEY", "000111", 1000002), 4, False) # Version should be 4, on_site=False
	assert_match(fetch_site_option(con, 8080, "ASHLEY", "000111", 1000003), 4, True) # Version should be 4, on_site=True

	print("Store version 5")
	store_site_option(con, 8080, "ASHLEY", "000111", 1000001, True)
	store_site_option(con, 8080, "ASHLEY", "000111", 1000002, True)
	store_site_option(con, 8080, "ASHLEY", "000111", 1000003, True)
	publish_site(con, 8080)

	print("Get the current version (version 5)")
	assert_match(fetch_site_option(con, 8080, "ASHLEY", "000111", 1000001), 5, True) # Version should be 5, on_site=True
	assert_match(fetch_site_option(con, 8080, "ASHLEY", "000111", 1000002), 5, True) # Version should be 5, on_site=True
	assert_match(fetch_site_option(con, 8080, "ASHLEY", "000111", 1000003), 5, True) # Version should be 5, on_site=True

	print("Rollback to a prior version (version 3)")
	rollback_site(con, 8080, 3)

	print("Get the current version (version 6, but actually version 3)")
	assert_match(fetch_site_option(con, 8080, "ASHLEY", "000111", 1000001), 6, False) # Version should be 6, on_site=False
	assert_match(fetch_site_option(con, 8080, "ASHLEY", "000111", 1000002), 6, False) # Version should be 6, on_site=False
	assert_match(fetch_site_option(con, 8080, "ASHLEY", "000111", 1000003), 6, True) # Version should be 6, on_site=True

	# All done
	print("All tests passed!")
	# Close the database connection
	con.close()

if __name__ == '__main__':
	main()
