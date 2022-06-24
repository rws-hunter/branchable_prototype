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
		where site_id=:site_id;
	''', params)
	con.commit()

def rollback_site(con, site_id, version_id):
	pass

def main():
	# Delete the old database before creating a new one
	if os.path.exists('wf.db'):
		os.remove('wf.db')
	# Open the SQL database connection
	con = sqlite3.connect('wf.db')
	# Make sure we have the schema set up
	create_tables(con)

	# Create the site
	create_site(con, 8080)

	# Store version 1
	store_site_option(con, 8080, "ASHLEY", "000111", 1000001, True)
	store_site_option(con, 8080, "ASHLEY", "000111", 1000002, True)
	store_site_option(con, 8080, "ASHLEY", "000111", 1000003, True)
	publish_site(con, 8080)

	# Store version 2
	store_site_option(con, 8080, "ASHLEY", "000111", 1000002, False)
	publish_site(con, 8080)

	# Store version 3
	store_site_option(con, 8080, "ASHLEY", "000111", 1000001, False)
	publish_site(con, 8080)

	# Get the current version
	print(fetch_site_option(con, 8080, "ASHLEY", "000111", 1000001)) # Version should be 3
	print(fetch_site_option(con, 8080, "ASHLEY", "000111", 1000002)) # Version should be 2
	print(fetch_site_option(con, 8080, "ASHLEY", "000111", 1000003)) # Version should be 1

	# Close the database connection
	con.close()

if __name__ == '__main__':
	main()
