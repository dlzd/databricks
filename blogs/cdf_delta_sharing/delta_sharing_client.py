import delta_sharing as ds

# To connect to our delta sharing server, we'll need the config.share file we get
# from our activation URL.  This is a one time download!
profile = '~/Downloads/config.share'
# Create a SharingClient
client = ds.SharingClient(profile)

# List all shared tables.
print(client.list_all_tables())

table_url = f"{profile}#ds_cdf_table_share.cdf_ds_external.Company2"

# Use delta sharing client to load data
df = ds.load_as_pandas(table_url)

print(df.loc[(df['RECID'] <= 200100)].sort_values(by=['RECID']))