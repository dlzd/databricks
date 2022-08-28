import delta_sharing

# To connect to our delta sharing server, we'll need the config.share file we get
# from our activation URL.  This is a one time download!
profile = '~/Downloads/config.share'
# Create a SharingClient
client = delta_sharing.SharingClient(profile)

# List all shared tables.
print(client.list_all_tables())

table_url = f"{profile}#uc_table_share.delta_sharing_demo_recip.EDINBURGH_locations"

# Use delta sharing client to load data
df = delta_sharing.load_as_pandas(table_url)

print(df.head(10))