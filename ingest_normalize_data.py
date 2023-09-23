import pandas as pd

# Every row lists one diagnosis given to a member on a certain day.
claims = pd.read_csv(filepath_or_buffer='https://ohtakehometest356.nyc3.digitaloceanspaces.com/claim_lines.csv', parse_dates=['date_svc'],
                     dtype={'record_id': 'int32',
                            'member_id': 'object',
                            'diag1': 'object'})
claims['diag1'] = claims['diag1'].str.replace('.', '')

# Diagnosis codes found on claim lines are mapped to higher level clinical categories.
# Not all diagnosis codes have a matching CCS code.
ccs = pd.read_csv(filepath_or_buffer='https://ohtakehometest356.nyc3.digitaloceanspaces.com/ccs.csv')

# to join diag codes to claim to make the more useful
claimsjoincodes = claims.merge(ccs, left_on='diag1', right_on='diag', how='left', suffixes=('_claims', '_codes'))

# Every entry in this data set corresponds to a drug prescription filled by a member.
prescriptions = pd.read_csv(filepath_or_buffer='https://ohtakehometest356.nyc3.digitaloceanspaces.com/prescription_drugs.csv', parse_dates=['date_svc'])

# to join all 3 files for member profile table
claimsjoinprescriptions = claimsjoincodes.merge(prescriptions,
                                                left_on='member_id',
                                                right_on='member_id',
                                                how='outer', suffixes=('_claims', '_prescriptions')).reset_index()

memberprofile = claimsjoinprescriptions.groupby('member_id').agg(
    earliest_claim=('date_svc_claims', 'min'),
    latest_claim_date=('date_svc_claims', 'max'),
    earliest_prescription=('date_svc_prescriptions', 'min'),
    latest_prescription=('date_svc_prescriptions', 'max'),
    number_of_claims=('record_id_claims', 'count'),
    number_of_prescriptions=('ndc', 'nunique')
).reset_index()

# this is the part to put the data in the postgres instance
import os
from sqlalchemy import create_engine

# Read environment variables
host = os.environ['host']
password = os.environ['password']
user = os.environ['user']
dbname = os.environ['database']
port = '25060'

# Construct DATABASE_URL
DATABASE_URL = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
engine = create_engine(DATABASE_URL)

# write the tables to the postgres instance
claimsjoincodes.to_sql('claims', engine, if_exists='replace', index=False, chunksize=5000)
prescriptions.to_sql('prescriptions', engine, if_exists='replace', index=False, chunksize=5000)
memberprofile.to_sql('member_profile', engine, if_exists='replace', index=False, chunksize=5000)

engine.dispose()