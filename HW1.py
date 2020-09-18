# -*- coding: utf-8 -*-
"""
Data Collection and ETL

Created on Thu Sep 17 10:52:02 2020

@author: Erika Montana
"""
import censusdata
import re
from sqlalchemy import create_engine
import sys
import ohio.ext.pandas

### editable section
state = "New York"
variables = ['B01003_001E','B02001_002E', 'B02001_003E','B02001_005E','B25024_001E',  'B25004_001E','B15003_002E','B15003_017E','B15003_022E','B15003_023E','B15003_025E']
variable_names = {'B01003_001E':'total_pop',
                  'B02001_002E':'white_pop', 
                  'B02001_003E':'black_pop',
                  'B02001_005E':'asian_pop',
                  'B25024_001E':'units_in_struc',  
                  'B25004_001E':'vacancy',
                  'B15003_002E':'edu_no_schooling',
                  'B15003_017E':'edu_hsd',
                  'B15003_022E':'edu_bs',
                  'B15003_023E':'edu_ms',
                  'B15003_025E':'edu_phd'}
table_name = "census_data"

### End editable section


##Part 1 - get data from Census
states = censusdata.geographies(censusdata.censusgeo([('state','*')]),'acs5',2018, key='db8c95da0a4bf1d0f0b43c6e66158daaef578790')
stategeo = states[state]

counties = censusdata.geographies(censusdata.censusgeo([stategeo.params()[0], ('county','*')]),'acs5',2018 , key='db8c95da0a4bf1d0f0b43c6e66158daaef578790')
countylist = list(counties.values())

#For each county in your chosen state, this will create a dataframe of all of the chosen variables down to the block group level
for county in countylist:
    params = county.params()
    if(county==countylist[0]):
        data = censusdata.download('acs5', 2018,
                             censusdata.censusgeo([params[0],params[1], ('block group', '*')]),
                             variables, key='db8c95da0a4bf1d0f0b43c6e66158daaef578790')
    else:
        data = data.append(censusdata.download('acs5', 2018,
                             censusdata.censusgeo([params[0],params[1], ('block group', '*')]),
                             variables, key='db8c95da0a4bf1d0f0b43c6e66158daaef578790'))


#Part 2 Transform data
data.rename(columns=variable_names, inplace=True) #make interpretable column names
#turn index into columns with relevent data (state, county, block group, etc)
data.reset_index(inplace=True)
data['index'] = data['index'].apply(str)
data['index'] = data['index'].apply(lambda x: re.sub(':.*','',x))
data[['block_grp','tract','county', 'state']]=data['index'].str.split(',', expand=True)
data['block_grp']=data['block_grp'].apply(lambda x: int(re.search('\d+',x).group(0)))
data['tract']=data['tract'].apply(lambda x: int(re.search('\d+',x).group(0)))
data = data.drop(['index'],axis=1)

#Part 3 Load on Database
#inifinite thanks to Rayid Ghani for helping me code/troubleshoot this part
db_string = sys.argv[1] #sysarg must be of the form "postgres://username:password@localhost:port/database"
engine = create_engine(db_string)
data.pg_copy_to(table_name, engine, if_exists='replace',index=False, schema = "emontana_test")


#additional code I'd started working on before getting office hour help
'''
#make create_table string from chosen variables
create_table = "create table census ("
for val in variable_names.values():
    create_table += val
    create_table += " integer, "
create_table += "block_grp integer, tract integer, county varchar, state varchar, primary key (state, county, tract, block_grp));" 
# read parameters from a secrets file, don't hard-code them!
db_params = 
conn = pg2.connect(
  host=db_params['host'],
  port=db_params['port'],
  dbname=db_params['dbname'],
  user=db_params['user'],
  password=db_params['password']
)
cur = conn.cursor()
cur.execute(create_table)


conn.commit()

# Close communication with the database
cur.close()
conn.close()
'''

