import pandas
import numpy
century = 2000 # mta dates stored as 2 digit year

mta_data = pandas.HDFStore('mta_data.h5')
turnstile_data = mta_data.turnstile_data
key_data = mta_data.key_data

####################################################################################
# append all date/times into 1 column of data - 8 times in a single row from raw data
# Also rename ENTRIES and EXITS, CUMENTRIES and CUMEXITS
turn_new = turnstile_data[['C/A','UNIT','SCP','DATE1','TIME1','DESC1','ENTRIES1','EXITS1']] 
turn_new.columns = ['C/A','UNIT','SCP','DATE','TIME','DESC','CUMENTRIES','CUMEXITS']
for i in [2,3,4,5,6,7,8] :
	turn_i = turnstile_data[['C/A','UNIT','SCP','DATE'+str(i),'TIME'+str(i),'DESC'+str(i),'ENTRIES'+str(i),'EXITS'+str(i)]] 	
	turn_i.columns = ['C/A','UNIT','SCP','DATE','TIME','DESC','CUMENTRIES','CUMEXITS']
	turn_new = turn_new.append(turn_i,ignore_index=True)

####################################################################################
# drop NAs
turn_new = turn_new[~turn_new['DATE'].isnull()]

####################################################################################
# Create datetime variable
year = [int(x[len(x)-2:]) + century for x in turn_new['DATE']] 
month = [int(x[:2]) for x in turn_new['DATE']]
day = [int(x[3:5]) for x in turn_new['DATE']]
hour = [int(x[:2]) for x in turn_new['TIME']] 
minute = [int(x[3:5]) for x in turn_new['TIME']] 
second = [int(x[len(x)-2:]) for x in turn_new['TIME']] 

turn_new['DATETIME'] = [pandas.datetime(year=year[i],month=month[i],day=day[i],hour=hour[i],minute=minute[i],second=second[i]) for i in xrange(len(year))]

####################################################################################
# Sort
turn_new = turn_new.sort(columns=['C/A','UNIT','SCP','DATETIME'])
turn_new.reset_index(inplace=True)

####################################################################################
# Define unique turnstyle IDs by combination of C/A UNIT and SCP
unique_turnstyles = turn_new[['C/A','UNIT','SCP']]
unique_turnstyles.drop_duplicates(inplace=True)
unique_turnstyles['turnstyle_id'] = xrange(len(unique_turnstyles))

temp = turn_new.merge(unique_turnstyles, how='left', on=['C/A','UNIT','SCP'])
#C/A = Control Area (A002)
#UNIT = Remote Unit for a station (R051)
#SCP = Subunit Channel Position represents an specific address for a device (02-00-00)

clean_data = temp[['turnstyle_id','DATETIME','DESC','CUMENTRIES','CUMEXITS']]

####################################################################################
### calculate the number of ENTRIES and EXITS between timestamps for each turnstyle
clean_data['ENTRIES'] = numpy.nan
clean_data['EXITS'] = numpy.nan

for i in unique_turnstyles.turnstyle_id :
	idx = clean_data['turnstyle_id']==i
	# ENTRIES
	clean_data['ENTRIES'][idx] = clean_data['CUMENTRIES'][idx] - clean_data['CUMENTRIES'][idx].shift(1)	
	# EXITS
	clean_data['EXITS'][idx] = clean_data['CUMEXITS'][idx] - clean_data['CUMEXITS'][idx].shift(1)

# save data in hdf5 format
clean_data.to_hdf('mta_data.h5','clean_data')	



