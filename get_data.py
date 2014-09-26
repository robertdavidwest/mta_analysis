import pandas
import urllib2
import datetime
import bs4

# station key data
key_url = 'http://web.mta.info/developers/resources/nyct/turnstile/Remote-Booth-Station.xls'
key_data = pandas.io.excel.read_excel(key_url)

# Get urls
index_url = 'http://web.mta.info/developers/turnstile.html'
response = urllib2.urlopen(index_url)
soup = bs4.BeautifulSoup(response)
dates = soup.find('div', {'class':'span-84'})
hrefs = dates.findAll('a')
url_suffix = [x.attrs['href'] for x in hrefs]
base_url = 'http://web.mta.info/developers/'

# Get field names
field_name_source = "http://web.mta.info/developers/resources/nyct/turnstile/ts_Field%20Description.txt"
field_name_data = urllib2.urlopen(field_name_source)
field_name_data.readline()
field_name_data.readline()
field_names = field_name_data.readline().rstrip()
field_names = field_names.split(',')

# get data (Just this month)
url = base_url + url_suffix.pop(0)
big_data = pandas.read_csv(url, names=field_names)
for suffix in url_suffix :
	# get data for current month	
	if int(suffix[len(suffix)-8:len(suffix)-6]) != datetime.datetime.today().month :
		break
	
	print 'current_date = ' + suffix
	
	url = base_url + suffix
	data = pandas.read_csv(url, names=field_names)
	big_data = big_data.append(data,ignore_index=False)

big_data.reset_index(inplace=True)	
# save data in hdf5 format
key_data.to_hdf('mta_data.h5','key_data')	
big_data.to_hdf('mta_data.h5','turnstile_data')	
	
	