from src import database
import argparse

db = database.DBConnection('iot.db')
c = db.c.cursor()

def humidity():
    c.execute("""select avg(dp.ch_value)
                                 from datastreams ds join datapoints dp on dp.ds_id = ds.ds_id
                                 where dp.ch_timestamp like '%2017-04-19%'
                                 and ds.ds_deployment = 'indoor'
                                 and ds.ds_kind != 'h';""")
    return c.fetchall()

def minmax():
    c.execute("""select max(dp.ch_id), min(dp.ch_id)
                                 from datapoints dp join datastreams ds on dp.ds_id = ds.ds_id
                                 where ds.ds_deployment = 'outdoor'
                                 and ds.ds_location in (1,2,3,4,5)
                                 and date(dp.ch_timestamp) between date("2017-04-16") AND date("2017-04-19");""")
    return c.fetchall()



supported_reports = {'humidity': humidity, 'minmax': minmax}

parser = argparse.ArgumentParser()
parser.add_argument('report_type', nargs=1, choices=supported_reports)

args = parser.parse_args()

result = supported_reports[args.report_type[0]]()
print(result)



