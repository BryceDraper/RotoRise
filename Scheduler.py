from apscheduler.schedulers.blocking import BlockingScheduler
import datetime
import csv
from datetime import timedelta
import os


def mlb():
    os.system('python MLBSpider.py')


def fangraph():
    os.system('python FanGraphSpider.py')


# Reads first game start time from csv and schedules FanGraphSpider to begin running at that time.
def schedulescheduler():
    start_time = ''
    with open('schedule.csv', 'r') as input:
        reader = csv.reader(input)
        for line in reader:
            start_time = line[1]
            break
    sched1 = BlockingScheduler()
    curdate = datetime.date.today().strftime('%Y-%m-%d')
    tomorrow = datetime.date.today() + timedelta(days=1)
    tomorrow = tomorrow.strftime('%Y-%m-%d')

    sched1.add_job(fangraph, 'interval', minutes=1, jitter=5, start_time=curdate + ' ' + start_time + ':00', end_date=tomorrow + ' 00:00:01')
    sched1.start()

    # Clears Dupcatcher
    with open('dupcatcher.csv', 'w') as output:
        csv.writer(output)


# Schedules MLB Spider to run daily and schedulescheduler to run one hour later
sched = BlockingScheduler()

sched.add_job(mlb, 'interval', hours=24, id='mlb', start_date='2018-04-20 08:00:00')
sched.add_job(schedulescheduler, 'interval', hours=24, id='schedulescheduler', start_date='2018-04-20 09:00:00')

sched.start()