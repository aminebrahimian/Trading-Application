import winsound
import time
from datetime import datetime
import schedule

FirstTimeRun = True

def job():
    print(datetime.now())
    winsound.Beep(700, 100)


while FirstTimeRun:
    datetime_object = datetime.now()
    if not datetime_object.minute % 5 and datetime_object.second == 0:
        print('********************Getting Cyclic Data Starts********************')
        schedule.every(5).minutes.at(":00").do(job)  # Task Scheduler
        print("Run time: ", datetime.now())
        job()
        FirstTimeRun = False
        break
    time.sleep(1)
while True:
    schedule.run_pending()
    time.sleep(1)