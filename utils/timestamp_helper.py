import time
from datetime import datetime
from datetime import date
from time import struct_time

'''
element of time:
    Year
    Month as an integer, ranging between 1 (January) and 12 (December)
    Day of the month
    Hour as an integer, ranging between 0 (12 A.M.) and 23 (11 P.M.)
    Minute
    Second
    Day of the week as an integer, ranging between 0 (Monday) and 6 (Sunday)
    Day of the year
    Daylight savings time as an integer with the following values:
        1 is daylight savings time.
        0 is standard time.
        -1 is unknown.
'''


def format_epoch_time(year: int, month: int, day: int, hr_0_23: int, min: int, second: int) -> str:
    time_tuple = (year, month, day, hr_0_23, min, second, 0, 0, -1)
    time_struct = struct_time(time_tuple)
    t = time.mktime(time_struct)
    return str(int(round(t)))


'''
Get date should (in theory) only return the date - not hr, min or sec
'''


def get_date() -> str:
    today = datetime.now()
    return format_epoch_time(today.year, today.month, today.day, today.hour, today.minute, today.second)


def get_timestamp() -> str:
    sec = str(int(round(time.mktime(time.localtime()))))
    return sec


def convert_epoch_to_str(epoch: str):
    return time.strftime('%m-%d-%Y %H:%M:%S', time.localtime(int(epoch)))

def get_now() -> str:
    today = datetime.now()
    return format_epoch_time(today.year, today.month, today.day, today.hour, today.minute, today.second)


def get_this_minute() -> str:
    today = datetime.now()
    return format_epoch_time(today.year, today.month, today.day, today.hour, today.minute, 0)


def get_n_days_ago(n: int) -> str:
    today = datetime.now()
    return format_epoch_time(today.year, today.month, today.day - n, 0, 0, 0)


def get_yesterday() -> str:
    return get_n_days_ago(1)


def get_tomorrow() -> str:
    return get_n_days_ago(-1)


def get_today() -> str:
    return get_n_days_ago(0)


def get_n_hrs_ago(n: int) -> str:
    today = datetime.now()
    return format_epoch_time(today.year, today.month, today.day, today.hour - n, 0, 0)


def get_an_hr_ago() -> str:
    return get_n_hrs_ago(1)


def get_n_min_ago(n: int) -> str:
    today = datetime.now()
    return format_epoch_time(today.year, today.month, today.day, today.hour, today.minute - n, 0)


def get_ten_min_ago() -> str:
    return get_n_min_ago(10)


def get_one_min_ago() -> str:
    return get_n_min_ago(1)



def get_n_sec_ago(n: int) -> str:
    today = datetime.now()
    return format_epoch_time(today.year, today.month, today.day, today.hour, today.minute, today.second - n)


def get_ten_sec_ago() -> str:
    return get_n_sec_ago(10)



def get_now_and_then(time_now: str, time_then_sec: int):
    today = time.localtime(int(time_now))

    return format_epoch_time(today.tm_year, today.tm_mon, today.tm_mday, today.tm_hour, today.tm_min, today.tm_sec - int(time_then_sec))



if __name__ == '__main__':

    print(get_timestamp())

    print(format_epoch_time(2020, 8, 12, 11, 20, 0))

    print(convert_epoch_to_str(get_date()))

    print(convert_epoch_to_str(get_timestamp()))

    for i in range(4):
        o = get_now_and_then(get_timestamp(), 30)

        print(convert_epoch_to_str(o))
        time.sleep(2)

    print(convert_epoch_to_str(get_yesterday()))
    print(convert_epoch_to_str(get_tomorrow()))
    print(convert_epoch_to_str(get_n_days_ago(2)))
    print(convert_epoch_to_str(get_n_hrs_ago(2)))
    print(convert_epoch_to_str(get_one_min_ago()))
    print(convert_epoch_to_str(get_ten_sec_ago()))
