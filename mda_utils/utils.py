'''
    Python utils file to provide utility functions to invoking libs
'''
from datetime import datetime, date


def gen_time_frame_list(start_date, end_date):
    """
    Method to generate time frame list between two given date based on the depth param
    :param start_date: start date object
    :param end_date: end date object
    :param depth: year/month/day based on the depth will generate the output
    return: list of date strings
    """
    date_strings = {'year':'%Y', 'month':'%Y%m', 'day':'%Y%m%d'}
    for depth, date_string in date_strings.items():
        try:
            start_date = datetime.strptime(start_date, date_string)
            end_date = datetime.strptime(end_date, date_string)
            break
        except ValueError:
            pass

    if start_date > end_date:
        raise ValueError(f"Start date {start_date} is not before end date {end_date}")
    
    year = start_date.year
    month = start_date.month
    day = start_date.day

    def depth_month(month, year):
        if month == 12:
            month = 1
            year += 1
        else:
            month += 1
        return month, year

    while (year, month, day) <= (end_date.year, end_date.month, end_date.day):
        if depth == 'year':
            yield date(year, month, day).strftime(date_strings[depth])
            year += 1
        
        elif depth == 'month':
            yield date(year, month, day).strftime(date_strings[depth])
            month, year = depth_month(month, year)

        elif depth == 'day':
            try:
                yield date(year, month, day).strftime(date_strings[depth])
                day += 1
            except ValueError:
                day = 1
                month, year = depth_month(month, year)
