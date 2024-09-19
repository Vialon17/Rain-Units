import time

from datetime import datetime

class Formater:

    datetime_format_dict = {
        'all': '%Y-%m-%d %H:%M:%S',
        'day': '%Y-%m-%d', 'd': '%Y-%m-%d',
        'year': '%Y', 'month': '%m', 'week': '%a',
    }

    datetime_timezone = {
        'utf': 0, 'china': 8,

    }

    def __init__(self) -> None:
        pass

    @classmethod
    def format_date( cls,
        target: str | datetime | float = '', 
        keyword: str = 'all', 
        time_zone = 'utf') -> str:
        '''
        Format Date To String.

        ------
        `String Target` -- Standard Format: `%a, %d %b %Y %H:%M:%S +0000`
        [click here show doc](https://docs.python.org/3/library/time.html#time.strptime)

        Default Return UTF Time.
        '''
        target = target if target != '' else time.time()

        if isinstance(target, datetime):
            target = target.timestamp()
        elif isinstance(target, str):
            target = time.mktime(time.strptime(target))

        target = target + cls.datetime_timezone.get(time_zone, 0) * 3600

        keyword = keyword if keyword not in cls.datetime_format_dict \
            else cls.datetime_format_dict[keyword]

        return time.strftime(keyword, time.gmtime(target))


if __name__ == '__main__':
    print(Formater.format_date(keyword = '%Z'))
    