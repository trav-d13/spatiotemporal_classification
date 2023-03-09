import sys
from datetime import datetime
from time import sleep

request_limit = 10000
"""int: The maximum daily request limit for Open-Meteo API's for non-commercial use"""
request_parameter_limit = 5
"""int: The number of parameters that each request can contain. 
For example the elevation API can contain 100 locations"""
collection_duration = 1
"""int: The duration in minutes that collection process should occur over"""
collection_target = 100
"""int: The number of target answers to be requested"""
no_requests = 0
"""int: This value will be updated on  method call to record the number of requests required"""
request_no = 0
"""int: The current number of request processed"""


def enforce_request_interval():
    global request_no
    interval = calculate_request_interval()
    sleep(interval)
    request_no = request_no + 1
    progress_bar(interval)


def enforce_request_interval_batching(batch_size, batch_limit):
    global request_no
    interval = calculate_request_interval_batching(batch_size, batch_limit)
    sleep(interval)
    request_no = request_no + 1
    progress_bar(interval)


def calculate_request_interval():
    """ Method calculates the interval time between requests.

    Each request will maximize the request parameter limit.

    """
    global no_requests
    no_requests = collection_target / request_parameter_limit
    interval = 60 * (collection_duration / no_requests)

    assert interval >= 1, "Minimum API interval limit exceeded. Cannot be less than 1 request per second"
    assert no_requests <= request_limit, "Maximum request limit exceeded. Number of daily requests less than 10000"
    return interval


def calculate_request_interval_batching(batch_size, batch_limit):
    global collection_target
    collection_target = batch_size * batch_limit

    return calculate_request_interval()


def progress_bar(interval):
    progress_bar_length = 100
    percentage_complete = (request_no / no_requests)
    filled = int(progress_bar_length * percentage_complete)
    running_time = (interval * request_no) / 60

    bar = '=' * filled + '-' * (progress_bar_length - filled)
    percentage_display = round(100 * percentage_complete, 1)
    sys.stdout.write('\r[%s] %s%s ... running: %s ... interval: %s' % (bar, percentage_display, '%', running_time, interval))
    sys.stdout.flush()

