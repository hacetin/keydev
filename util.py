"""
Includes a group of functions used by different scripts.
"""

from datetime import datetime, time, timedelta
from copy import deepcopy
import sqlite3
import unittest
import pickle


# These parameters are used wherever dataset they are needed in the source code.
# So, any change here will affect everywhere (experiments, validation etc.).
project_list = ["hadoop", "hive", "pig", "hbase", "derby", "zookeeper"]
sws_list = [180, 365]

# This is the default date format in sqlite3 databases. Every part of the code uses
# the same date format
_default_date_format = "%Y-%m-%dT%H:%M:%SZ"


def get_dataset_path(project_name):
    """
    Specify the dataset path. This method is used wherever dataset path is needed
    in the source code. So, if naming is changed here, it will affect everywhere.

    Parameters
    ----------
    project_name (str):
        Name of the project to read change sets.
    """
    return "data/{}_change_sets.json".format(project_name)


def get_exp_name(project_name, nl=10, nfl=50, sws=365):
    """
    Specify the experiment naming. This method is used wherever experiment name
    is needed in the source code. So, if the experimment naming is changed here,
    it will affect everywhere.

    Parameters
    ----------
    project_name (str):
        Name of the project to read change sets.

    dl (float) (default = 10):
        Distance limit. Limit for DFS of reachable files.

    nfl (int) (default = 50):
        Number of files limit. Limit for handling large files.

    sws (int) (default = 365):
        Sliding_window_size, in other words number of days to include the graph.
    """
    return "{}_dl{}_nfl{}_sws{}".format(project_name, nl, nfl, sws)


def str_to_date(date_str, date_format=_default_date_format):
    """
    Convert time string to datetime object.

    Parameters
    ----------
    date_str (str):
        Date string in the appropriate format.

    date_format (str) (default = "%Y-%m-%dT%H:%M:%SZ"):
        Date format (proper for datetime module) of the given string.

    Return
    ------
    datetime.datetime:
        Corresponding datetime object of the given date_str.
    """
    return datetime.strptime(date_str, date_format)


def date_to_str(date, date_format=_default_date_format):
    """
    Convert the given datetime object to string.

    Parameters
    ----------
    date (datetime.datetime):
        Given date.

    date_format (str) (default = "%Y-%m-%dT%H:%M:%SZ"):
        Date format string (proper for datetime module) to convert the given date
        into its string representation.

    Return
    ------
    str:
        Date string in the given format.
    """
    return date.strftime(date_format)


def max_of_day(date):
    """
    Create a copy of the given date with the last hour, the last minute,
    the last second and the last milisecond of the day.

    Parameters
    ----------
    date (datetime.datetime):
        Given date.

    Return
    ------
    datetime.datetime:
        A copy of the given date with the last hour, the last minute,
        the last second and the last milisecond of the day.
    """
    return datetime.combine(date, time.max)


def highest_k(d, k):
    """
    Get the list of the keys (of `d`) which have the highest `k` values.

    Parameters
    ----------
    d (dict):
        Dictionary with comparable values.

    k (int):
        Number representing how many keys will be returned. If the number of items
        in `d` is more than `k`, the function return all values.

    Returns
    -------
    list:
        Top `k` keys (of `d`) that have highest values.
    """
    return list(sort_dict(d, by_value=True, reverse=True))[:k]


def execute_db_query(dbpath, query):
    """
    Execute the SQL `query` in the sqlite3 database in `dbpath`.

    Parameters
    ----------
    dbpath (str):
        Path to the database.

    query (str):
        SQL query to execute in the sqlite3 database in `dbpath`.

    Return
    ------
    list:
        All fetched results from the sqlite3 database in `dbpath`.
    """
    # Connect to the db
    source = sqlite3.connect(dbpath)

    # Backup the db into the memory.
    conn = sqlite3.connect(":memory:")
    source.backup(conn)

    # Get the cursor and execute the query
    cur = conn.cursor()
    cur.execute(query)

    return cur.fetchall()


def sort_dict(d, by_value=False, reverse=False):
    """
    Sort the given dictionary.
    If `by_value=True`, sort by values. Othwerwise sort by keys.
    If `reverse=True`, sort in descending order. Otherwise sort in ascending order.

    Parameters
    ----------
    d (dict):
        Any dictionary.

    by_value (bool) (default = False):
        If True, sort by values. Othwerwise sort by keys.

    reverse (bool) (default = False):
        If True, sort in descending order. If False, sort in ascending order.

    Returns
    -------
    dict:
        Sorted copy of the given dictionary.
    """
    if by_value:
        return {k: d[k] for k in sorted(d, key=lambda x: d[x], reverse=reverse)}
    else:
        return {k: d[k] for k in sorted(d, reverse=reverse)}


def print_log(info, log_path, mode="a"):
    """
    Print given info along with time string to the file in the `log_path`.

    Parameters
    ----------
    info (str):
        Text to log.

    log_path (str):
        Path to log file.

    mode (str) (default = 'a'):
        'a' (append) or 'w' (write). 'a' appends to the existing log file.
        'w' overwrites the existing log file.
    """
    assert mode in ["a", "w"], "Log mode can be 'a' (append) or 'w' (write)"

    info = "{}: {}".format(datetime.today(), info)
    with open(log_path, mode, encoding="utf8") as f:
        f.write(info)


def load_results(exp_name):
    """
    Load the results of the pre-runned experiments from pickle files.

    Parameters
    ----------
    exp_name (str):
        Name of the experiment used while saving results

    Returns:
    dict:
       Results read from the corresponding pickle file.
    """
    path = "results/{}.pkl".format(exp_name)
    with open(path, "rb") as f:
        return pickle.load(f)


def dump_results(exp_name, date_to_results):
    """
    Dump the given results into a pickle file. Overwrite if a file with the given
    name already exists.

    Parameters
    ----------
    exp_name (str):
        Name of the experiment used while saving results.

    date_to_results (dict):
        Mapping from date to results.
    """
    path = "results/{}.pkl".format(exp_name)
    with open(path, "wb") as f:
        pickle.dump(date_to_results, f)


def find_leaving_developers(G):
    """
    Find the leaving developers in the given graph without changing the graph.
    If any existing developer disappear from the graph when sliding the window,
    we consider s/he left the project at his/her last contribution date.

    Parameters
    ----------
    G (graph.HistoryGraph): Graph to find leaving developers. It will not be changed.

    Returns
    -------
    dict:
        Mapping from date to developers left that date (last contribution day).
        It doesn't include dates with no leaving developers.
    """
    G_ = deepcopy(G)
    absence_limit = G_._sliding_window_size
    date_to_leaving_developers = {}
    prev_developers = set()
    while True:
        developers = set(G_.get_developers())
        leaving_developers = prev_developers.difference(developers)
        prev_developers = developers
        if leaving_developers:
            date = G_.get_last_included_date()
            leaving_day = date - timedelta(days=absence_limit)
            date_to_leaving_developers[leaving_day] = leaving_developers

        if not G_.forward_graph_one_day():
            break

    return date_to_leaving_developers


class TestUtil(unittest.TestCase):
    def test_sort_dict(self):
        d = {1: 10, 2: 11, 4: 8, 3: 9}

        assert sort_dict(d, by_value=False, reverse=False) == {
            1: 10,
            2: 11,
            3: 9,
            4: 8,
        }, "Testing 'sort_dict' with 'by_value=False' and 'reverse=False' failed."

        assert sort_dict(d, by_value=True, reverse=False) == {
            4: 8,
            3: 9,
            1: 10,
            2: 11,
        }, "Testing 'sort_dict' with 'by_value=True' and 'reverse=False' failed."

        assert sort_dict(d, by_value=False, reverse=True) == {
            4: 8,
            3: 9,
            2: 11,
            1: 10,
        }, "Testing 'sort_dict' with 'by_value=False' and 'reverse=True' failed."

        assert sort_dict(d, by_value=True, reverse=True) == {
            2: 11,
            1: 10,
            3: 9,
            4: 8,
        }, "Testing 'sort_dict' with 'by_value=True' and 'reverse=True' failed."

    def test_highest_k(self):
        d = {1: 10, 2: 11, 4: 8, 3: 9}
        assert highest_k(d, 2) == [
            2,
            1,
        ], "Testing highest_k failed. Order of the returned list may be wrong."


if __name__ == "__main__":
    unittest.main()
