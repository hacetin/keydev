"""
Generates statistics for the tables in the paper.
"""
from util import (
    execute_db_query,
    get_dataset_path,
    find_leaving_developers,
    project_list,
    sws_list,
)
from data_manager import DataManager
from graph import HistoryGraph


def leaving_developers_table():
    """
    Generate the number of leaving developers for each project.
    """

    print("\n*** Number of Leaving Developers ***\n")
    print("Absence Limit ", ("{:<15}" * len(project_list)).format(*project_list))
    for absence_limit in sws_list:
        print("{:<15}".format(absence_limit), end="")
        for project_name in project_list:
            dataset_path = get_dataset_path(project_name)
            G = HistoryGraph(dataset_path, sliding_window_size=absence_limit)
            date_to_leaving_developers = find_leaving_developers(G)
            leaving_developers = [
                dev for devs in date_to_leaving_developers.values() for dev in devs
            ]
            print("{:<15}".format(len(leaving_developers)), end="")
        print()
    print()


def number_of_developers_before_preprocessing():
    """
    Generate the number of all distint developers for each project before preprocessing.
    For example, author name correction not applied yet.
    """
    print("\n*** Number of Developers Before Preprocessing ***\n")
    print(("{:<12}" * len(project_list)).format(*project_list))
    for project_name in project_list:
        num_devs = execute_db_query(
            "data/{}.sqlite3".format(project_name),
            "SELECT count(DISTINCT author) FROM change_set",
        )[0][0]
        print("{:<12}".format(num_devs), end="")
    print("\n")


def dataset_details_after_preprocess():
    """
    Generate statistics after dataset preprocessing for each project.
    """

    print("\n*** Dataset Details After Preprocessing ***\n")
    print("Project        # Developers     # CS     # CS > 10        # CS > 50")
    for project_name in project_list:
        dataset_path = get_dataset_path(project_name)
        dm = DataManager(dataset_path, None)
        developers = set()
        nums_cs = 0
        nums_cs_10 = 0
        nums_cs_50 = 0
        add_or_modify = set(["MODIFY", "ADD"])
        for date, change_sets in dm._date_to_change_sets.items():
            for cs in change_sets:
                developers.add(cs.author)
                files_add_modify = []
                for cc in cs.code_changes:
                    if cc.change_type in add_or_modify:
                        files_add_modify.append(cc.file_path)

                # Increase counters
                nums_cs += 1
                if len(files_add_modify) > 10:
                    nums_cs_10 += 1
                if len(files_add_modify) > 50:
                    nums_cs_50 += 1

        print(
            "{:<15}{}\t\t{}\t{:>5}({:.2f})\t{:>5}({:.2f})".format(
                project_name,
                len(developers),
                nums_cs,
                nums_cs_10,
                100 * nums_cs_10 / nums_cs,
                nums_cs_50,
                100 * nums_cs_50 / nums_cs,
            )
        )
    print()


def average_number_of_developers():
    """
    Generate the average number of developers in the graph for each project.
    """

    print("\n*** Average Number of Developer ***\n")
    print("{:<15}".format("SWS"), end="")
    print(("{:<15}" * len(project_list)).format(*project_list))
    for sws in sws_list:
        print("{:<15}".format(sws), end="")
        for project_name in project_list:
            dataset_path = get_dataset_path(project_name)
            G = HistoryGraph(dataset_path, sliding_window_size=sws)
            dev_nums = []
            while True:
                devs = G.get_developers()
                dev_nums.append(len(devs))
                if not G.forward_graph_one_day():
                    break
            avg_dev_num = sum(dev_nums) / len(dev_nums)
            print("{:<15.2f}".format(avg_dev_num), end="")
        print()
    print()


if __name__ == "__main__":
    leaving_developers_table()
    number_of_developers_before_preprocessing()
    dataset_details_after_preprocess()
    average_number_of_developers()
