"""
Generates statistics for the tables in the paper.
"""
from util import get_exp_name, load_results, get_dataset_path, project_list
from data_manager import DataManager
from graph import HistoryGraph


def leaving_developers_table():
    """
    Generates the number of leaving developers for each project.
    """

    print("Absence Limit ", ("{:<15}" * len(project_list)).format(*project_list))
    for absence_limit in [180, 365]:
        print("{:<15}".format(absence_limit), end="")
        for project_name in project_list:
            date_to_results = load_results(
                get_exp_name(project_name, sws=absence_limit)
            )
            leaving_developers = [
                rep
                for results in date_to_results.values()
                for rep in results["replacements"]
            ]
            print("{:<15}".format(len(leaving_developers)), end="")
        print()
    print("\n")


def dataset_details_after_preprocess():
    """
    Generates the statistics after preprocessing for each project.
    """

    print("Project        # CS      # CS > 10        # CS > 50")
    for project_name in project_list:
        dataset_path = get_dataset_path(project_name)
        dm = DataManager(dataset_path, None)
        nums_cs = 0
        nums_cs_10 = 0
        nums_cs_50 = 0
        add_or_modify = set(["MODIFY", "ADD"])
        for date, change_sets in dm._date_to_change_sets.items():
            for cs in change_sets:
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
            "{:<15}{}\t{:>5}({:.2f})\t{:>5}({:.2f})".format(
                project_name,
                nums_cs,
                nums_cs_10,
                100 * nums_cs_10 / nums_cs,
                nums_cs_50,
                100 * nums_cs_50 / nums_cs,
            )
        )
    print()


def average_num_developers():
    """
    Generates the average number of developers in the graph for each project.
    """

    avg_dev_nums = []
    all_dev_nums = []
    for project_name in project_list:
        dataset_path = get_dataset_path(project_name)
        G = HistoryGraph(dataset_path)
        dev_nums = []
        all_devs = set()
        while True:
            devs = G.get_developers()
            all_devs.update(devs)
            dev_nums.append(len(devs))
            if not G.forward_graph_one_day():
                break
        avg_dev_nums.append(sum(dev_nums) / len(dev_nums))
        all_dev_nums.append(len(all_devs))

    print(("{:<15}" * len(project_list)).format(*project_list))
    print(("{:<15.2f}" * len(avg_dev_nums)).format(*avg_dev_nums), end="")
    print()
    print(("{:<15}" * len(all_dev_nums)).format(*all_dev_nums), end="")
    print("\n")


if __name__ == "__main__":
    leaving_developers_table()
    dataset_details_after_preprocess()
    average_num_developers()
