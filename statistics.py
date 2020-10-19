from replacement_validation import find_leaving_developers
from data_manager import DataManager
from graph import HistoryGraph


def leaving_developers_table():
    pnames = ["hadoop", "hive", "pig", "hbase", "derby", "zookeeper"]
    print("Absence Limit ", ("{:<15}" * len(pnames)).format(*pnames))
    for absence_limit in [180, 365]:
        print("{:<15}".format(absence_limit), end="")
        for pname in pnames:
            dataset_path = "data/{}_change_sets.json".format(pname)
            date_to_leaving_developers = find_leaving_developers(
                dataset_path, absence_limit
            )
            leaving_developers = [
                dev for devs in date_to_leaving_developers.values() for dev in devs
            ]
            print("{:<15}".format(len(leaving_developers)), end="")
        print()
    print("\n")


def dataset_details_after_preprocess():
    print("Project        # CS      # CS > 10        # CS > 50")
    for pname in ["hadoop", "hive", "pig", "hbase", "derby", "zookeeper"]:
        dataset_path = "data/{}_change_sets.json".format(pname)
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
                pname,
                nums_cs,
                nums_cs_10,
                100 * nums_cs_10 / nums_cs,
                nums_cs_50,
                100 * nums_cs_50 / nums_cs,
            )
        )
    print()


def average_num_developers():
    pnames = ["hadoop", "hive", "pig", "hbase", "derby", "zookeeper"]
    avg_dev_nums = []
    all_dev_nums = []
    for pname in pnames:
        dataset_path = "data/{}_change_sets.json".format(pname)
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

    print(("{:<15}" * len(pnames)).format(*pnames))
    print(("{:<15.2f}" * len(avg_dev_nums)).format(*avg_dev_nums), end="")
    print()
    print(("{:<15}" * len(all_dev_nums)).format(*all_dev_nums), end="")
    print("\n")


if __name__ == "__main__":
    leaving_developers_table()
    dataset_details_after_preprocess()
    average_num_developers()
