from graph import HistoryGraph
from data_manager import DataManager
from datetime import timedelta
import random
from joblib import Parallel, delayed
from util import load_results

random.seed(2020)


@delayed
def validation(project_name, sliding_window_size, check_days, max_k, random_val):
    """
    Perform validation with given parameters.

    Parameters
    ----------
    project_name (str):
        Name of the project to read change sets
    sliding_window_size (str):
        Number of days to include the graph
    check_days (list)
        List of integers to check if recomendations are true or false
    max_k (int):
        Maximum k for topk and mrr calculation. When max_k is 3, top1, top2 and top3
        will be calculated, and the ranks in MRR calculations can 1, 2 and 3.
    random_val (bool):
        If True, `max_k` replacements will be selected randomly.

    Returns
    -------
    list:
        First item of the list is the name of the experiment. Second and the following
        items will include accuracy and MRR for each check day. For example, returns
        [pig_sws365, (7, {top1:.5, top2:.7, mrr:.6}), (30, {top1:.6, top2:.9, mrr:.7})].
    """

    dataset_path = "data/{}_change_sets.json".format(project_name)

    dm = DataManager(dataset_path, None)
    G = HistoryGraph(dataset_path, sliding_window_size)

    ranks = {check_day: [] for check_day in check_days}
    our_results = load_results(project_name, sws=sliding_window_size)
    for date, results in our_results.items():
        if not results["replacements"]:
            continue

        # update graph
        G.forward_until(date)

        for leaving_dev, recommended_devs in results["replacements"].items():
            if not recommended_devs:
                continue

            if random_val:
                # Randomly select "max_k" developers
                other_devs = results["developers"]
                other_devs.remove(leaving_dev)
                recommended_devs = random.sample(other_devs, max_k)
            else:
                # Convert dictionary keys to list and get first "max_k" items
                recommended_devs = list(recommended_devs)[:max_k]

            leaving_dev_files = set(G.find_reachable_files(leaving_dev))

            for check_day in check_days:
                # get the change sets for the check days
                change_sets = dm.get_specific_window(
                    date + timedelta(days=1), date + timedelta(days=check_day)
                )
                rank = float("inf")
                for i, recommended_dev in enumerate(recommended_devs):
                    recommended_dev_files = set(G.find_reachable_files(recommended_dev))
                    target_files = leaving_dev_files - recommended_dev_files

                    if check_modification(change_sets, recommended_dev, target_files):
                        rank = i + 1
                        break

                ranks[check_day].append(rank)

    exp_name = "{}_sws{}".format(project_name, sliding_window_size)
    ret_items = [exp_name]

    for check_day in check_days:
        res = {}
        for k in range(1, max_k + 1):
            res["top{}".format(k)] = cal_accuracy(ranks[check_day], k)

        res["mrr"] = cal_mrr(ranks[check_day])

        ret_items.append((check_day, res))
    return ret_items


def check_modification(change_sets, recommended_dev, target_files):
    """
    Check the given change sets. If `recommended_dev` change any of `target_files`,
    return True. Otherwise return False.

    Returns
    -------
    bool:
        True if `recommended_dev` change any of `target_files`. Otherwise, False.
    """

    for cs in change_sets:
        if cs.author != recommended_dev:
            continue

        for cc in cs.code_changes:
            if cc.change_type == "RENAME" and cc.old_file_path in target_files:
                target_files.remove(cc.old_file_path)
                target_files.add(cc.file_path)
            elif cc.change_type == "MODIFY" and cc.file_path in target_files:
                return True


def cal_accuracy(ranks, k):
    """
    Calculate topk accuracy. The numbers in `ranks` which are less than or equal to `k`
    will be considered as hit, in other words True.

    Parameters
    ----------
    ranks (list):
        List of integers showing the found ranks.

    k (int):
        Limit to consider the rank True or not.

    Returns
    -------
    float:
        A number between 0 and 100 (inclusive).

    """

    return 100 * sum(1 for rank in ranks if rank <= k) / len(ranks)


def cal_mrr(ranks):
    """
    Calculate mean reciprocal rank (MRR). Mean of the inverse of the integers given in `ranks`

    Parameters
    ----------
    ranks (list):
        List of integers showing the found ranks.

    Returns
    -------
    float:
        A number between 0 and 100 (inclusive).
    """

    return 100 * sum(1 / rank for rank in ranks) / len(ranks)


if __name__ == "__main__":
    experiments = []
    for project_name in ["hadoop", "hive", "pig", "hbase", "derby", "zookeeper"]:
        for sliding_window_size in [180, 365]:
            experiments.append((project_name, sliding_window_size))

    max_k = 3
    check_days = (7, 30, 90)

    # OUR APPROACH
    outputs = Parallel(n_jobs=-1, verbose=10)(
        validation(*params, check_days, max_k, False) for params in experiments
    )

    print("\nOUR APPROACH\n")
    for output in outputs:
        print(output[0])
        for res_tuple in output[1:]:
            check_day = res_tuple[0]
            formatted_res = {
                metric: "{:.2f}".format(value) for metric, value in res_tuple[1].items()
            }
            print(check_day, formatted_res)

    # RANDOM SELECTION
    num_sims = 100
    experiments *= num_sims
    outputs = Parallel(n_jobs=-1, verbose=10)(
        validation(*params, check_days, max_k, True) for params in experiments
    )

    # Find the average of all simulations (Sum all and find average while printing)
    grouped_res = {}

    for output in outputs:
        exp_name = output[0]
        if exp_name not in grouped_res:
            grouped_res[exp_name] = {}
        for res_tuple in output[1:]:
            check_day = res_tuple[0]
            res_dict = res_tuple[1]
            if check_day not in grouped_res[exp_name]:
                grouped_res[exp_name][res_tuple[0]] = {metric: 0 for metric in res_dict}

            for metric, value in res_dict.items():
                grouped_res[exp_name][check_day][metric] += value

    print("\n\nRANDOM SELECTION - {} simulations\n".format(num_sims))
    for exp_name, output in grouped_res.items():
        print(exp_name)
        for check_day, res_dict in output.items():
            print(
                check_day,
                {k: "{:.2f}".format(v / num_sims) for k, v in res_dict.items()},
            )
