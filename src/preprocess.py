"""
Preprocesses the raw databases, and creates JSON formatted datasets.
"""
import json
from util import execute_db_query, get_dataset_path, project_list
from collections import defaultdict


def get_commit_to_issues(project_name):
    """
    Get a mapping from commit hash to issue ids.

    Parameters
    ----------
    project_name (str):
        Name of the project. "<project_name>.sqlite3" has to be in data folder.

    Returns
    -------
    dict:
        Mapping from commit hash to issue ids.
    """
    query_results = execute_db_query(
        "data/{}.sqlite3".format(project_name),
        """
        SELECT issue_id, commit_hash
        FROM change_set_link
        """,
    )

    commit_to_issues = defaultdict(list)
    for issue_id, commit_hash in query_results:
        commit_to_issues[commit_hash].append(issue_id)

    return commit_to_issues


def get_commit_to_codechanges(project_name):
    """
    Get a mapping from commit hash to code changes.

    Parameters
    ----------
    project_name (str):
        Name of the project. "<project_name>.sqlite3" has to be in data folder.

    Returns
    -------
    dict:
        Mapping from commit hash to code changes.
    """
    query_results = execute_db_query(
        "data/{}.sqlite3".format(project_name),
        """
        SELECT commit_hash, file_path, change_type, sum_added_lines, sum_removed_lines
        FROM code_change
        """,
    )
    commit_to_codechanges = defaultdict(list)
    for commit_hash, fpath, ctype, num_added, num_deleted in query_results:
        if fpath.endswith(".java"):
            fname = fpath[fpath.rfind("/") + 1 :]
            commit_to_codechanges[commit_hash].append(
                (fpath, ctype, fname, num_added, num_deleted)
            )

    return commit_to_codechanges


def get_commits(project_name):
    """
    Get commits in temporal order. Merge commits are excluded.

    Parameters
    ----------
    project_name (str):
        Name of the project. "<project_name>.sqlite3" has to be in data folder.

    Returns
    -------
    list:
        Tuples of commit hash (i.e. id), author and date in temporal order.
        For example, "[(commit1, author1, 12Oct2013), (commit2, author1, 19Oct2013)]"
    """
    query_results = execute_db_query(
        "data/{}.sqlite3".format(project_name),
        """
        SELECT commit_hash, author, committed_date
        FROM change_set 
        WHERE is_merge=0
        ORDER BY committed_date
        """,
    )

    return [
        (commit_hash, author, committed_date)
        for commit_hash, author, committed_date in query_results
    ]


def extract_change_sets(project_name, author_mapping):
    """
    Extracts change sets from sqlite3 database.

    Parameters
    ----------
    project_name (str):
        Name of the project. "<project_name>.sqlite3" has to be in data folder.

    author_mapping (str):
        Mapping from aliases to real names of developers

    Returns
    -------
    str:
        JSON formatted string.
    """
    # Get the mapping from commit hash to issue ids
    commit_to_issues = get_commit_to_issues(project_name)

    # Get the mapping from commit hash to code changes
    commit_to_codechanges = get_commit_to_codechanges(project_name)

    # Get the commits in temporal order
    commits = get_commits(project_name)

    current_files = set()
    change_set_jsons = []
    prev_comparison_str = ""
    for commit_hash, author, date in commits:
        # Ignore if the commit has no code change
        if commit_hash not in commit_to_codechanges:
            continue

        change_set_dict = {}

        # Change set info
        change_set_dict["commit_hash"] = commit_hash
        author = author.lower()
        author = author_mapping.get(author, author)  # Correct author name.
        change_set_dict["author"] = author
        change_set_dict["date"] = date

        # Related issues
        change_set_dict["issues"] = commit_to_issues.get(commit_hash, [])

        # For each file name, find code changes in the change set
        fname_to_cchanges = defaultdict(list)
        for fpath, ctype, fname, num_added, num_deleted in commit_to_codechanges[
            commit_hash
        ]:
            fname_to_cchanges[fname].append(
                {
                    "ctype": ctype,
                    "fpath": fpath,
                    "num_added": num_added,
                    "num_deleted": num_deleted,
                }
            )

        # Find code changes in the commit
        extracted_changes = []
        for fname, cchanges_queue in fname_to_cchanges.items():
            while cchanges_queue:
                extracted_change = None

                # Pop one code change from the queue
                cchange = cchanges_queue.pop(0)

                # Check ADD and DELETE types for RENAME
                if cchange["ctype"] == "ADD":  # Possible RENAME
                    # Search for corresponding DELETE
                    for cc in cchanges_queue:
                        if (
                            cc["ctype"] == "DELETE"
                            and cc["num_deleted"] == cchange["num_added"]
                        ):  # RENAME
                            extracted_change = {
                                "file_path": cchange["fpath"],
                                "change_type": "RENAME",
                                "old_file_path": cc["fpath"],
                            }
                            cchanges_queue.remove(cc)  # Remove corresponding DELETE
                            break
                elif cchange["ctype"] == "DELETE":  # Possible RENAME
                    # Search for corresponding ADD
                    for cc in cchanges_queue:
                        if (
                            cc["ctype"] == "ADD"
                            and cc["num_added"] == cchange["num_deleted"]
                        ):  # RENAME
                            extracted_change = {
                                "file_path": cc["fpath"],
                                "change_type": "RENAME",
                                "old_file_path": cchange["fpath"],
                            }
                            cchanges_queue.remove(cc)  # Remove corresponding ADD
                            break

                if extracted_change == None:  # No RENAME situation detected
                    extracted_change = {
                        "file_path": cchange["fpath"],
                        "change_type": cchange["ctype"],
                    }

                extracted_changes.append(extracted_change)

                # This is for tracking the set of files after the commit
                ctype = extracted_change["change_type"]
                fpath = extracted_change["file_path"]

                if ctype == "DELETE" and fpath in current_files:
                    current_files.remove(fpath)
                elif ctype == "ADD":
                    current_files.add(fpath)
                elif ctype == "RENAME":
                    current_files.discard(extracted_change["old_file_path"])
                    current_files.add(fpath)

        if extracted_changes != []:
            change_set_dict["code_changes"] = extracted_changes
            change_set_dict["num_current_files"] = len(current_files)
            change_set_json = json.dumps(change_set_dict, ensure_ascii=False)

            # Prevent same commits (only hashes are different)
            comparison_str = change_set_json.split('"author":')[1]
            if comparison_str != prev_comparison_str:
                change_set_jsons.append(change_set_json)
            prev_comparison_str = comparison_str

    text = '{"change_sets": [' + ",\n".join(change_set_jsons) + "]}"
    return text


# # Author Mapping
# For each dataset, we manually created a dictionary to map different names of the same
# author to one of his/her names

# The manual procedure is as follows:
# 1. Convert all author names to lower case
# 2. Map author names by considering names and email addresses.
# 3. Search name aternatives online in suspicious cases.

pig_author_mapping = {
    "daijy": "jianyong dai",
    "rohini": "rohini palaniswamy",
}

hive_author_mapping = {
    "aihuaxu": "aihua xu",
    "amareshwari sriramadasu": "amareshwari sri ramadasu",
    "author: teddy choi": "teddy choi",
    "chao sun": "sun chao",
    "chengxiang": "chengxiang li",
    "chinnrao l": "chinna r lalam",
    "chinna rao l": "chinna r lalam",
    "ctang": "chaoyu tang",
    "daniel dai": "jianyong dai",
    "dapeng sun": "sun dapeng",
    "gopal v": "gopal vijayaraghavan",
    "haindrich zoltán (kirk)": "zoltan haindrich",
    "iilya yalovyy": "illya yalovyy",
    "ke jia": "jia ke",
    "jpullokk": "john pullokkaran",
    "mithun rk": "mithun radhakrishnan",
    "pengchengxiong": "pengcheng xiong",
    "prasanth j": "prasanth jayachandran",
    "ran gu": "ran wu",
    "sahil takir": "sahil takiar",
    "sankarh": "sankar hariappan",
    "sergey": "sergey shelukhin",
    "sergio peña": "sergio pena",
    "thejas nair": "thejas m nair",
    "vikram": "vikram dixit k",
    "wei": "wei zheng",
    "xzhang": "xuefu zhang",
}

hadoop_author_mapping = {
    "=": "carlo curino",
    "aaron myers": "aaron twining myers",
    "aaron t. myers": "aaron twining myers",
    "alejandro abdelnur": "alejandro humberto abdelnur",
    "amareshwari sriramadasu": "amareshwari sri ramadasu",
    "arp": "arpit agarwal",
    "arun murthy": "arun c. murthy",
    "brandonli": "brandon li",
    "ccurino": "carlo curino",
    "clamb": "charles lamb",
    "chensammi": "sammi chen",
    "chris douglas": "christopher douglas",
    "chun-yang chen": "scott chun-yang chen",
    "cnauroth": "chris nauroth",
    "colin mccabe": "colin patrick mccabe",
    "colin p. mccabe": "colin patrick mccabe",
    "devaraj k": "devarajulu k",
    "doug cutting": "douglass cutting",
    "drankye": "kai zheng",
    "inigo": "inigo goiri",
    "jakob homan": "jakob glen homan",
    "jason lowe": "jason darrell lowe",
    "jian": "jian he",
    "jitendra pandey": "jitendra nath pandey",
    "jonathan eagles": "jonathan turner eagles",
    "junping_du": "junping du",
    "konstantin boudnik": "konstantin i boudnik",
    "konstantin shvachko": "konstantin v shvachko",
    "mattf": "matthew j. foley",
    "matthew foley": "matthew j. foley",
    "ravi  gummadi": "ravi gummadi",
    "rohithsharmaks": "rohith sharma k s",
    "sandy ryza": "sanford ryza",
    "stack": "michael stack",
    "subru": "subru krishnan",
    "sunil": "sunil g",
    "sunilg": "sunil g",
    "tgraves": "thomas graves",
    "tsz-wo sze": "tsz-wo nicholas sze",
    "uma mahesh": "uma maheswara rao g",
    "vinayakumarb": "vinayakumar b",
    "vinod kumar vavilapalli (i am also known as @tshooter.)": "vinod kumar vavilapalli",
    "vrushali": "vrushali channapattan",
    "vrushali c": "vrushali channapattan",
    "waltersu4549": "walter su",
    "wenxinhe": "wenxin he",
    "xuan": "xuan gong",
    "xuangong": "xuan gong",
    "yliu": "yi liu",
    "zhezhang": "zhe zhang",
}

hbase_author_mapping = {
    "andrew purtell": "andrew kyle purtell",
    "anoopsjohn": "anoopsamjohn",
    "apekshit": "apekshit(appy) sharma",
    "apekshit sharma": "apekshit(appy) sharma",
    "chenyechao": "yechao chen",
    "chiaping tsai": "chia-ping tsai",
    "churro": "rahul gidwani",
    "ckulkarni": "chinmay kulkarni",
    "dskskv": "chetkhatri",
    "eclark": "elliott clark",
    "gjacoby": "geoffrey",
    "jeffrey": "jeffrey zhong",
    "jxiang": "jimmy xiang",
    "jonathan hsieh": "jonathan m hsieh",
    "jyates": "jesse yates",
    "keith winkler": "keith david winkler",
    "liangxie": "xieliang",
    "mbertozzi": "matteo bertozzi",
    "ndimiduk": "nick dimiduk",
    "nke": "nkeywal",
    "rahulgidwani": "rahul gidwani",
    "rajeshbabu": "rajeshbabu chintaguntla",
    "reid": "reid chan",
    "rgidwani": "rahul gidwani",
    "smaddineni": "sukumar maddineni",
    "stack": "michael stack",
    "ted yu": "zhihong yu",
    "tedyu": "zhihong yu",
    "thiruvel": "thiruvel thirumoolan",
    "tom tsuruhara": "tomu tsuruhara",
    "unknown": "rsvasude",
    "xiaowen147": "yun zhao",
    "vasudevan": "ramkrishna",
    "yi deng": "david deng",
}

derby_author_mapping = {}

zookeeper_author_mapping = {
    "andor molnár": "andor molnar",
    "flavio junqueira": "flavio paiva junqueira",
    "fpj": "flavio paiva junqueira",
    "patrick hunt": "patrick d. hunt",
    "raúl gutiérrez segalés": "raul gutierrez segales",
    "raúl gutierrez s": "raul gutierrez segales",
    "robert evans": "robert (bobby) evans",
}

if __name__ == "__main__":
    for project in project_list:
        author_mapping = eval("{}_author_mapping".format(project))

        # First, extract commits and generate a JSON formatted string.
        text = extract_change_sets(project, author_mapping)

        # Dump the extracted JSON formatted text
        dataset_path = get_dataset_path(project)
        with open(dataset_path, "w", encoding="utf8") as f:
            f.write(text)
