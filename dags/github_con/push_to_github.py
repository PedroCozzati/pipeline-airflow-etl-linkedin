import os
from github import Github
from dotenv import load_dotenv
load_dotenv()


def add_or_update_in_git():
    
    access_tocken = os.getenv("ACCESS_TOKEN")
    github_repo = os.getenv("GITHUB_REPO")
    git_branch = os.getenv("GIT_BRANCH")

    db_file = "../prod/dados_analitico.db"
    db_directory = os.path.dirname(__file__)
    db_path = os.path.join(db_directory, db_file)

    initial_file = db_path
    folder_empl_in_git = "dados_analitico.db"

    g = Github(access_tocken)
    

    repo = g.get_user().get_repo(github_repo)
    
    print(repo)

    all_files = []
    contents = repo.get_contents("")

    while contents:
        file_content = contents.pop(0)
        if file_content.type == "dir":
            contents.extend(repo.get_contents(file_content.path))
        else:
            file = file_content
            all_files.append(str(file).replace('ContentFile(path="', '').replace('")', ''))

    with open(initial_file, 'rb') as file:
        content = file.read()

    # Upload to github
    if folder_empl_in_git in all_files:
        contents = repo.get_contents(folder_empl_in_git)
        repo.update_file(contents.path, "committing files", content, contents.sha, branch=git_branch)
        return folder_empl_in_git + ' UPDATED'
    else:
        repo.create_file(folder_empl_in_git, "committing files", content, branch=git_branch)
        return folder_empl_in_git + ' CREATED'

