from os import listdir, getcwd
from os.path import join, isdir, dirname, basename

hhop_dir_name = "hhop_lib"


# courtesy of https://stackoverflow.com/questions/49034576/move-up-directory-until-folder-found-python
def find_hhop_location():
    filepath = None
    # dir of current working directory
    par_dir = getcwd()
    while True:
        # get basenames of all the directories in that parent
        dirs = [
            basename(join(par_dir, d))
            for d in listdir(par_dir)
            if isdir(join(par_dir, d))
        ]
        # the parent contains desired directory
        if hhop_dir_name in dirs:
            filepath = par_dir
            return join(filepath, hhop_dir_name)
        # back it out another parent otherwise
        par_dir = dirname(par_dir)
    raise Exception(
        "cannot found hhop library. Use import os; os.getcwd() to help yourself to find it"
    )
