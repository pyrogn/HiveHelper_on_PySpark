from os import listdir, getcwd
from os.path import join, isdir, dirname, basename

HHOP_DIR_NAME = "hhop_lib"


# courtesy of https://stackoverflow.com/questions/49034576/move-up-directory-until-folder-found-python
def find_hhop_location():
    """copy paster from SO to find a path with hhop_lib"""
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
        if HHOP_DIR_NAME in dirs:
            filepath = par_dir
            return join(filepath, HHOP_DIR_NAME)
        # back it out another parent otherwise
        par_dir = dirname(par_dir)
