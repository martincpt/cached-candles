import os

from typing import Literal, Any

BaseDirLiterals = Literal["CURRENT_DIRECTORY", "PARENT_DIRECTORY"]

class AutoCreateDirectories:
    """Utility class to automatically create directories in the given directory.

    Args:
        dirs (list[str] | str, optional): The list of (or single) directory name(s) to create. Defaults to [].
        base_dir (str | BaseDirLiterals, optional): An absolute path to be used as base directory or one of the BaseDirLiterals. Defaults to "CURRENT_DIRECTORY".
        relative_file (str, optional): The relative file in case of a BaseDirLiterals was set. Defaults to __file__.
    """ 

    def __init__(self, dirs: list[str]|str  = [], base_dir: str|BaseDirLiterals = "CURRENT_DIRECTORY", relative_file: str = __file__) -> None:             
        # make dirs iterable
        dirs = dirs if isinstance(dirs, list) else [dirs]

        # dirs dictionary will hold created directories
        self.dirs = {}

        # set relative file
        self.relative_file = relative_file

        # set base dir
        self.set_base_dir(base_dir)
         
        # auto create the given directories
        for dir in dirs:
            self.create(dir)        

    def set_base_dir(self, base_dir: str|BaseDirLiterals) -> None:
        """Sets the base directory according to the relative file.

        Args:
            base_dir (str|BaseDirLiterals): An absolute path to be used as base directory or one of the BaseDirLiterals.

        Raises:
            ValueError: If invalid base_dir value was given.
        """

        # get the relative file's holding directory
        current_dir = os.path.dirname(os.path.abspath(self.relative_file))
        
        # handle special literals
        if base_dir == "CURRENT_DIRECTORY":
            # use current directory as the base
            self.base_dir = current_dir
            return
        if base_dir == "PARENT_DIRECTORY":
            # use current directory's parent as the base
            self.base_dir = os.path.dirname(current_dir) 
            return
        
        # any other case: handle as absolute path
        if os.path.exists(base_dir):
            # set abs path
            self.base_dir = base_dir
        else:
            # but it must exists
            raise ValueError("`base_dir` must be an absolute path and it needs to exist or one of the BaseDirLiterals.")

    def get_path(self, *args: list[str]) -> str:
        """A shortcut for os.path.join"""
        return os.path.join(*args)

    def create(self, dir: str) -> str:
        """Gets or creates the given directory. 

        Args:
            dir (str): The name of the directory to create.

        Returns:
            str: The absolute path of the directory.
        """
        return self.get(dir)

    def get(self, dir: str) -> str:
        """Gets or creates the given directory. 

        Args:
            dir (str): The name of the directory to get or create.

        Returns:
            str: The absolute path of the directory.
        """

        # early return if current instance already holds it
        if dir in self.dirs:
            return self.dirs[dir]

        # get the abs path
        dir_abs_path = dir if os.path.isabs(dir) else os.path.join(self.base_dir, dir)

        # check if exists and create if it doesn't
        if not os.path.exists(dir_abs_path):
            os.makedirs(dir_abs_path)   

        # store the path to dirs dictionary
        self.dirs[dir] = dir_abs_path

        return dir_abs_path


def auto_create_dirs(*args, **kwargs) -> AutoCreateDirectories:
    """Generic shortcut for creating an AutoCreateDirectories instance on the fly in case a snake cased callable better fits the code style.

    Returns:
        AutoCreateDirectories: The created AutoCreateDirectories instance.
    """
    return AutoCreateDirectories(*args, **kwargs)
