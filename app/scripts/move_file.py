import shutil
import datetime
import numpy as np
import pathlib


def move_file(from_folder, to_folder):
    """Moves the most older csv file from from_folder to to_folder.

    Parameters
    ----------
    from_folder : str
        Folder from which the file is moved.
    to_folder : str
        Folder to which the file is moved.

    Returns
    -------
    from_filepath : pathlib.Path
        Filepath of the file before being moved.
    to_filepath : pathlib.Path
        Filepath of the file after being moved.
    """

    files = list(pathlib.Path(from_folder).rglob("*.csv"))
    files = [str(f) for f in files]
    argmin_date = np.argmin(
        [
            datetime.datetime.strptime(
                str(f.split("/")[-1].split("_")[1]), "%Y%m%dT%H%M"
            )
            for f in files
        ]
    )
    from_filepath = pathlib.Path(files[argmin_date])
    to_filepath = pathlib.Path(
        to_folder).joinpath(from_filepath.name)
    shutil.move(
        from_filepath, to_filepath
    )
    return from_filepath, to_filepath
