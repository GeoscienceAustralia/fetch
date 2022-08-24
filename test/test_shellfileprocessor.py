
import os
from pathlib import Path

from fetch._core import ShellFileProcessor


def test_shellfilepro_required_files_there():
    command = 'ls {base}.py'
    # required_files = (r'^(?P<base>.*test.+)\.py$',['{base}.py','{base}.py'])
    required_files = ('^(?P<base>.*test.+)\\.py$', ['{base}.py', '{base}.py'])
    file_path = os.path.abspath(__file__)
    expect_file = '{base}.py'
    sfp = ShellFileProcessor(command=command, expect_file=expect_file, input_files=required_files)
    results = sfp.process(file_path)

    assert results == Path(__file__).absolute().as_posix()


def test_shellfilepro_required_files_not_there():
    command = 'ls {base}.py'
    required_files = (r'^(?P<base>.*test.+)\.py$', ['{base}.py', '/this/is/not/here/please.py'])
    file_path = os.path.abspath(__file__)
    sfp = ShellFileProcessor(command=command, expect_file=file_path, input_files=required_files)
    results = sfp.process(file_path)
    # A rather toothless assert given required_files_there would return the same result...
    assert file_path == results
    # Future option is to ask for a tmp_path, and make your command touch {tmp_path / 'test_file.txt'}.
    # Then if the file exists, you know the command was run. It could work in both tests.

# Add a test when no required_files parameter is supplied at all
